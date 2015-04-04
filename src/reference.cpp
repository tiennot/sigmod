#include "reference.h"
#include "flushthread.h"
#include "forgetthread.h"

//Stores the schema
vector<uint32_t> schema;

//Stores the content of the relations (i.e. tuples)
vector<unordered_map<uint32_t,vector<uint64_t>>> relations;

//Maps tuples to their content (and their relation id)
TupleCBuffer * tupleContentPtr;
Tuple currentTuple = 0;

//Maps a relation's column and a value to the tuples that affected it
transactionHistory_t * transactionHistoryPtr;

//Stores the booleans for output & last echoed validation + mutex
pair<vector<bool>, uint64_t> queryResults;
mutex mutexQueryResults;

//Lists of (validationQuery, (query, columns)) to be processed by each flush thread
queriesToProcess_t * queriesToProcessPtr[NB_THREAD];

//Lists of tuples and their values to be indexed by each flush thread (for each relation)
tuplesToIndex_t * tuplesToIndexPtr[NB_THREAD];

//Maps each unique column with figures
uColIndicator_t * uColIndicatorPtr;

//Stuff for synchronization
atomic<uint32_t> processingFlushThreadsNb(NB_THREAD), processingForgetThreadsNb(0);
condition_variable_any conditionFlush, conditionForget;
mutex mutexFlush, mutexForget;
atomic<bool> referenceOver(false);
atomic<uint64_t> forgetTupleBound(0);

//---------------------------------------------------------------------------
static void processDefineSchema(const DefineSchema& d)
{
    //Inserts the number of columns for each relation
    //e.g. schema = {23, 37, 43, 16, 17, 5, 15, 19, 47, ...}
    schema.clear();
    schema.insert(schema.begin(),d.columnCounts,d.columnCounts+d.relationCount);

    //Resizes relation vector
    relations.clear();
    relations.resize(d.relationCount);

    //Allocates maps for history
    transactionHistoryPtr->resize(schema.size());
    uColIndicatorPtr->resize(schema.size());

    for(uint32_t i=0; i!=schema.size(); ++i){
        (*transactionHistoryPtr)[i].resize(schema[i]);
        (*uColIndicatorPtr)[i].resize(schema[i]);
        for(uint32_t j=0; j!=schema[i]; ++j){
            //A map that keeps the tuples for each value
            (*transactionHistoryPtr)[i][j].first = new unordered_map<uint64_t, vector<Tuple>>;
            //A vector, used when only <, >, <=, >=
            (*transactionHistoryPtr)[i][j].second = new vector<uint64_t>;
        }
    }
}
//---------------------------------------------------------------------------
static void processTransaction(const Transaction& t)
{
    //Maps transaction with the tuples
    tupleContentPtr->addTEntry(t.transactionId);

    const char* reader=t.operations;

    // Delete all indicated tuples
    for (uint32_t index=0;index!=t.deleteCount;++index) {
        const TransactionOperationDelete* o= (const TransactionOperationDelete*) reader;

        //Gets thread assigned
        uint32_t thread = assignedThread(o->relationId);

        //Loops through the tuples to delete
        for (const uint64_t* key=o->keys,*keyLimit=key+o->rowCount;key!=keyLimit;++key) {
            //If the tuple key exists in the relation
            auto find = relations[o->relationId].find(*key);
            if (find!=relations[o->relationId].end()) {
                vector<uint64_t> * tupleValues = &(find->second);
                //Inserts in the tupleValues
                tupleContentPtr->push_back(*tupleValues);
                //Adds to the tuples to index
                (*tuplesToIndexPtr[thread]).push(make_pair(o->relationId, make_pair(currentTuple, move(*tupleValues))));
                //Erase
                relations[o->relationId].erase(*key);
                //Increments counter
                currentTuple++;
            }
        }
        reader+=sizeof(TransactionOperationDelete)+(sizeof(uint64_t)*o->rowCount);
    }

    // Insert new tuples
    for (uint32_t index=0;index!=t.insertCount;++index) {
        const TransactionOperationInsert* o= (const TransactionOperationInsert*) reader;

        //Gets thread assigned
        uint32_t thread = assignedThread(o->relationId);

        //Loops through the tuples to insert
        for (const uint64_t* values=o->values,*valuesLimit=values+(o->rowCount*schema[o->relationId]);values!=valuesLimit;values+=schema[o->relationId]) {
            //Adds to the tuples to index
            vector<uint64_t> tupleValues;
            tupleValues.insert(tupleValues.begin(),values,values+schema[o->relationId]);
            tuplesToIndexPtr[thread]->push(make_pair(o->relationId, make_pair(currentTuple, tupleValues)));
            //Inserts in the tupleContent
            tupleContentPtr->push_back(tupleValues);
            //Inserts
            relations[o->relationId][values[0]] = move(tupleValues);
            //Increments counter
            currentTuple++;
        }
        reader+=sizeof(TransactionOperationInsert)+(sizeof(uint64_t)*o->rowCount*schema[o->relationId]);
    }
}
//---------------------------------------------------------------------------
static void processValidationQueries(const ValidationQueries& v)
{
    const char* reader=v.queries;
    for (unsigned index=0;index!=v.queryCount;++index) {
        auto& q=*reinterpret_cast<const Query*>(reader);

        //Adds query to the list to process by the relevant thread
        auto thread = assignedThread(q.relationId);
        queriesToProcessPtr[thread]->push_back(make_pair(v, make_pair(q, vector<Query::Column>())));
        //Adds the columns
        vector<Query::Column> * vCol = &(queriesToProcessPtr[thread]->back().second.second);
        vCol->assign(q.columns, q.columns+q.columnCount);

        //Offsets reader
        reader+=sizeof(Query)+(sizeof(Query::Column)*q.columnCount);
    }

    //Marks as false by default
    queryResults.first.push_back(false);
}
//---------------------------------------------------------------------------
static void processFlush(const Flush& f)
{
    cerr << "Flush " << f.validationId << endl;

    mutexFlush.lock();

    //Tells consumer threads that we are done
    for(uint32_t thread=0; thread!=NB_THREAD; ++thread){
        tuplesToIndexPtr[thread]->signalProducerDone();
    }
    //Waits for flush threads to be done
    conditionFlush.wait(mutexFlush);
    mutexFlush.unlock();

    //Outputs all the queryResults available
    for(uint64_t vId=queryResults.second; vId!=f.validationId+1; ++vId) {
        char c='0' + queryResults.first[vId];
        cout.write(&c,1);
    }
    queryResults.second = f.validationId+1;

    //Flush output
    cout.flush();
}
//---------------------------------------------------------------------------
static void processForget(const Forget& f)
{
    cerr << "Forget " << f.transactionId << endl;

    /*
     * Forget thread needs to make sure indexing is not ongoing
     */
    mutexFlush.lock();
    //Tells consumer threads that we are done
    for(uint32_t thread=0; thread!=NB_THREAD; ++thread){
        tuplesToIndexPtr[thread]->signalProducerDone();
    }
    //Waits for flush threads to be done
    conditionFlush.wait(mutexFlush);
    mutexFlush.unlock();


    //Forget in tupleContent
    tupleContentPtr->forget(f.transactionId);

    //Notifies the Forget threads
    forgetTupleBound = tupleContentPtr->getForgetTuple(f.transactionId);
    while(processingForgetThreadsNb!=NB_THREAD){} //Safety while, shoudn't happen
    mutexForget.lock();
    conditionForget.notify_all();
    conditionForget.wait(mutexForget);
    mutexForget.unlock();
}
//---------------------------------------------------------------------------
// Read the message body and cast it to the desired type
template<typename Type> static const Type& readBody(istream& in,vector<char>& buffer,uint32_t len) {
    buffer.resize(len);
    in.read(buffer.data(),len);
    return *reinterpret_cast<const Type*>(buffer.data());
}
//---------------------------------------------------------------------------
int main()
{
    //cerr << "Pause... "; sleep(15); cerr << "Resumes..." << endl;

    //Allocates stuff
    tupleContentPtr = new TupleCBuffer(33544432);
    transactionHistoryPtr = new transactionHistory_t;
    uColIndicatorPtr = new uColIndicator_t;
    for(uint32_t thread=0; thread!=NB_THREAD; ++thread){
        queriesToProcessPtr[thread] = new queriesToProcess_t;
        tuplesToIndexPtr[thread] = new tuplesToIndex_t(33544432/NB_THREAD);
    }

    //Instanciates threads
    for(uint32_t i=0; i!=NB_THREAD; ++i){
        thread(&FlushThread::launch, FlushThread(i)).detach();
        thread(&ForgetThread::launch, ForgetThread(i)).detach();
    }

    vector<char> message;
    while (true) {
        // Retrieve the message
        MessageHead head;
        cin.read(reinterpret_cast<char*>(&head),sizeof(head));

        // crude error handling, should never happen
        if (!cin) {
            cerr << "read error" << endl; abort();
        }

        // Interprets the message
        switch (head.type) {
            case MessageHead::ValidationQueries:
                processValidationQueries(readBody<ValidationQueries>(cin,message,head.messageLen));
                break;
            case MessageHead::Transaction:
                processTransaction(readBody<Transaction>(cin,message,head.messageLen));
                break;
            case MessageHead::Flush:
                processFlush(readBody<Flush>(cin,message,head.messageLen));
                break;
            case MessageHead::Forget:
                processForget(readBody<Forget>(cin,message,head.messageLen));
                break;
            case MessageHead::DefineSchema:
                processDefineSchema(readBody<DefineSchema>(cin,message,head.messageLen));
                break;
            case MessageHead::Done:
                referenceOver = true;
                mutexFlush.lock();
                conditionFlush.notify_all();
                mutexFlush.unlock();
                mutexForget.lock();
                conditionForget.notify_all();
                mutexForget.unlock();
                //Desallocates history
                for(uint32_t i=0; i!=schema.size(); ++i){
                    for(uint32_t j=0; j!=schema[i]; ++j){
                        delete (*transactionHistoryPtr)[i][j].first;
                        delete (*transactionHistoryPtr)[i][j].second;
                    }
                }
                //Desallocates
                delete transactionHistoryPtr;
                delete uColIndicatorPtr;
                for(uint32_t thread=0; thread!=NB_THREAD; ++thread){
                    delete queriesToProcessPtr[thread];
                    delete tuplesToIndexPtr[thread];
                }
                delete tupleContentPtr;
                return 0;
            default:
                // crude error handling, should never happen
                cerr << "malformed message" << endl; abort();
        }
    }
}
//---------------------------------------------------------------------------
