#include "reference.h"
#include "flushthread.h"
#include "forgetthread.h"

//Stores the schema
vector<uint32_t> schema;

//Stores the content of the relations (i.e. tuples)
vector<map<uint32_t,vector<uint64_t>>> relations;

//Maps tuples to their content (and their relation id)
tupleContent_t * tupleContentPtr[NB_THREAD];

//Maps a relation's column and a value to the tuples that affected it
transactionHistory_t * transactionHistoryPtr[NB_THREAD];

//Stores the booleans for output + mutex
boost::container::flat_map<uint64_t,bool> queryResults;
mutex mutexQueryResults;

//Lists of (validationQuery, (query, columns)) to be processed by each flush thread
queriesToProcess_t * queriesToProcessPtr[NB_THREAD];

//Lists of tuples and their values to be indexed by each flush thread (for each relation)
tuplesToIndex_t * tuplesToIndexPtr[NB_THREAD];

//Maps each unique column with figures
uColIndicator_t * uColIndicatorPtr[NB_THREAD];

//Stuff for synchronization
atomic<uint32_t> processingFlushThreadsNb(0), processingForgetThreadsNb(0);
condition_variable_any conditionFlush, conditionForget;
mutex mutexFlush, mutexForget;
atomic<bool> referenceOver(false);
atomic<uint64_t> forgetTransactionId(0);

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
    for(uint32_t thread=0; thread!=NB_THREAD; ++thread){
        transactionHistoryPtr[thread]->resize(schema.size());
        uColIndicatorPtr[thread]->resize(schema.size());
    }
    for(uint32_t i=0; i!=schema.size(); ++i){
        auto thread = assignedThread(i);
        (*transactionHistoryPtr[thread])[i].resize(schema[i]);
        (*uColIndicatorPtr[thread])[i].resize(schema[i]);
        for(uint32_t j=0; j!=schema[i]; ++j){
            (*transactionHistoryPtr[thread])[i][j] = new map<uint64_t, vector<Tuple>>;
        }
    }
}
//---------------------------------------------------------------------------
static void processTransaction(const Transaction& t)
{
    //Tuple identifier
    Tuple tuple{t.transactionId, 0};

    const char* reader=t.operations;

    // Delete all indicated tuples
    for (uint32_t index=0;index!=t.deleteCount;++index) {
        const TransactionOperationDelete* o= (const TransactionOperationDelete*) reader;

        //Gets thread assigned
        uint32_t thread = assignedThread(o->relationId);

        //Loops through the tuples to delete
        for (const uint64_t* key=o->keys,*keyLimit=key+o->rowCount;key!=keyLimit;++key) {
            //If the tuple key exists in the relation
            if (relations[o->relationId].count(*key)) {
                vector<uint64_t>& tupleValues = relations[o->relationId][*key];
                //Adds to the tuples to index
                (*tuplesToIndexPtr[thread]).push_back(pair<uint32_t, pair<Tuple, vector<uint64_t>>>(o->relationId, pair<Tuple, vector<uint64_t>>(tuple, move(tupleValues))));
                //Erase
                relations[o->relationId].erase(*key);
                //Increments internId
                ++tuple.internId;
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
            tuplesToIndexPtr[thread]->push_back(make_pair(o->relationId, make_pair(tuple, vector<uint64_t>())));
            auto * tupleValues = &(tuplesToIndexPtr[thread]->back().second.second);
            tupleValues->insert(tupleValues->begin(),values,values+schema[o->relationId]);
            //Inserts
            relations[o->relationId][values[0]] = *tupleValues;
            //Increments internId
            ++tuple.internId;
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

        //Counts predicates
        uint32_t nbPredicPerCol[schema[q.relationId]] = {0};
        for (auto c=q.columns,cLimit=c+q.columnCount;c!=cLimit;++c){
            ++nbPredicPerCol[c->column];
        }

        //Prevents "useless" queries from being added to the processing queue
        bool notToBePushed = false;
        for(uint32_t i=0; i<schema[q.relationId]; ++i){
            if(nbPredicPerCol[i]>1){
                const Query::Column::Op * op = NULL;
                uint64_t value = 0;
                for (auto c=q.columns,cLimit=c+q.columnCount;c!=cLimit;++c){
                    if(c->column != i) continue;
                    if(op==NULL){
                        op = &(c->op);
                        value = c->value;
                    }else{
                        auto op2 = c->op;
                        switch(*op){
                            case Query::Column::Equal:
                                if(op2==Query::Column::Equal) notToBePushed = c->value!=value;
                                else if(op2==Query::Column::Greater) notToBePushed = c->value>=value;
                                else if(op2==Query::Column::GreaterOrEqual) notToBePushed = c->value>value;
                                else if(op2==Query::Column::Less) notToBePushed = c->value<=value;
                                else if(op2==Query::Column::LessOrEqual) notToBePushed = c->value<value;
                                else if(op2==Query::Column::NotEqual) notToBePushed = c->value==value;
                                break;
                            case Query::Column::Greater:
                                if(op2==Query::Column::Equal) notToBePushed = c->value<=value;
                                else if(op2==Query::Column::Less) notToBePushed = c->value<=value;
                                else if(op2==Query::Column::LessOrEqual) notToBePushed = c->value<=value;
                                break;
                            case Query::Column::GreaterOrEqual:
                                if(op2==Query::Column::Equal) notToBePushed = c->value<value;
                                else if(op2==Query::Column::Less) notToBePushed = c->value<=value;
                                else if(op2==Query::Column::LessOrEqual) notToBePushed = c->value<value;
                                break;
                            case Query::Column::Less:
                                if(op2==Query::Column::Equal) notToBePushed = c->value>=value;
                                else if(op2==Query::Column::Greater) notToBePushed = c->value>=value;
                                else if(op2==Query::Column::GreaterOrEqual) notToBePushed = c->value>=value;
                                break;
                            case Query::Column::LessOrEqual:
                                if(op2==Query::Column::Equal) notToBePushed = c->value>value;
                                else if(op2==Query::Column::Greater) notToBePushed = c->value>=value;
                                else if(op2==Query::Column::GreaterOrEqual) notToBePushed = c->value>value;
                                break;
                            default:
                                break;
                        }
                    }
                    if(notToBePushed) break;
                }
                if(notToBePushed) break;
            }
        }

        //Adds query to the list to process by the relevant thread
        if(!notToBePushed){
            //Push
            auto thread = assignedThread(q.relationId);
            queriesToProcessPtr[thread]->push_back(make_pair(v, make_pair(q, vector<Query::Column>())));
            //Adds the columns
            vector<Query::Column> * vCol = &(queriesToProcessPtr[thread]->back().second.second);
            vCol->resize(q.columnCount);
            memmove(vCol->data(), &(q.columns), (sizeof(Query::Column)*q.columnCount));
        }else{
            queryResults[v.validationId] = false;
        }

        //Offsets reader
        reader+=sizeof(Query)+(sizeof(Query::Column)*q.columnCount);
    }
}
//---------------------------------------------------------------------------
static void processFlush(const Flush& f)
{
    cerr << "Flush " << f.validationId << endl;

    //Notifies the flush threads
    while(processingFlushThreadsNb!=NB_THREAD){} //Safety while, shoudn't happen
    mutexFlush.lock();
    conditionFlush.notify_all();
    conditionFlush.wait(mutexFlush);
    mutexFlush.unlock();

    //Outputs all the queryResults available
    mutexQueryResults.lock();
    while ((!queryResults.empty())&&((*queryResults.begin()).first<=f.validationId)) {
        char c='0'+(*queryResults.begin()).second;
        cout.write(&c,1);
        queryResults.erase(queryResults.begin());
    }
    mutexQueryResults.unlock();

    //Flush output
    cout.flush();
}
//---------------------------------------------------------------------------
static void processForget(const Forget& f)
{
    cerr << "Forget " << f.transactionId << endl;

    //Notifies the Forget threads
    forgetTransactionId = f.transactionId;
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

    //Allocates maps
    for(uint32_t thread=0; thread!=NB_THREAD; ++thread){
        transactionHistoryPtr[thread] = new transactionHistory_t;
        tupleContentPtr[thread] = new tupleContent_t;
        queriesToProcessPtr[thread] = new queriesToProcess_t;
        tuplesToIndexPtr[thread] = new tuplesToIndex_t;
        uColIndicatorPtr[thread] = new uColIndicator_t;
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
                    auto thread = assignedThread(i);
                    for(uint32_t j=0; j!=schema[i]; ++j){
                        delete (*(transactionHistoryPtr[thread]))[i][j];
                    }
                }
                //Desallocates
                for(uint32_t thread=0; thread!=NB_THREAD; ++thread){
                    delete transactionHistoryPtr[thread];
                    delete tupleContentPtr[thread];
                    delete queriesToProcessPtr[thread];
                    delete tuplesToIndexPtr[thread];
                    delete uColIndicatorPtr[thread];
                }
                return 0;
            default:
                // crude error handling, should never happen
                cerr << "malformed message" << endl; abort();
        }
    }
}
//---------------------------------------------------------------------------
