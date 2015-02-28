// SIGMOD Programming Contest 2015
// Author: Camille TIENNOT (camille.tiennot@telecom-paristech.fr)
//
//---------------------------------------------------------------------------
// This is free and unencumbered software released into the public domain.
//
// Anyone is free to copy, modify, publish, use, compile, sell, or
// distribute this software, either in source code form or as a compiled
// binary, for any purpose, commercial or non-commercial, and by any
// means.
//
// In jurisdictions that recognize copyright laws, the author or authors
// of this software dedicate any and all copyright interest in the
// software to the public domain. We make this dedication for the benefit
// of the public at large and to the detriment of our heirs and
// successors. We intend this dedication to be an overt act of
// relinquishment in perpetuity of all present and future rights to this
// software under copyright law.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
// IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
// OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
// ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
//
// For more information, please refer to <http://unlicense.org/>
//---------------------------------------------------------------------------
#include <iostream>
#include <map>
#include <vector>
#include <set>
#include <cassert>
#include <cstdint>
#include <pthread.h>
using namespace std;
//---------------------------------------------------------------------------

//Declaration for launchThread
void *launchThread(void *ptr);

//---------------------------------------------------------------------------
// Wire protocol messages
//---------------------------------------------------------------------------
struct MessageHead {
    /// Message types
    enum Type : uint32_t { Done, DefineSchema, Transaction, ValidationQueries, Flush, Forget };
    /// Total message length, excluding this head
    uint32_t messageLen;
    /// The message type
    Type type;
};
//---------------------------------------------------------------------------
struct DefineSchema {
    /// Number of relations
    uint32_t relationCount;
    /// Column counts per relation, one count per relation. The first column is always the primary key
    uint32_t columnCounts[];
};
//---------------------------------------------------------------------------
struct Transaction {
    /// The transaction id. Monotonic increasing
    uint64_t transactionId;
    /// The operation counts
    uint32_t deleteCount,insertCount;
    /// A sequence of transaction operations. Deletes first, total deleteCount+insertCount operations
    char operations[];
};
//---------------------------------------------------------------------------
struct TransactionOperationDelete {
    /// The affected relation
    uint32_t relationId;
    /// The row count
    uint32_t rowCount;
    /// The deleted values, rowCount primary keyss
    uint64_t keys[];
};
//---------------------------------------------------------------------------
struct TransactionOperationInsert {
    /// The affected relation
    uint32_t relationId;
    /// The row count
    uint32_t rowCount;
    /// The inserted values, rowCount*relation[relationId].columnCount values
    uint64_t values[];
};
//---------------------------------------------------------------------------
struct ValidationQueries {
    /// The validation id. Monotonic increasing
    uint64_t validationId;
    /// The transaction range
    uint64_t from,to;
    /// The query count
    uint32_t queryCount;
    /// The queries
    char queries[];
};
//---------------------------------------------------------------------------
struct Query {
    /// A column description
    struct Column {
        /// Support operations
        enum Op : uint32_t { Equal, NotEqual, Less, LessOrEqual, Greater, GreaterOrEqual };
        /// The column id
        uint32_t column;
        /// The operations
        Op op;
        /// The constant
        uint64_t value;
    };

    /// The relation
    uint32_t relationId;
    /// The number of bound columns
    uint32_t columnCount;
    /// The bindings
    Column columns[];
};
//---------------------------------------------------------------------------
struct Flush {
    /// All validations to this id (including) must be answered
    uint64_t validationId;
};
//---------------------------------------------------------------------------
struct Forget {
    /// Transactions older than that (including) will not be tested for
    uint64_t transactionId;
};

//---------------------------------------------------------------------------
// Begin reference implementation
//---------------------------------------------------------------------------

//Our four threads
enum AuxThread : uint32_t { Thread1, Thread2, Thread3, Thread4 };

//A structure to identify a unique transaction operation (i.e. one insertion/deletion)
struct UniqueOp {
    uint64_t transactionId; //Id of the transaction
    uint32_t index; //Index of the insertion/deletion
    uint64_t key; //tuple n°?? in the insertion/deletion
    enum OpType : uint32_t { Insertion, Deletion};
    OpType opType;

    bool operator<(const UniqueOp& op) const{
        if(transactionId!=op.transactionId) return transactionId < op.transactionId;
        if(opType!=op.opType) return opType==Deletion;
        if(index!=op.index) return index<op.index;
        if(key!=op.key) return key<op.key;
        return false;
    }

    bool operator==(const UniqueOp& op) const{
        return transactionId==op.transactionId
                && opType==op.opType
                && index==op.index
                && key==op.key;
    }
};

//Stores the schema
static vector<uint32_t> schema;

//Stores the content of the relations (i.e. tuples) + mutex
static vector<map<uint32_t,vector<uint64_t>>> relations;

//Map one relation to one thread
static map<uint32_t, AuxThread> assignedThreads;

//Map<relationId, vector<map<id of operation, value>>>
static map<uint32_t,vector<map<UniqueOp, uint64_t>>>
    transactionHistory1, transactionHistory2, transactionHistory3, transactionHistory4;

//Stores the booleans for output + mutex
static map<uint64_t,bool> queryResults;
pthread_mutex_t mutexQueryResults = PTHREAD_MUTEX_INITIALIZER;

//Lists of (validationQuery, (query, columns)) to be processed by each thread
static vector<pair<ValidationQueries, pair<Query, vector<Query::Column>>>>
    queriesToProcess1, queriesToProcess2, queriesToProcess3, queriesToProcess4;

//Four condition variables + mutexes to synchronize threads
pthread_cond_t  cond1   = PTHREAD_COND_INITIALIZER;
pthread_cond_t  cond2   = PTHREAD_COND_INITIALIZER;
pthread_cond_t  cond3   = PTHREAD_COND_INITIALIZER;
pthread_cond_t  cond4   = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mut1    = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mut2    = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mut3    = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mut4    = PTHREAD_MUTEX_INITIALIZER;

//Variables to tells thread to terminate
static bool endThread1 = false;
static bool endThread2 = false;
static bool endThread3 = false;
static bool endThread4 = false;

//---------------------------------------------------------------------------
//Function that tells which thread handles a given relation
inline static AuxThread assignedThread(uint32_t relationId){
    return assignedThreads[relationId];
}
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

    //Maps relations with threads
    assignedThreads.clear();
    for(uint32_t i=0; i<d.relationCount; ++i){
        map<uint32_t,vector<map<UniqueOp, uint64_t>>> * transactionHistory = NULL;
        if(i<d.relationCount/4){
            assignedThreads[i]=Thread1;
            transactionHistory = &transactionHistory1;
        }else if(i<d.relationCount/2){
            assignedThreads[i]=Thread2;
            transactionHistory = &transactionHistory2;
        }else if(i<3*d.relationCount/4){
            assignedThreads[i]=Thread3;
            transactionHistory = &transactionHistory3;
        }else{
            assignedThreads[i]=Thread4;
            transactionHistory = &transactionHistory4;
        }

        (*transactionHistory)[i].resize(d.columnCounts[i]);
    }
}
//---------------------------------------------------------------------------
static void processTransaction(const Transaction& t)
{
    vector<pair<uint32_t,vector<uint64_t>>> operations1, operations2, operations3, operations4;
    const char* reader=t.operations;

    // Delete all indicated tuples
    for (uint32_t index=0;index!=t.deleteCount;++index) {
        const TransactionOperationDelete* o= (const TransactionOperationDelete*) reader;

        //Switch according to the thread assigned
        AuxThread thread = assignedThread(o->relationId);
        map<uint32_t,vector<map<UniqueOp, uint64_t>>> * transactionHistory = NULL;
        switch(thread){
            case Thread1:
                transactionHistory = &transactionHistory1;
                break;
            case Thread2:
                transactionHistory = &transactionHistory2;
                break;
            case Thread3:
                transactionHistory = &transactionHistory3;
                break;
            default:
                transactionHistory = &transactionHistory4;
        }

        //Loops through the tuples to delete
        for (const uint64_t* key=o->keys,*keyLimit=key+o->rowCount;key!=keyLimit;++key) {
            //If the tuple key exists in the relation
            if (relations[o->relationId].count(*key)) {
                vector<uint64_t> tuple = relations[o->relationId][*key];
                //For each column we add the value to the history
                for(uint32_t col=0; col!=schema[o->relationId]; ++col){
                    UniqueOp op{t.transactionId, index, *key, UniqueOp::OpType::Deletion};
                    (*transactionHistory)[o->relationId][col][op] = tuple[col]; //TODO move()?
                }
                relations[o->relationId].erase(*key);
            }
        }
        reader+=sizeof(TransactionOperationDelete)+(sizeof(uint64_t)*o->rowCount);
    }

    // Insert new tuples
    for (uint32_t index=0;index!=t.insertCount;++index) {
        const TransactionOperationInsert* o= (const TransactionOperationInsert*) reader;
        //Switch according to the thread assigned
        AuxThread thread = assignedThread(o->relationId);
        map<uint32_t,vector<map<UniqueOp, uint64_t>>> * transactionHistory = NULL;
        switch(thread){
            case Thread1:
                transactionHistory = &transactionHistory1;
                break;
            case Thread2:
                transactionHistory = &transactionHistory2;
                break;
            case Thread3:
                transactionHistory = &transactionHistory3;
                break;
            default:
                transactionHistory = &transactionHistory4;
        }
        //Loops through the tuples to insert
        for (const uint64_t* values=o->values,*valuesLimit=values+(o->rowCount*schema[o->relationId]);values!=valuesLimit;values+=schema[o->relationId]) {
            vector<uint64_t> tuple;
            tuple.insert(tuple.begin(),values,values+schema[o->relationId]);

            UniqueOp op{t.transactionId, index, tuple[0], UniqueOp::OpType::Insertion};

            //For each column we add the value to the history
            for(uint32_t col=0; col!=schema[o->relationId]; ++col){
                (*transactionHistory)[o->relationId][col][op] = tuple[col]; //TODO move()?
            }
            relations[o->relationId][values[0]]=move(tuple);
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

        //Build vector of columns
        vector<Query::Column> vColumns;
        uint32_t columnCount = q.columnCount;
        for (auto c=q.columns,cLimit=c+columnCount;c!=cLimit;++c){
            vColumns.push_back(*c);
        }

        //Adds query to the list to process by the relevant thread
        switch(assignedThread(q.relationId)){
            case Thread1:
                queriesToProcess1.push_back(pair<ValidationQueries, pair<Query, vector<Query::Column>>>(v, pair<Query, vector<Query::Column>>(q, vColumns)));
                break;
            case Thread2:
                queriesToProcess2.push_back(pair<ValidationQueries, pair<Query, vector<Query::Column>>>(v, pair<Query, vector<Query::Column>>(q, vColumns)));
                break;
            case Thread3:
                queriesToProcess3.push_back(pair<ValidationQueries, pair<Query, vector<Query::Column>>>(v, pair<Query, vector<Query::Column>>(q, vColumns)));
                break;
            default:
                queriesToProcess4.push_back(pair<ValidationQueries, pair<Query, vector<Query::Column>>>(v, pair<Query, vector<Query::Column>>(q, vColumns)));
                break;
        }
        //Offsets reader
        reader+=sizeof(Query)+(sizeof(Query::Column)*columnCount);
    }
}
//---------------------------------------------------------------------------
static void processFlush(const Flush& f)
{
    clog << "Flush" << endl;
    //Sends signal to aux threads
    pthread_mutex_lock( &mut1 );
    pthread_cond_signal( &cond1 );
    pthread_mutex_lock( &mut2 );
    pthread_cond_signal( &cond2 );
    pthread_mutex_lock( &mut3 );
    pthread_cond_signal( &cond3 );
    pthread_mutex_lock( &mut4 );
    pthread_cond_signal( &cond4 );

    //Waits for aux threads to end processing
    pthread_cond_wait( &cond1, &mut1 );
    pthread_cond_wait( &cond2, &mut2 );
    pthread_cond_wait( &cond3, &mut3 );
    pthread_cond_wait( &cond4, &mut4 );

    //Unlock the mutexes
    pthread_mutex_unlock( &mut1 );
    pthread_mutex_unlock( &mut2 );
    pthread_mutex_unlock( &mut3 );
    pthread_mutex_unlock( &mut4 );

    //Outputs all the queryResults available
    while ((!queryResults.empty())&&((*queryResults.begin()).first<=f.validationId)) {
        char c='0'+(*queryResults.begin()).second;
        cout.write(&c,1);
        queryResults.erase(queryResults.begin());
    }

    //Flush output
    cout.flush();
}
//---------------------------------------------------------------------------
static void processForget(const Forget& f)
{

    clog << "Forget (" << f.transactionId << ")" << endl;
    //Erase from transaction history of thread1
    for(auto iter:transactionHistory1){
        for(auto iter2:iter.second){
            for(auto iter3=iter2.begin(); iter3!=iter2.end();){
                auto iter3cpy = iter3;
                iter3++;
                if(((UniqueOp) iter3cpy->first).transactionId<f.transactionId){
                    iter2.erase(iter3cpy);
                }
            }
        }
    }

    //Erase from transaction history of thread2
    for(auto iter:transactionHistory2){
        for(auto iter2:iter.second){
            for(auto iter3=iter2.begin(); iter3!=iter2.end();){
                auto iter3cpy = iter3;
                iter3++;
                if(((UniqueOp) iter3cpy->first).transactionId<f.transactionId){
                    iter2.erase(iter3cpy);
                }
            }
        }
    }

    //Erase from transaction history of thread3
    for(auto iter:transactionHistory3){
        for(auto iter2:iter.second){
            for(auto iter3=iter2.begin(); iter3!=iter2.end();){
                auto iter3cpy = iter3;
                iter3++;
                if(((UniqueOp) iter3cpy->first).transactionId<f.transactionId){
                    iter2.erase(iter3cpy);
                }
            }
        }
    }

    //Erase from transaction history of thread4
    for(auto iter:transactionHistory4){
        for(auto iter2:iter.second){
            for(auto iter3=iter2.begin(); iter3!=iter2.end();){
                auto iter3cpy = iter3;
                iter3++;
                if(((UniqueOp) iter3cpy->first).transactionId<f.transactionId){
                    iter2.erase(iter3cpy);
                }
            }
        }
    }

    /*for(uint32_t i=0; i<schema.size(); ++i){
        if(assignedThread(i)==Thread1) (transactionHistory1)[i].resize(schema[i]);
        if(assignedThread(i)==Thread2) (transactionHistory2)[i].resize(schema[i]);
        if(assignedThread(i)==Thread3) (transactionHistory3)[i].resize(schema[i]);
        if(assignedThread(i)==Thread4) (transactionHistory4)[i].resize(schema[i]);
    }*/
}
//---------------------------------------------------------------------------
// Read the message body and cast it to the desired type
template<typename Type> static const Type& readBody(istream& in,vector<char>& buffer,uint32_t len) {
    buffer.resize(len);
    in.read(buffer.data(),len);
    return *reinterpret_cast<const Type*>(buffer.data());
}
//---------------------------------------------------------------------------
//Function that handle behavior of aux threads
void *launchThread(void *ptr){
    //Retrieves the id of the thread
    int threadId = *((int*) ptr);

    //Defines aliases for variables according to threadId
    vector<pair<ValidationQueries, pair<Query, vector<Query::Column>>>> * queriesToProcess = NULL;
    map<uint32_t,vector<map<UniqueOp, uint64_t>>> * transactionHistory = NULL;
    pthread_cond_t * cond = NULL;
    pthread_mutex_t * mut = NULL;
    bool * endThread = NULL;

    switch(threadId){
        case 1:
            queriesToProcess = &queriesToProcess1;
            transactionHistory = &transactionHistory1;
            cond = &cond1;
            mut = &mut1;
            endThread = &endThread1;
            break;
        case 2:
            queriesToProcess = &queriesToProcess2;
            transactionHistory = &transactionHistory2;
            cond = &cond2;
            mut = &mut2;
            endThread = &endThread2;
            break;
        case 3:
            queriesToProcess = &queriesToProcess3;
            transactionHistory = &transactionHistory3;
            cond = &cond3;
            mut = &mut3;
            endThread = &endThread3;
            break;
        case 4:
            queriesToProcess = &queriesToProcess4;
            transactionHistory = &transactionHistory4;
            cond = &cond4;
            mut = &mut4;
            endThread = &endThread4;
            break;
    }

    //Waits for the main thread signal (first time)
    pthread_mutex_lock( mut );
    pthread_cond_wait( cond, mut );
    pthread_mutex_unlock( mut );

    //Starts working
    while(true){
        if(!queriesToProcess->empty()){
            //Gets the query from the list
            auto back = queriesToProcess->back();
            ValidationQueries v = move(back.first);
            Query q = move(back.second.first);
            vector<Query::Column> columns = move(back.second.second); //TODO move?

            //Retrieves current result
            pthread_mutex_lock( &mutexQueryResults );
            if(!queryResults.count(v.validationId)) queryResults[v.validationId]=false;
            bool currentResult = queryResults[v.validationId]==true;
            pthread_mutex_unlock( &mutexQueryResults );

            //Handles query only if current result is false
            if(!currentResult){

                /*
                 * Thread query handle begins
                 */

                //History of insertion/deletion for all columns of the relation
                auto& relationHistory = (*transactionHistory)[q.relationId];

                vector<UniqueOp> candidates;

                //If there is no column
                if(q.columnCount==0){
                    //Query conflicts if there is at least one transaction affecting the relation
                    bool found = false;
                    for(auto iter: relationHistory[0]){
                        if(iter.first.transactionId<v.from || iter.first.transactionId>v.to) continue;
                        found = true;
                        break;
                    }
                    if(found){
                        pthread_mutex_lock( &mutexQueryResults );
                        queryResults[v.validationId]=true;
                        pthread_mutex_unlock( &mutexQueryResults );
                    }
                    //Remove query
                    queriesToProcess->pop_back();
                    continue;
                }

                //Adds all the matching candidates for first predicate
                Query::Column * firstPredic = &columns[0];
                auto * columnHistory = &relationHistory[firstPredic->column];
                for(auto iter=columnHistory->begin(); iter!=columnHistory->end(); ++iter){
                    //If the query is not in range, continue
                    if(iter->first.transactionId<v.from || iter->first.transactionId>v.to) continue;

                    uint64_t tupleValue = iter->second, queryValue=firstPredic->value;
                    bool result=false;
                    switch (firstPredic->op) {
                        case Query::Column::Equal: result=(tupleValue==queryValue); break;
                        case Query::Column::NotEqual: result=(tupleValue!=queryValue); break;
                        case Query::Column::Less: result=(tupleValue<queryValue); break;
                        case Query::Column::LessOrEqual: result=(tupleValue<=queryValue); break;
                        case Query::Column::Greater: result=(tupleValue>queryValue); break;
                        case Query::Column::GreaterOrEqual: result=(tupleValue>=queryValue); break;
                    }
                    if(result){
                        candidates.push_back(iter->first);
                    }
                }

                //Loops through the other predicates
                for (auto c=columns.begin(); c!=columns.end(); ++c) {
                    if(c==columns.begin()) continue; //TODO remove

                    //Loops through the candidates to eliminate non-matching
                    uint64_t queryValue=c->value;
                    for(auto iter=candidates.begin(); iter!=candidates.end();){
                        bool result=false;
                        uint64_t tupleValue=relationHistory[c->column][*iter];
                        switch (c->op) {
                            case Query::Column::Equal: result=(tupleValue==queryValue); break;
                            case Query::Column::NotEqual: result=(tupleValue!=queryValue); break;
                            case Query::Column::Less: result=(tupleValue<queryValue); break;
                            case Query::Column::LessOrEqual: result=(tupleValue<=queryValue); break;
                            case Query::Column::Greater: result=(tupleValue>queryValue); break;
                            case Query::Column::GreaterOrEqual: result=(tupleValue>=queryValue); break;
                        }
                        if (!result){
                            candidates.erase(iter);
                        }else{
                            ++iter;
                        }
                    }
                }

                //Updates result
                if(!candidates.empty()){
                    pthread_mutex_lock( &mutexQueryResults );
                    queryResults[v.validationId]=true;
                    pthread_mutex_unlock( &mutexQueryResults );
                }

                /*
                 * Ends thread query handling
                 */
            }

            //Erase the query from the list
            queriesToProcess->pop_back();
        }else{
            //Sends signal to tell main thread processing is done
            pthread_mutex_lock( mut );
            pthread_cond_signal( cond );
            pthread_mutex_unlock( mut );

            //Waits for the main thread signal
            pthread_mutex_lock( mut );
            pthread_cond_wait( cond, mut );
            if(*endThread){
                pthread_mutex_unlock( mut );
                exit(EXIT_SUCCESS);
            }
            pthread_mutex_unlock( mut );

        }
    }
    //Should never get there
    return NULL;
}
//---------------------------------------------------------------------------
int main()
{
    //Instanciates four threads to process the queries
    pthread_t thread1, thread2, thread3, thread4;
    int thread1Id = 1, thread2Id = 2, thread3Id = 3, thread4Id = 4;
    if( pthread_create( &thread1, NULL, launchThread, (void*) &thread1Id)
            || pthread_create( &thread2, NULL, launchThread, (void*) &thread2Id)
            || pthread_create( &thread3, NULL, launchThread, (void*) &thread3Id)
            || pthread_create( &thread4, NULL, launchThread, (void*) &thread4Id)) {
        exit(EXIT_FAILURE);
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
                //Tells the threads to exit
                pthread_mutex_lock( &mut1 );
                pthread_mutex_lock( &mut2 );
                pthread_mutex_lock( &mut3 );
                pthread_mutex_lock( &mut4 );
                endThread1 = true;
                endThread2 = true;
                endThread3 = true;
                endThread4 = true;
                pthread_cond_signal( &cond1 );
                pthread_cond_signal( &cond2 );
                pthread_cond_signal( &cond3 );
                pthread_cond_signal( &cond4 );
                pthread_mutex_unlock( &mut1 );
                pthread_mutex_unlock( &mut2 );
                pthread_mutex_unlock( &mut3 );
                pthread_mutex_unlock( &mut4 );
                //Waits for the four threads to end
                pthread_join(thread1, NULL);
                pthread_join(thread2, NULL);
                pthread_join(thread3, NULL);
                pthread_join(thread4, NULL);
                return 0;
            default:
                // crude error handling, should never happen
                cerr << "malformed message" << endl; abort();
        }
    }
}
//---------------------------------------------------------------------------
