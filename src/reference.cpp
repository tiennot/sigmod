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
#include <list>
#include <set>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <pthread.h>
using namespace std;
//---------------------------------------------------------------------------


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

//Stores the schema
static vector<uint32_t> schema;

//Stores the content of the relations (i.e. tuples) + mutex
static vector<map<uint32_t,vector<uint64_t>>> relations;
pthread_mutex_t mutexRelations = PTHREAD_MUTEX_INITIALIZER;

//Maps the Id of a transaction with a pair containing relationId, tuple values + mutexes
static map<uint64_t,vector<pair<uint32_t,vector<uint64_t>>>> transactionHistory1;
static map<uint64_t,vector<pair<uint32_t,vector<uint64_t>>>> transactionHistory2;
pthread_mutex_t mutexTransactionHistory1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexTransactionHistory2 = PTHREAD_MUTEX_INITIALIZER;

//Stores the booleans for output + mutex
static map<uint64_t,bool> queryResults;
pthread_mutex_t mutexQueryResults = PTHREAD_MUTEX_INITIALIZER;

//Store for each relationId the transactions that affected the relation + mutexes
static map<uint32_t, vector<uint64_t>> relationEditor1;
static map<uint32_t, vector<uint64_t>> relationEditor2;
pthread_mutex_t mutexRelationEditor1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexRelationEditor2 = PTHREAD_MUTEX_INITIALIZER;

//Lists of (validationQuery, (query, columns)) to be processed by each thread + mutexes
static vector<pair<ValidationQueries, pair<Query, vector<Query::Column>>>> queriesToProcess1;
static vector<pair<ValidationQueries, pair<Query, vector<Query::Column>>>> queriesToProcess2;
pthread_mutex_t mutexQueriesToProcess1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexQueriesToProcess2 = PTHREAD_MUTEX_INITIALIZER;

//Condition variable before flush for each thread + mutexes
pthread_cond_t  readyToFlush1   = PTHREAD_COND_INITIALIZER;
pthread_cond_t  readyToFlush2   = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutexReadyToFlush1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexReadyToFlush2 = PTHREAD_MUTEX_INITIALIZER;

//Booleans to tells if the threads are waiting + mutexes
static bool waiting1 = false;
static bool waiting2 = false;
pthread_mutex_t mutexWaiting1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexWaiting2 = PTHREAD_MUTEX_INITIALIZER;

//Condition variable to tell threads their list is not empty + mutexes
pthread_cond_t  notEmpty1   = PTHREAD_COND_INITIALIZER;
pthread_cond_t  notEmpty2   = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutexNotEmpty1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexNotEmpty2 = PTHREAD_MUTEX_INITIALIZER;

//Set to true at end of execution + mutexes
static bool noMoreQuery1 = false;
static bool noMoreQuery2 = false;
pthread_mutex_t mutexNoMoreQuery1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutexNoMoreQuery2 = PTHREAD_MUTEX_INITIALIZER;

//---------------------------------------------------------------------------
//Function that tells which thread handle a given relation
inline static int assignedThread(uint32_t relationId){
    /*
     * 0 is the main thread which performs insertions, deletion, flush, etc.
     * 1 performs half the validations
     * 2 performs half the validations
     * Hence this function returns either 1 or 2 but should never return 0
     */
    return relationId<=15 ? 1 : 2;
}
//---------------------------------------------------------------------------
static void processDefineSchema(const DefineSchema& d)
{
    //Inserts the number of columns for each relation
    //e.g. schema = {23, 37, 43, 16, 17, 5, 15, 19, 47, ...}
    schema.clear();
    schema.insert(schema.begin(),d.columnCounts,d.columnCounts+d.relationCount);

    //Resizes relation vector
    pthread_mutex_lock( &mutexRelations );
    relations.clear();
    relations.resize(d.relationCount);
    pthread_mutex_unlock( &mutexRelations );
}
//---------------------------------------------------------------------------
static void processTransaction(const Transaction& t)
{
    vector<pair<uint32_t,vector<uint64_t>>> operations1;
    vector<pair<uint32_t,vector<uint64_t>>> operations2;
    const char* reader=t.operations;

    //Keeps track of the relations we updated
    set<uint32_t> relationsEdited1;
    set<uint32_t> relationsEdited2;

    // Delete all indicated tuples
    for (uint32_t index=0;index!=t.deleteCount;++index) {
        const TransactionOperationDelete* o= (const TransactionOperationDelete*) reader;

        //Switch according to the thread assigned
        auto& operations = assignedThread(o->relationId)==1 ? operations1 : operations2;
        auto& relationsEdited = assignedThread(o->relationId)==1 ? relationsEdited1 : relationsEdited2;

        relationsEdited.insert(o->relationId);
        //Loops through the tuples to delete
        for (const uint64_t* key=o->keys,*keyLimit=key+o->rowCount;key!=keyLimit;++key) {
            //If the tuple key exists in the relation
            if (relations[o->relationId].count(*key)) {
                operations.push_back(pair<uint32_t,vector<uint64_t>>(o->relationId,move(relations[o->relationId][*key])));
                relations[o->relationId].erase(*key);
            }
        }
        reader+=sizeof(TransactionOperationDelete)+(sizeof(uint64_t)*o->rowCount);
    }

    // Insert new tuples
    for (uint32_t index=0;index!=t.insertCount;++index) {
        const TransactionOperationInsert* o= (const TransactionOperationInsert*) reader;

        //Switch according to the thread assigned
        auto& operations = assignedThread(o->relationId)==1 ? operations1 : operations2;
        auto& relationsEdited = assignedThread(o->relationId)==1 ? relationsEdited1 : relationsEdited2;

        relationsEdited.insert(o->relationId);
        //Loops through the tuples to insert
        for (const uint64_t* values=o->values,*valuesLimit=values+(o->rowCount*schema[o->relationId]);values!=valuesLimit;values+=schema[o->relationId]) {
            vector<uint64_t> tuple;
            tuple.insert(tuple.begin(),values,values+schema[o->relationId]);
            operations.push_back(pair<uint32_t,vector<uint64_t>>(o->relationId,tuple));
            relations[o->relationId][values[0]]=move(tuple);
        }
        reader+=sizeof(TransactionOperationInsert)+(sizeof(uint64_t)*o->rowCount*schema[o->relationId]);
    }

    //Register transaction as editor for thread 1
    pthread_mutex_lock( &mutexRelationEditor1 );
    for(auto iter : relationsEdited1){
        relationEditor1[iter].push_back(t.transactionId);
    }
    pthread_mutex_unlock( &mutexRelationEditor1 );

    //Register transaction for thread 1
    pthread_mutex_lock( &mutexTransactionHistory1 );
    transactionHistory1[t.transactionId]=move(operations1);
    pthread_mutex_unlock( &mutexTransactionHistory1 );

    //Register transaction as editor for thread 2
    pthread_mutex_lock( &mutexRelationEditor2 );
    for(auto iter : relationsEdited2){
        relationEditor2[iter].push_back(t.transactionId);
    }
    pthread_mutex_unlock( &mutexRelationEditor2 );

    //Register transaction for thread 2
    pthread_mutex_lock( &mutexTransactionHistory2 );
    transactionHistory2[t.transactionId]=move(operations2);
    pthread_mutex_unlock( &mutexTransactionHistory2 );
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
        if(assignedThread(q.relationId)==1){
            pthread_mutex_lock( &mutexQueriesToProcess1 );
            queriesToProcess1.push_back(pair<ValidationQueries, pair<Query, vector<Query::Column>>>(v, pair<Query, vector<Query::Column>>(q, vColumns)));
            pthread_mutex_unlock( &mutexQueriesToProcess1 );
            //Tells thread1
            pthread_mutex_lock( &mutexWaiting1 );
            if(waiting1){
                pthread_mutex_lock( &mutexNotEmpty1 );
                pthread_cond_signal( &notEmpty1 );
                pthread_mutex_unlock( &mutexNotEmpty1 );
            }
            pthread_mutex_unlock( &mutexWaiting1 );
        }else{
            pthread_mutex_lock( &mutexQueriesToProcess2 );
            queriesToProcess2.push_back(pair<ValidationQueries, pair<Query, vector<Query::Column>>>(v, pair<Query, vector<Query::Column>>(q, vColumns)));
            pthread_mutex_unlock( &mutexQueriesToProcess2 );
            //Tells thread2
            pthread_mutex_lock( &mutexWaiting2 );
            if(waiting2){
                pthread_mutex_lock( &mutexNotEmpty2 );
                pthread_cond_signal( &notEmpty2 );
                pthread_mutex_unlock( &mutexNotEmpty2 );
            }
            pthread_mutex_unlock( &mutexWaiting2 );
        }
        //Offsets reader
        reader+=sizeof(Query)+(sizeof(Query::Column)*columnCount);
    }
}
//---------------------------------------------------------------------------
static void processFlush(const Flush& f)
{
    //If thread1 is waiting we tell to proceed
    pthread_mutex_lock( &mutexWaiting1 );
    if(waiting1){
        pthread_mutex_lock( &mutexNotEmpty1 );
        pthread_cond_signal( &notEmpty1 );
        pthread_mutex_unlock( &mutexNotEmpty1 );
    }
    pthread_mutex_lock( &mutexReadyToFlush1 );
    pthread_mutex_unlock( &mutexWaiting1 );
    //The main thread waits for thread1
    pthread_cond_wait( &readyToFlush1, &mutexReadyToFlush1 );
    pthread_mutex_unlock( &mutexReadyToFlush1 );

    //If thread2 is waiting we tell to proceed
    pthread_mutex_lock( &mutexWaiting2 );
    if(waiting2){
        pthread_mutex_lock( &mutexNotEmpty2 );
        pthread_cond_signal( &notEmpty2 );
        pthread_mutex_unlock( &mutexNotEmpty2 );
    }
    pthread_mutex_lock( &mutexReadyToFlush2 );
    pthread_mutex_unlock( &mutexWaiting2 );
    //The main thread waits for thread2
    pthread_cond_wait( &readyToFlush2, &mutexReadyToFlush2 );
    pthread_mutex_unlock( &mutexReadyToFlush2 );

    //Outputs all the queryResults available
    pthread_mutex_lock( &mutexQueryResults );
    while ((!queryResults.empty())&&((*queryResults.begin()).first<=f.validationId)) {
        char c='0'+(*queryResults.begin()).second;
        cout.write(&c,1);
        queryResults.erase(queryResults.begin());
    }
    pthread_mutex_unlock( &mutexQueryResults );

    //Flush output
    cout.flush();
}
//---------------------------------------------------------------------------
static void processForget(const Forget& f)
{
    //Erase from transaction history of thread1
    pthread_mutex_lock( &mutexTransactionHistory1 );
    while ((!transactionHistory1.empty())&&((*transactionHistory1.begin()).first<=f.transactionId))
        transactionHistory1.erase(transactionHistory1.begin());
    pthread_mutex_unlock( &mutexTransactionHistory1 );

    //Erase from transaction history of thread2
    pthread_mutex_lock( &mutexTransactionHistory2 );
    while ((!transactionHistory2.empty())&&((*transactionHistory2.begin()).first<=f.transactionId))
        transactionHistory2.erase(transactionHistory2.begin());
    pthread_mutex_unlock( &mutexTransactionHistory2 );

    //Erase from relation editors for thread1
    pthread_mutex_lock( &mutexRelationEditor1 );
    for(auto iter : relationEditor1){
        vector<uint64_t> editors = iter.second;
        while(!editors.empty() && *(editors.begin())<= f.transactionId){
            editors.erase(editors.begin());
        }
    }
    pthread_mutex_unlock( &mutexRelationEditor1 );

    //Erase from relation editors for thread2
    pthread_mutex_lock( &mutexRelationEditor2 );
    for(auto iter : relationEditor2){
        vector<uint64_t> editors = iter.second;
        while(!editors.empty() && *(editors.begin())<= f.transactionId){
            editors.erase(editors.begin());
        }
    }
    pthread_mutex_unlock( &mutexRelationEditor2 );
}
//---------------------------------------------------------------------------
// Read the message body and cast it to the desired type
template<typename Type> static const Type& readBody(istream& in,vector<char>& buffer,uint32_t len) {
    buffer.resize(len);
    in.read(buffer.data(),len);
    return *reinterpret_cast<const Type*>(buffer.data());
}
//---------------------------------------------------------------------------
//Function that handle behavior of thread1
void *launchThread(void *ptr){
    //Retrieves the id of the thread
    int threadId = *((int*) ptr);
    clog << "Thread " << threadId << " just launched!" << endl;

    auto& mutexQueriesToProcess = threadId==1 ? mutexQueriesToProcess1 : mutexQueriesToProcess2;
    auto& queriesToProcess = threadId==1 ? queriesToProcess1 : queriesToProcess2;
    auto& mutexRelationEditor = threadId==1 ? mutexRelationEditor1 : mutexRelationEditor2;
    auto& relationEditor = threadId==1 ? relationEditor1 : relationEditor2;
    auto& mutexNoMoreQuery = threadId==1 ? mutexNoMoreQuery1 : mutexNoMoreQuery2;
    auto& noMoreQuery = threadId==1 ? noMoreQuery1 : noMoreQuery2;
    auto& mutexReadyToFlush = threadId==1 ? mutexReadyToFlush1 : mutexReadyToFlush2;
    auto& readyToFlush = threadId==1 ? readyToFlush1 : readyToFlush2;
    auto& mutexTransactionHistory = threadId==1 ? mutexTransactionHistory1 : mutexTransactionHistory2;
    auto& transactionHistory = threadId==1 ? transactionHistory1 : transactionHistory2;
    auto& mutexNotEmpty = threadId==1 ? mutexNotEmpty1 : mutexNotEmpty2;
    auto& notEmpty = threadId==1 ? notEmpty1 : notEmpty2;
    auto& mutexWaiting = threadId==1 ? mutexWaiting1 : mutexWaiting2;
    auto& waiting = threadId==1 ? waiting1 : waiting2;

    //Starts working
    while(true){  
        pthread_mutex_lock( &mutexQueriesToProcess );
        if(!queriesToProcess.empty()){
            //Gets the query from the list
            auto front = queriesToProcess.front();
            ValidationQueries v = move(front.first);
            Query q = move(front.second.first);
            vector<Query::Column> columns = move(front.second.second);

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

                pthread_mutex_lock( &mutexRelationEditor );
                pthread_mutex_lock( &mutexTransactionHistory );

                bool conflict=false;

                auto vector = relationEditor[q.relationId];
                auto from = lower_bound(vector.begin(), vector.end(), v.from);
                auto to = lower_bound(vector.begin(), vector.end(), v.to+1);

                //Loops through the transactions
                for (auto iter=from; iter!=to; ++iter) {
                    for (auto& op: transactionHistory[(*iter)]) {
                        // Check if the relation is the same
                        if (op.first!=q.relationId)
                        continue;

                        // Check if all predicates are satisfied
                        auto& tuple=op.second;
                        bool match=true;
                        for (Query::Column c: columns) {
                            uint64_t tupleValue=tuple[c.column],queryValue=c.value;
                            bool result=false;
                            switch (c.op) {
                                case Query::Column::Equal: result=(tupleValue==queryValue); break;
                                case Query::Column::NotEqual: result=(tupleValue!=queryValue); break;
                                case Query::Column::Less: result=(tupleValue<queryValue); break;
                                case Query::Column::LessOrEqual: result=(tupleValue<=queryValue); break;
                                case Query::Column::Greater: result=(tupleValue>queryValue); break;
                                case Query::Column::GreaterOrEqual: result=(tupleValue>=queryValue); break;
                            }
                            if (!result) { match=false; break; }
                        }
                        if (match) {
                            conflict=true;
                            break;
                        }
                    }
                    if(conflict) break;
                }

                pthread_mutex_unlock( &mutexRelationEditor );
                pthread_mutex_unlock( &mutexTransactionHistory );

                //Updates result
                if(conflict){
                    pthread_mutex_lock( &mutexQueryResults );
                    queryResults[v.validationId]=true;
                    pthread_mutex_unlock( &mutexQueryResults );
                }

                /*
                 * Ends thread query handling
                 */
            }

            //Erase the query from the list
            queriesToProcess.erase(queriesToProcess.begin());

            //Unlocks
            pthread_mutex_unlock( &mutexQueriesToProcess );
        }else{
            //Unlocks
            pthread_mutex_unlock( &mutexQueriesToProcess );

            //If the program is done
            pthread_mutex_lock( &mutexNoMoreQuery );
            if(noMoreQuery){
                clog << "Thread " << threadId << " about to exit..." << endl;
                pthread_mutex_unlock( &mutexNoMoreQuery );
                pthread_exit(NULL);
            }
            pthread_mutex_unlock( &mutexNoMoreQuery );

            //Uses condition variable to tell the main thread size==0
            pthread_mutex_lock( &mutexReadyToFlush );
            pthread_cond_signal( &readyToFlush );

            //Sets waiting to true to indicates its waiting
            pthread_mutex_lock( &mutexWaiting );
            pthread_mutex_unlock( &mutexReadyToFlush );
            waiting = true;

            //Waits for the signal to retry
            pthread_mutex_lock( &mutexNotEmpty );
            pthread_mutex_unlock( &mutexWaiting ); //A bit tricky: ensure no signal is sent between
            pthread_cond_wait( &notEmpty, &mutexNotEmpty );
            pthread_mutex_unlock( &mutexNotEmpty );

            //Sets waiting to false
            pthread_mutex_lock( &mutexWaiting );
            waiting = false;
            pthread_mutex_unlock( &mutexWaiting );
        }
    }
    return NULL;
}
//---------------------------------------------------------------------------
int main()
{
    //Initializes thread1 & thread2
    pthread_t thread1, thread2;
    int thread1Id = 1, thread2Id = 2;
    if( pthread_create( &thread1, NULL, launchThread, (void*) &thread1Id)
            || pthread_create( &thread2, NULL, launchThread, (void*) &thread2Id) ) {
        clog << "Error while creating the threads" << endl;
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
                //Tells thread1 it's over
                pthread_mutex_lock( &mutexNoMoreQuery1 );
                noMoreQuery1 = true;
                pthread_mutex_lock( &mutexNoMoreQuery1 );
                //Tells thread2 it's over
                pthread_mutex_lock( &mutexNoMoreQuery2 );
                noMoreQuery2 = true;
                pthread_mutex_lock( &mutexNoMoreQuery2 );
                //Wait for the threads to end
                pthread_join(thread1, NULL);
                pthread_join(thread2, NULL);
                return 0;
            default:
                // crude error handling, should never happen
                cerr << "malformed message" << endl; abort();
        }
    }
}
//---------------------------------------------------------------------------
