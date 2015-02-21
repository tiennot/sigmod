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
//Stores the content of the relations (i.e. tuples)
static vector<map<uint32_t,vector<uint64_t>>> relations;

//Maps the Id of a transaction with a pair containing relationId, tuple values
static map<uint64_t,vector<pair<uint32_t,vector<uint64_t>>>> transactionHistory;
//Stores the booleans for output
static map<uint64_t,bool> queryResults;

//Store for each relationId the transactions that affected the relation
static map<uint32_t, vector<uint64_t>> relationEditor;

//Lists of (validationId, query) to be processed by the second thread + mutex
static vector<pair<uint64_t, Query>> queriesSecondThread;
pthread_mutex_t mutexQueries = PTHREAD_MUTEX_INITIALIZER;

//Condition variable before flush
pthread_cond_t  readyToFlush   = PTHREAD_COND_INITIALIZER;
pthread_mutex_t mutexFlush = PTHREAD_MUTEX_INITIALIZER;

//---------------------------------------------------------------------------
static void processDefineSchema(const DefineSchema& d)
{
    schema.clear();
    //Inserts the number of columns for each relation
    //e.g. schema = {23, 37, 43, 16, 17, 5, 15, 19, 47, ...}
    schema.insert(schema.begin(),d.columnCounts,d.columnCounts+d.relationCount);
    relations.clear();
    relations.resize(d.relationCount);
}
//---------------------------------------------------------------------------
static void processTransaction(const Transaction& t)
{
    vector<pair<uint32_t,vector<uint64_t>>> operations;
    const char* reader=t.operations;

    //Keeps track of the relations we updated
    set<uint32_t> relationsEdited;

    // Delete all indicated tuples
    for (uint32_t index=0;index!=t.deleteCount;++index) {
        const TransactionOperationDelete* o= (const TransactionOperationDelete*) reader;
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

    //Register transaction as editor
    for(auto iter : relationsEdited){
        relationEditor[iter].push_back(t.transactionId);
    }
    //Register transaction
    transactionHistory[t.transactionId]=move(operations);
}
//---------------------------------------------------------------------------
static void processValidationQueries(const ValidationQueries& v)
{
    bool conflict=false;
    const char* reader=v.queries;
    for (unsigned index=0;index!=v.queryCount;++index) {
        auto& q=*reinterpret_cast<const Query*>(reader);

        //Adds the query to the query list for second thread
        pthread_mutex_lock( &mutexQueries );
        queriesSecondThread.push_back(pair<uint64_t, Query>(v.validationId,q));
        pthread_mutex_unlock( &mutexQueries );

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
                for (auto c=q.columns,cLimit=c+q.columnCount;c!=cLimit;++c) {
                    uint64_t tupleValue=tuple[c->column],queryValue=c->value;
                    bool result=false;
                    switch (c->op) {
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
        reader+=sizeof(Query)+(sizeof(Query::Column)*q.columnCount);
    }

    queryResults[v.validationId]=conflict;
}
//---------------------------------------------------------------------------
static void processFlush(const Flush& f)
{
    //The main thread waits for the second one to finish processing all the queries
    pthread_mutex_lock( &mutexFlush );
    pthread_cond_wait( &readyToFlush, &mutexFlush );
    pthread_mutex_unlock( &mutexFlush );

    //Outputs all the queryResults available
    while ((!queryResults.empty())&&((*queryResults.begin()).first<=f.validationId)) {
        char c='0'+(*queryResults.begin()).second;
        cout.write(&c,1);
        queryResults.erase(queryResults.begin());
    }
    cout.flush();
}
//---------------------------------------------------------------------------
static void processForget(const Forget& f)
{
    //Erase from transaction history
    while ((!transactionHistory.empty())&&((*transactionHistory.begin()).first<=f.transactionId))
        transactionHistory.erase(transactionHistory.begin());
    //Erase from relation editors
    for(auto iter : relationEditor){
        vector<uint64_t> editors = iter.second;
        while(!editors.empty() && *(editors.begin())<= f.transactionId){
            editors.erase(editors.begin());
        }
    }
}
//---------------------------------------------------------------------------
// Read the message body and cast it to the desired type
template<typename Type> static const Type& readBody(istream& in,vector<char>& buffer,uint32_t len) {
    buffer.resize(len);
    in.read(buffer.data(),len);
    return *reinterpret_cast<const Type*>(buffer.data());
}
//---------------------------------------------------------------------------
static unsigned int nbQueryHandled = 0;
static bool noMoreQuery = false;
//Second thread function
void *secondThread(void *ptr){
    ptr = ptr;
    while(true){
        pthread_mutex_lock( &mutexQueries );
        if(queriesSecondThread.size()){
            //Gets the query from the list
            /*
            uint64_t queryId = queriesSecondThread[0].first;
            Query q = queriesSecondThread[0].second;
            clog << queryId << " (" << q.relationId << ")" << endl;
            */
            pthread_mutex_unlock( &mutexQueries );

            //Handles the query
            ++nbQueryHandled;

            //Erase the query from the list
            pthread_mutex_lock( &mutexQueries );
            queriesSecondThread.erase(queriesSecondThread.begin());
        }else{
            //Uses condition variable to tell the main thread size==0
            pthread_mutex_lock( &mutexFlush );
            pthread_cond_signal( &readyToFlush );
            pthread_mutex_unlock( &mutexFlush );

            //If the program is done
            if(noMoreQuery) break;
        }
        pthread_mutex_unlock( &mutexQueries );
    }
    return NULL;
}
//---------------------------------------------------------------------------
int main()
{
    //Initializes the second thread
    pthread_t second_thread;
    int threadId = 1;
    if(pthread_create( &second_thread, NULL, secondThread, (void*) &threadId)) {
        clog << "Error while creating the thread" << endl;
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
                //Tells the second thread it's over
                noMoreQuery = true;
                //Wait for the second thread to end
                pthread_join(second_thread, NULL);
                clog << "Nb query handled: " << nbQueryHandled << endl;
                clog << "Size of the query list: " << queriesSecondThread.size() << endl;
                return 0;
            default:
                // crude error handling, should never happen
                cerr << "malformed message" << endl; abort();
        }
    }
}
//---------------------------------------------------------------------------
