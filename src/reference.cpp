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
#include <chrono>
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

//A structure to identify a unique tuple
struct Tuple {
    uint64_t transactionId; //Id of the transaction
    uint64_t internId; //Index of the insertion/deletion

    bool operator<(const Tuple& tuple) const{
        return transactionId < tuple.transactionId ||
                (transactionId==tuple.transactionId && internId<tuple.internId);
    }

    bool operator==(const Tuple& tuple) const{
        return transactionId==tuple.transactionId && internId==tuple.internId;
    }

    friend ostream &operator<<(ostream &out, Tuple tuple){
        out << "Tuple{TransId=" << tuple.transactionId;
        out << " InternId=" << tuple.internId << "}";
        return out;
    }
};

//A structure to identify a unique column
struct UniqueColumn {
    uint32_t relationId; //Id of the relation
    uint32_t column; //The column number

    bool operator<(const UniqueColumn& col) const{
        return relationId < col.relationId
                || (relationId==col.relationId && column<col.column);
    }

    bool operator==(const UniqueColumn& col) const{
        return relationId == col.relationId && column == col.column;
    }
};

//Stores the schema
static vector<uint32_t> schema;

//Stores the content of the relations (i.e. tuples) + mutex
static vector<map<uint32_t,vector<uint64_t>>> relations;

//Maps one relation to one thread
static vector<AuxThread> assignedThreads;

//Maps tuples to their content
static map<Tuple, vector<uint64_t>>
    tupleContent1, tupleContent2, tupleContent3, tupleContent4;

//Maps a relation's column and a value to the tuples that affected it
static map<UniqueColumn, map<uint64_t, vector<Tuple>>>
    transactionHistory1, transactionHistory2, transactionHistory3, transactionHistory4;

//Stores the booleans for output + mutex
static map<uint64_t,bool> queryResults;
pthread_mutex_t mutexQueryResults = PTHREAD_MUTEX_INITIALIZER;

//Lists of (validationQuery, (query, columns)) to be processed by each thread
static vector<pair<ValidationQueries, pair<Query, vector<Query::Column>>>>
    queriesToProcess1, queriesToProcess2, queriesToProcess3, queriesToProcess4;
//---------------------------------------------------------------------------
//Function that tells which thread handles a given relation
inline static AuxThread assignedThread(uint32_t relationId){
    return assignedThreads[relationId]; //TODO return relationId%4 ???
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
    assignedThreads.resize(d.relationCount);
    for(uint32_t i=0; i<d.relationCount; ++i){
        if(i<d.relationCount/4) assignedThreads[i]=Thread1;
        else if(i<d.relationCount/2) assignedThreads[i]=Thread2;
        else if(i<3*d.relationCount/4) assignedThreads[i]=Thread3;
        else assignedThreads[i]=Thread4;
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

        //Switch according to the thread assigned
        AuxThread thread = assignedThread(o->relationId);
        map<UniqueColumn, map<uint64_t, vector<Tuple>>> * transactionHistory = NULL;
        map<Tuple, vector<uint64_t>> * tupleContent = NULL;
        switch(thread){
            case Thread1:
                transactionHistory = &transactionHistory1;
                tupleContent = &tupleContent1;
                break;
            case Thread2:
                transactionHistory = &transactionHistory2;
                tupleContent = &tupleContent2;
                break;
            case Thread3:
                transactionHistory = &transactionHistory3;
                tupleContent = &tupleContent3;
                break;
            default:
                transactionHistory = &transactionHistory4;
                tupleContent = &tupleContent4;
        }

        //Loops through the tuples to delete
        for (const uint64_t* key=o->keys,*keyLimit=key+o->rowCount;key!=keyLimit;++key) {
            //If the tuple key exists in the relation
            if (relations[o->relationId].count(*key)) {
                vector<uint64_t> tupleValues = relations[o->relationId][*key];
                //For each column we add the value to the history
                for(uint32_t col=0; col!=schema[o->relationId]; ++col){
                    UniqueColumn uCol{o->relationId, col};
                    (*transactionHistory)[uCol][tupleValues[col]].push_back(tuple);

                }
                //Move to tupleContent and erase
                (*tupleContent)[tuple]=move(tupleValues);
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

        //Switch according to the thread assigned
        AuxThread thread = assignedThread(o->relationId);
        map<UniqueColumn, map<uint64_t, vector<Tuple>>> * transactionHistory = NULL;
        map<Tuple, vector<uint64_t>> * tupleContent = NULL;
        switch(thread){
            case Thread1:
                transactionHistory = &transactionHistory1;
                tupleContent = &tupleContent1;
                break;
            case Thread2:
                transactionHistory = &transactionHistory2;
                tupleContent = &tupleContent2;
                break;
            case Thread3:
                transactionHistory = &transactionHistory3;
                tupleContent = &tupleContent3;
                break;
            default:
                transactionHistory = &transactionHistory4;
                tupleContent = &tupleContent4;
        }

        //Loops through the tuples to insert
        for (const uint64_t* values=o->values,*valuesLimit=values+(o->rowCount*schema[o->relationId]);values!=valuesLimit;values+=schema[o->relationId]) {
            vector<uint64_t> tupleValues;
            tupleValues.insert(tupleValues.begin(),values,values+schema[o->relationId]);
            //For each column we add the value to the history
            for(uint32_t col=0; col!=schema[o->relationId]; ++col){
                UniqueColumn uCol{o->relationId, col};
                (*transactionHistory)[uCol][tupleValues[col]].push_back(tuple);
            }
            //Adds to tupleContent and inserts
            (*tupleContent)[tuple]= tupleValues;
            relations[o->relationId][values[0]]=move(tupleValues);
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
    //Instanciates four threads to process the queries
    pthread_t thread1, thread2, thread3, thread4;
    int thread1Id = 1, thread2Id = 2, thread3Id = 3, thread4Id = 4;
    if( pthread_create( &thread1, NULL, launchThread, (void*) &thread1Id)
            || pthread_create( &thread2, NULL, launchThread, (void*) &thread2Id)
            || pthread_create( &thread3, NULL, launchThread, (void*) &thread3Id)
            || pthread_create( &thread4, NULL, launchThread, (void*) &thread4Id)) {
        exit(EXIT_FAILURE);
    }

    //Waits for the four threads to end
    pthread_join(thread1, NULL);
    pthread_join(thread2, NULL);
    pthread_join(thread3, NULL);
    pthread_join(thread4, NULL);

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
    Tuple bound{f.transactionId, 0};

    //Erase from transaction history of thread1
    for(auto iter=transactionHistory1.begin(); iter!=transactionHistory1.end(); ++iter){
        auto * secondMap = &(iter->second);
        for(auto iter2=secondMap->begin(); iter2!=secondMap->end(); ++iter2){
            vector<Tuple> * tuples = &(iter2->second);
            tuples->erase(tuples->begin(), lower_bound(tuples->begin(), tuples->end(), bound));
        }
    }

    //Erase from transaction history of thread2
    for(auto iter=transactionHistory2.begin(); iter!=transactionHistory2.end(); ++iter){
        auto * secondMap = &(iter->second);
        for(auto iter2=secondMap->begin(); iter2!=secondMap->end(); ++iter2){
            vector<Tuple> * tuples = &(iter2->second);
            tuples->erase(tuples->begin(), lower_bound(tuples->begin(), tuples->end(), bound));
        }
    }

    //Erase from transaction history of thread3
    for(auto iter=transactionHistory3.begin(); iter!=transactionHistory3.end(); ++iter){
        auto * secondMap = &(iter->second);
        for(auto iter2=secondMap->begin(); iter2!=secondMap->end(); ++iter2){
            vector<Tuple> * tuples = &(iter2->second);
            tuples->erase(tuples->begin(), lower_bound(tuples->begin(), tuples->end(), bound));
        }
    }

    //Erase from transaction history of thread4
    for(auto iter=transactionHistory4.begin(); iter!=transactionHistory4.end(); ++iter){
        auto * secondMap = &(iter->second);
        for(auto iter2=secondMap->begin(); iter2!=secondMap->end(); ++iter2){
            vector<Tuple> * tuples = &(iter2->second);
            tuples->erase(tuples->begin(), lower_bound(tuples->begin(), tuples->end(), bound));
        }
    }

    //Erases from the tupleContent maps
    tupleContent1.erase(tupleContent1.begin(), tupleContent1.lower_bound(bound));
    tupleContent2.erase(tupleContent2.begin(), tupleContent2.lower_bound(bound));
    tupleContent3.erase(tupleContent3.begin(), tupleContent3.lower_bound(bound));
    tupleContent4.erase(tupleContent4.begin(), tupleContent4.lower_bound(bound));

}
//---------------------------------------------------------------------------
// Read the message body and cast it to the desired type
template<typename Type> static const Type& readBody(istream& in,vector<char>& buffer,uint32_t len) {
    buffer.resize(len);
    in.read(buffer.data(),len);
    return *reinterpret_cast<const Type*>(buffer.data());
}
//---------------------------------------------------------------------------
//Given an iterator to a Tuple object and a vector of Column, tells if match
inline bool tupleMatch(const vector<uint64_t> &tupleValues, vector<Query::Column> * columns){
    bool match = true;
    for (auto c=columns->begin(); c!=columns->end(); ++c) {
        uint64_t tupleValue = tupleValues[c->column];
        uint64_t queryValue = c->value;

        bool result=false;
        switch (c->op) {
            case Query::Column::Equal: result=(tupleValue==queryValue); break;
            case Query::Column::NotEqual: result=(tupleValue!=queryValue); break;
            case Query::Column::Less: result=(tupleValue<queryValue); break;
            case Query::Column::LessOrEqual: result=(tupleValue<=queryValue); break;
            case Query::Column::Greater: result=(tupleValue>queryValue); break;
            case Query::Column::GreaterOrEqual: result=(tupleValue>=queryValue); break;
        }
        if (!result) {
            match = false;
            break;
        }
    }
    return match;
}
//---------------------------------------------------------------------------
//Function that handle behavior of aux threads
void *launchThread(void *ptr){
    //Retrieves the id of the thread
    int threadId = *((int*) ptr);

    //Defines aliases for variables according to threadId
    vector<pair<ValidationQueries, pair<Query, vector<Query::Column>>>> * queriesToProcess = NULL;
    map<Tuple, vector<uint64_t>> * tupleContentPtr = NULL;
    map<UniqueColumn, map<uint64_t, vector<Tuple>>> * transactionHistoryPtr = NULL;

    switch(threadId){
        case 1:
            queriesToProcess = &queriesToProcess1;
            tupleContentPtr = &tupleContent1;
            transactionHistoryPtr = &transactionHistory1;
            break;
        case 2:
            queriesToProcess = &queriesToProcess2;
            tupleContentPtr = &tupleContent2;
            transactionHistoryPtr = &transactionHistory2;
            break;
        case 3:
            queriesToProcess = &queriesToProcess3;
            tupleContentPtr = &tupleContent3;
            transactionHistoryPtr = &transactionHistory3;
            break;
        case 4:
            queriesToProcess = &queriesToProcess4;
            tupleContentPtr = &tupleContent4;
            transactionHistoryPtr = &transactionHistory4;
            break;
    }

    map<UniqueColumn, map<uint64_t, vector<Tuple>>>& transactionHistory = *transactionHistoryPtr;
    map<Tuple, vector<uint64_t>>& tupleContent = *tupleContentPtr;

    //Starts working
    while(true){
        if(!queriesToProcess->empty()){
            //Gets the query from the list
            auto back = queriesToProcess->back();
            ValidationQueries * v = &(back.first);
            Query * q = &(back.second.first);
            vector<Query::Column> * columns = &(back.second.second);

            //Retrieves current result
            pthread_mutex_lock( &mutexQueryResults );
            if(!queryResults.count(v->validationId)) queryResults[v->validationId]=false;
            bool currentResult = queryResults[v->validationId]==true;
            pthread_mutex_unlock( &mutexQueryResults );

            //Handles query only if current result is false
            if(!currentResult){

                /*
                 * Thread query handle begins
                 */

                register bool foundSomeone = false;

                //If there is no column (shouldn't happen often)
                if(q->columnCount==0){
                    UniqueColumn uCol{q->relationId, 0};
                    //Query conflicts if there is at least one transaction affecting the relation
                    bool found = false;
                    Tuple tFrom{v->from, 0};
                    for(auto iter: transactionHistory[uCol]){
                        auto lowerBound = lower_bound(iter.second.begin(), iter.second.end(), tFrom);
                        if(lowerBound!=iter.second.end() && (*lowerBound).transactionId<=v->to){
                            found=true;
                            break;
                        }
                    }
                    if(found){
                        pthread_mutex_lock( &mutexQueryResults );
                        queryResults[v->validationId]=true;
                        pthread_mutex_unlock( &mutexQueryResults );
                    }
                    //Remove query
                    queriesToProcess->pop_back();
                    continue;
                }

                //Looks for the "right" predicate to use first
                Query::Column * filterPredic = &((*columns)[0]); //TODO change that
                for(auto pIter=columns->begin(); pIter!=columns->end(); ++pIter){
                    if(pIter->op==Query::Column::Equal){
                        filterPredic = &(*pIter);
                        break;
                    }
                    if(pIter->op!=Query::Column::NotEqual){
                        filterPredic = &(*pIter);
                    }
                }

                //Checks the matching candidates for first predicate

                UniqueColumn firstUCol{q->relationId, filterPredic->column};
                Tuple tFrom{v->from, 0};
                Tuple tTo{v->to+1, 0};

                //The most common case is equal
                if(filterPredic->op==Query::Column::Equal){
                    auto& tupleList = transactionHistory[firstUCol][filterPredic->value];
                    auto tupleFrom = lower_bound(tupleList.begin(), tupleList.end(), tFrom);
                    auto tupleTo = lower_bound(tupleFrom, tupleList.end(), tTo);
                    //Loops through tuples and adds them to the candidates
                    for(auto iter=tupleFrom; iter!=tupleTo; ++iter){
                        auto& tupleValues = tupleContent[*iter];
                        if(tupleMatch(tupleValues, columns)==true){
                            foundSomeone = true;
                            break;
                        }
                    }
                //The other cases are more difficult
                }else{
                    //We will iterate in the relevant values
                    const bool notEqualCase = filterPredic->op==Query::Column::NotEqual;
                    auto tupleListStart = transactionHistory[firstUCol].begin();
                    auto tupleListEnd = transactionHistory[firstUCol].end();

                    if(filterPredic->op==Query::Column::Greater){
                        tupleListStart = transactionHistory[firstUCol].lower_bound(filterPredic->value+1);
                    }else if(filterPredic->op==Query::Column::GreaterOrEqual){
                        tupleListStart = transactionHistory[firstUCol].lower_bound(filterPredic->value);
                    }else if(filterPredic->op==Query::Column::Less){
                        tupleListEnd = transactionHistory[firstUCol].lower_bound(filterPredic->value);
                    }else if(filterPredic->op==Query::Column::LessOrEqual){
                        tupleListEnd = transactionHistory[firstUCol].lower_bound(filterPredic->value+1);
                    }

                    //Iterates through the values
                    for(auto iter=tupleListStart; iter!=tupleListEnd && !foundSomeone; ++iter){
                        //The not equal special case
                        if(notEqualCase && iter->first==filterPredic->value) continue;

                        auto& tupleList = transactionHistory[firstUCol][iter->first];
                        auto tupleFrom = lower_bound(tupleList.begin(), tupleList.end(), tFrom);
                        auto tupleTo = lower_bound(tupleFrom, tupleList.end(), tTo);
                        //Loops through tuples and adds them to the candidates
                        for(auto iter2=tupleFrom; iter2!=tupleTo; ++iter2){
                            auto& tupleValues = tupleContent[*iter2];
                            if(tupleMatch(tupleValues, columns)==true){
                                foundSomeone = true;
                                break;
                            }
                        }
                    }
                }


                //Updates result
                if(foundSomeone){
                    pthread_mutex_lock( &mutexQueryResults );
                    queryResults[v->validationId]=true;
                    pthread_mutex_unlock( &mutexQueryResults );
                }

                /*
                 * Ends thread query handling
                 */
            }

            //Erase the query from the list
            queriesToProcess->pop_back();
        }else{
            pthread_exit(EXIT_SUCCESS);
        }
    }
    //Should never get there
    return NULL;
}
//---------------------------------------------------------------------------
int main()
{
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
                return 0;
            default:
                // crude error handling, should never happen
                cerr << "malformed message" << endl; abort();
        }
    }
}
//---------------------------------------------------------------------------
