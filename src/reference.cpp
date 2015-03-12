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
#include <functional>
#include <thread>
#include <mutex>
#include <unistd.h>
using namespace std;
//---------------------------------------------------------------------------

//Declaration for thread handling functions
void flushThread(uint32_t thread);
void forgetThread(uint32_t thread, uint64_t transactionId);

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

    bool operator!=(const Tuple& tuple) const{
        return transactionId!=tuple.transactionId || internId!=tuple.internId;
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
        return column == col.column && relationId == col.relationId;
    }
};

//Number of threads
const static uint32_t nbThreads = 8;

//Stores the schema
static vector<uint32_t> schema;

//Stores the content of the relations (i.e. tuples)
static vector<map<uint32_t,vector<uint64_t>>> relations;

//Maps tuples to their content
static map<Tuple, vector<uint64_t>> * tupleContentPtr[nbThreads];

//Maps a relation's column and a value to the tuples that affected it
static map<UniqueColumn, map<uint64_t, vector<Tuple>>> * transactionHistoryPtr[nbThreads];

//Stores the booleans for output + mutex
static map<uint64_t,bool> queryResults;
mutex mutexQueryResults;

//Lists of (validationQuery, (query, columns)) to be processed by each thread
static vector<pair<ValidationQueries, pair<Query, vector<Query::Column>>>> * queriesToProcessPtr[nbThreads];

//---------------------------------------------------------------------------
//Function that tells which thread handles a given relation
inline static uint32_t assignedThread(uint32_t relationId){
    return relationId % nbThreads; //Better if nbThreads is a power of 2
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
                vector<uint64_t> tupleValues = relations[o->relationId][*key];
                //For each column we add the value to the history
                for(uint32_t col=0; col!=schema[o->relationId]; ++col){
                    UniqueColumn uCol{o->relationId, col};
                    (*transactionHistoryPtr[thread])[uCol][tupleValues[col]].push_back(tuple);

                }
                //Move to tupleContent and erase
                (*tupleContentPtr[thread])[tuple]=move(tupleValues);
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
            vector<uint64_t> tupleValues;
            tupleValues.insert(tupleValues.begin(),values,values+schema[o->relationId]);
            //For each column we add the value to the history
            for(uint32_t col=0; col!=schema[o->relationId]; ++col){
                UniqueColumn uCol{o->relationId, col};
                (*transactionHistoryPtr[thread])[uCol][tupleValues[col]].push_back(tuple);
            }
            //Adds to tupleContent and inserts
            (*tupleContentPtr[thread])[tuple]= tupleValues;
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
        uint32_t nbPredicPerCol[schema[q.relationId]] = {0};
        for (auto c=q.columns,cLimit=c+columnCount;c!=cLimit;++c){
            ++nbPredicPerCol[c->column];
            vColumns.push_back(*c);
        }

        //Prevents "useless" queries from being added to the processing queue
        bool notToBePushed = false;
        for(uint32_t i=0; i<schema[q.relationId]; ++i){
            if(nbPredicPerCol[i]>1){
                const Query::Column::Op * op = NULL;
                uint64_t value = 0;
                for (auto c=q.columns,cLimit=c+columnCount;c!=cLimit;++c){
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
            auto thread = assignedThread(q.relationId);
            queriesToProcessPtr[thread]->push_back(pair<ValidationQueries, pair<Query, vector<Query::Column>>>(v, pair<Query, vector<Query::Column>>(q, move(vColumns))));
        }else{
            queryResults[v.validationId] = false;
        }

        //Offsets reader
        reader+=sizeof(Query)+(sizeof(Query::Column)*columnCount);
    }
}
//---------------------------------------------------------------------------
static void processFlush(const Flush& f)
{
    cerr << "Flush " << f.validationId << endl;
    //Instanciates threads in the pool
    vector<thread> threadPool;
    for(uint32_t i=0; i!=nbThreads; ++i){
        threadPool.push_back(thread(flushThread, i));
    }

    //Waits for the threads to end
    while(!threadPool.empty()){
        threadPool.back().join();
        threadPool.pop_back();
    }

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
    //Instanciates threads in the pool
    vector<thread> threadPool;
    for(uint32_t i=0; i!=nbThreads; ++i){
        threadPool.push_back(thread(forgetThread, i, f.transactionId));
    }
    //Waits for the threads to end
    while(!threadPool.empty()){
        threadPool.back().join();
        threadPool.pop_back();
    }
}
//---------------------------------------------------------------------------
//Function that handle threads used to process forget
void forgetThread(uint32_t thread, uint64_t transactionId){
    Tuple bound{transactionId, 0};
    //Erase from transaction history
    auto& transactionHistory = *(transactionHistoryPtr[thread]);
    for(auto iter=transactionHistory.begin(); iter!=transactionHistory.end(); ++iter){
        auto * secondMap = &(iter->second);
        for(auto iter2=secondMap->begin(); iter2!=secondMap->end(); ++iter2){
            vector<Tuple> * tuples = &(iter2->second);
            tuples->erase(tuples->begin(), lower_bound(tuples->begin(), tuples->end(), bound));
        }
    }
    //Erase from tupleContent
    auto& tupleContent = *(tupleContentPtr[thread]);
    tupleContent.erase(tupleContent.begin(), tupleContent.lower_bound(bound));
    pthread_exit(EXIT_SUCCESS);
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
            case Query::Column::Less: result=(tupleValue<queryValue); break;
            case Query::Column::LessOrEqual: result=(tupleValue<=queryValue); break;
            case Query::Column::Greater: result=(tupleValue>queryValue); break;
            case Query::Column::GreaterOrEqual: result=(tupleValue>=queryValue); break;
            case Query::Column::NotEqual: result=(tupleValue!=queryValue); break;
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
void flushThread(uint32_t thread){
    //Defines aliases for variables according to thread
    map<UniqueColumn, map<uint64_t, vector<Tuple>>>& transactionHistory = *(transactionHistoryPtr[thread]);
    map<Tuple, vector<uint64_t>>& tupleContent = *(tupleContentPtr[thread]);
    vector<pair<ValidationQueries, pair<Query, vector<Query::Column>>>>& queriesToProcess = *(queriesToProcessPtr[thread]);

    //Starts working
    while(!queriesToProcess.empty()){
        //Gets the query from the list
        auto& back = queriesToProcess.back();
        ValidationQueries * v = &(back.first);

        Query * q = &(back.second.first);
        vector<Query::Column> * columns = &(back.second.second);

        //Retrieves current result
        mutexQueryResults.lock();

        if(!queryResults.count(v->validationId)) queryResults[v->validationId]=false;
        bool currentResult = queryResults[v->validationId]==true;

        mutexQueryResults.unlock();

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
                    mutexQueryResults.lock();
                    queryResults[v->validationId]=true;
                    mutexQueryResults.unlock();
                }
                //Remove query
                queriesToProcess.pop_back();
                continue;
            }

            //Looks for the "right" strategy to test the tuples
            vector<Query::Column*> eCol; //Vector containing the equal columns
            eCol.reserve(q->columnCount);

            for(auto pIter=columns->begin(); pIter!=columns->end(); ++pIter){
                if(pIter->op==Query::Column::Equal){
                    eCol.push_back(&(*pIter));
                }
            }

            Tuple tFrom{v->from, 0};
            Tuple tTo{v->to+1, 0};

            //The most common case is when there is at least one equal column
            if(!eCol.empty()){
                //The case of 1 predic, ==
                if(q->columnCount==1){
                    auto filterPredic = &((*columns)[0]);
                    UniqueColumn firstUCol = UniqueColumn{q->relationId, filterPredic->column};
                    auto& tupleList = transactionHistory[firstUCol][filterPredic->value];
                    auto tupleFrom = lower_bound(tupleList.begin(), tupleList.end(), tFrom);
                    auto tupleTo = lower_bound(tupleFrom, tupleList.end(), tTo);
                    if(tupleFrom!=tupleTo){
                        mutexQueryResults.lock();
                        queryResults[v->validationId]=true;
                        mutexQueryResults.unlock();
                    }
                    //Remove query
                    queriesToProcess.pop_back();
                    continue;
                }

                uint32_t eColNb = eCol.size();
                vector<Tuple>::iterator tupleFrom[eColNb];
                vector<Tuple>::iterator tupleTo[eColNb];

                bool foundEmptyList = false;
                for(uint32_t i=0; i!=eColNb; ++i){
                    UniqueColumn firstUCol = UniqueColumn{q->relationId, eCol[i]->column};
                    auto tupleList = &(transactionHistory[firstUCol][eCol[i]->value]);
                    tupleFrom[i] = lower_bound(tupleList->begin(), tupleList->end(), tFrom);
                    tupleTo[i] = lower_bound(tupleFrom[i], tupleList->end(), tTo);
                    if(tupleFrom[i]==tupleTo[i]){
                        foundEmptyList = true;
                        break;
                    }
                }

                if(foundEmptyList){
                    queriesToProcess.pop_back();
                    continue;
                }

                vector<Tuple>::iterator iter[eColNb];
                //Initializes the iterators
                for(uint32_t i=0; i!=eColNb; ++i){
                    iter[i] = tupleFrom[i];
                }

                //Goes through the iterators
                bool endWhile = false;
                while(true){
                    //Adjusts the iterators
                    bool continueWhile = false;
                    for(uint32_t i=1; i!=eColNb; ++i){
                        if(*(iter[0]) != *(iter[i])){
                            if(*(iter[i]) < *(iter[0])){
                                ++iter[i];
                                if(iter[i]==tupleTo[i]) endWhile = true;
                            }else{
                                ++iter[0];
                                if(iter[0]==tupleTo[0]) endWhile = true;
                            }
                            continueWhile=true;
                            break;
                        }
                    }
                    if(endWhile) break;
                    if(continueWhile) continue;

                    //Proceeds test
                    auto& tupleValues = tupleContent[*(iter[0])];
                    if(tupleMatch(tupleValues, columns)==true){
                        foundSomeone = true;
                        break;
                    }

                    //Increments iterators
                    for(uint32_t i=0; i!=eColNb; ++i){
                        ++iter[i];
                        if(iter[i]==tupleTo[i]){
                            endWhile = true;
                            break;
                        }
                    }
                    if(endWhile) break;
                }
            }
            //The other cases are more difficult
            else{
                //We will iterate in the relevant values
                Query::Column * filterPredic = NULL;
                for(auto pIter=columns->begin(); pIter!=columns->end(); ++pIter){
                    if(pIter->op!=Query::Column::NotEqual){
                        filterPredic = &(*pIter);
                        break;
                    }
                }
                if(filterPredic==NULL) filterPredic = &((*columns)[0]);

                UniqueColumn firstUCol = UniqueColumn{q->relationId, filterPredic->column};

                const bool notEqualCase = filterPredic->op==Query::Column::NotEqual;
                auto tupleListStart = transactionHistory[firstUCol].begin();
                auto tupleListEnd = transactionHistory[firstUCol].end();

                if(filterPredic->op==Query::Column::Greater){
                    tupleListStart = transactionHistory[firstUCol].upper_bound(filterPredic->value);
                }else if(filterPredic->op==Query::Column::GreaterOrEqual){
                    tupleListStart = transactionHistory[firstUCol].lower_bound(filterPredic->value);
                }else if(filterPredic->op==Query::Column::Less){
                    tupleListEnd = transactionHistory[firstUCol].lower_bound(filterPredic->value);
                }else if(filterPredic->op==Query::Column::LessOrEqual){
                    tupleListEnd = transactionHistory[firstUCol].upper_bound(filterPredic->value);
                }

                //Iterates through the values
                for(auto iter=tupleListStart; iter!=tupleListEnd && !foundSomeone; ++iter){
                    //The not equal special case
                    if(notEqualCase && iter->first==filterPredic->value) continue;

                    auto& tupleList = transactionHistory[firstUCol][iter->first];
                    auto tupleFrom = lower_bound(tupleList.begin(), tupleList.end(), tFrom);
                    auto tupleTo = lower_bound(tupleFrom, tupleList.end(), tTo);
                    //Loops through tuples and checks them
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
                mutexQueryResults.lock();
                queryResults[v->validationId]=true;
                mutexQueryResults.unlock();
            }

            /*
             * Ends thread query handling
             */
        }

        //Erase the query from the list
        queriesToProcess.pop_back();
    }
    pthread_exit(EXIT_SUCCESS);
}
//---------------------------------------------------------------------------
int main()
{
    //Allocates maps
    for(uint32_t thread=0; thread!=nbThreads; ++thread){
        transactionHistoryPtr[thread] = new map<UniqueColumn, map<uint64_t, vector<Tuple>>>;
        tupleContentPtr[thread] = new map<Tuple, vector<uint64_t>>;
        queriesToProcessPtr[thread] = new vector<pair<ValidationQueries, pair<Query, vector<Query::Column>>>>;
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
                //cerr << "Pause... "; sleep(15); cerr << "Resumes..." << endl;
                processDefineSchema(readBody<DefineSchema>(cin,message,head.messageLen));
                break;
            case MessageHead::Done:

                //Desallocates
                for(uint32_t thread=0; thread!=nbThreads; ++thread){
                    delete transactionHistoryPtr[thread];
                    delete tupleContentPtr[thread];
                    delete queriesToProcessPtr[thread];
                }
                return 0;
            default:
                // crude error handling, should never happen
                cerr << "malformed message" << endl; abort();
        }
    }
}
//---------------------------------------------------------------------------
