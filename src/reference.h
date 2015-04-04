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

#ifndef REFERENCE_H
#define REFERENCE_H

#include <iostream>
#include <map>
#include <unordered_map>
#include <vector>
#include <chrono>
#include <set>
#include <cassert>
#include <cstdint>
#include <functional>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <unistd.h>
#include <string.h>
#include "tuplecbuffer.h"
#include "pcqueue.h"

#define NB_THREAD 8

using namespace std;

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
struct DefineSchema {
    /// Number of relations
    uint32_t relationCount;
    /// Column counts per relation, one count per relation. The first column is always the primary key
    uint32_t columnCounts[];
};
struct Transaction {
    /// The transaction id. Monotonic increasing
    uint64_t transactionId;
    /// The operation counts
    uint32_t deleteCount,insertCount;
    /// A sequence of transaction operations. Deletes first, total deleteCount+insertCount operations
    char operations[];
};
struct TransactionOperationDelete {
    /// The affected relation
    uint32_t relationId;
    /// The row count
    uint32_t rowCount;
    /// The deleted values, rowCount primary keyss
    uint64_t keys[];
};
struct TransactionOperationInsert {
    /// The affected relation
    uint32_t relationId;
    /// The row count
    uint32_t rowCount;
    /// The inserted values, rowCount*relation[relationId].columnCount values
    uint64_t values[];
};
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
struct Flush {
    /// All validations to this id (including) must be answered
    uint64_t validationId;
};
struct Forget {
    /// Transactions older than that (including) will not be tested for
    uint64_t transactionId;
};

//---------------------------------------------------------------------------
//A structure to keep "stats" for each unique column
//---------------------------------------------------------------------------
struct UColFigures {
    //The nb of distinct values that affected
    uint64_t nbOfValues;
    //The total nb of tuple that affected
    uint64_t nbOfTuples;
    //The minimum value that affected the column
    uint64_t minValue;
    //The maximum unique value that affected the column
    uint64_t maxValue;

    //Constructor
    UColFigures(): nbOfValues(0), nbOfTuples(0), minValue(UINT64_MAX), maxValue(0) {}

    //Give the estimated number of tuples
    uint64_t estimateNbTuples(Query::Column * column){
        if(minValue==maxValue) return nbOfTuples;
        switch(column->op){
        case Query::Column::Greater:
        case Query::Column::GreaterOrEqual:
            if(column->value > maxValue) return 0;
            return nbOfTuples*(maxValue-column->value); // /(maxValue-minValue);
        case Query::Column::Less:
        case Query::Column::LessOrEqual:
            if(column->value < minValue) return 0;
            return nbOfTuples*(column->value-minValue); // /(maxValue-minValue);
        case Query::Column::NotEqual:
            return nbOfTuples * (maxValue - minValue);
        case Query::Column::Equal:
            if(column->value > maxValue || column->value < minValue) return 0;
            return nbOfTuples * (maxValue - minValue) / nbOfValues;
        default:
            //Should never happen
            return nbOfTuples * (maxValue - minValue);
        }
    }
};

//---------------------------------------------------------------------------
//Define types
//---------------------------------------------------------------------------
typedef vector<vector<pair<unordered_map<uint64_t, vector<Tuple>> *, vector<uint64_t> * >>> transactionHistory_t;
typedef vector<pair<ValidationQueries, pair<Query, vector<Query::Column>>>> queriesToProcess_t;
typedef PCQueue<pair<uint32_t, pair<Tuple, vector<uint64_t>>>> tuplesToIndex_t;
typedef vector<vector<UColFigures>> uColIndicator_t;

//---------------------------------------------------------------------------
//Extern declarations
//---------------------------------------------------------------------------
extern vector<uint32_t> schema;
extern vector<unordered_map<uint32_t,vector<uint64_t>>> relations;
extern TupleCBuffer * tupleContentPtr;
extern transactionHistory_t * transactionHistoryPtr;
extern pair<vector<bool>, uint64_t> queryResults;
extern mutex mutexQueryResults;
extern queriesToProcess_t * queriesToProcessPtr[];
extern tuplesToIndex_t * tuplesToIndexPtr[];
extern uColIndicator_t * uColIndicatorPtr;
extern atomic<uint32_t> processingFlushThreadsNb, processingForgetThreadsNb;
extern condition_variable_any conditionFlush, conditionForget;
extern mutex mutexFlush, mutexForget;
extern atomic<bool> referenceOver;
extern atomic<uint64_t> forgetTupleBound;

//---------------------------------------------------------------------------
// Given an iterator to a Tuple object and a vector of Column, tells if match
//---------------------------------------------------------------------------
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
// Given a relation id, tells which thread is in charge
//---------------------------------------------------------------------------
inline static uint32_t assignedThread(uint32_t relationId){
    return relationId % NB_THREAD; //Better if NB_THREAD is a power of 2
}

#endif
