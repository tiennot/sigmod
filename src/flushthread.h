#ifndef FLUSHTHREAD_H
#define FLUSHTHREAD_H

#include "reference.h"

class FlushThread {

public:
    FlushThread(uint32_t thread){
        this->thread = thread;
    }
    ~FlushThread(){
    }

    //Function to use for thread
    void launch();

private:
    //Thread id
    uint32_t thread;
    //Aliases
    transactionHistory_t * transactionHistory;
    TupleCBuffer * tupleContent;
    queriesToProcess_t * queriesToProcess;
    tuplesToIndex_t * tuplesToIndex;
    uColIndicator_t * uColIndicator;
    //Process the queries from the queue
    void processQueries();
    //Index the tuples from the queue
    void indexTuples();
    //Processing functions
    bool processQuery_NoColumn() const;
    bool processQuery_OneEqualOnly() const;
    bool processQuery_WithEqualColumns() const;
    bool processQuery_WithNoEqualColumns() const;
    bool testMapForValue(unordered_map<uint64_t, vector<uint64_t>> * map, vector<Query::Column> * columns, const uint64_t &value) const;
    //Attributes for query processing
    ValidationQueries * v;
    Query * q;
    vector<Query::Column> * columns;
    bool foundSomeone;
    Tuple tFrom = 0;
    Tuple tTo = 0;
    vector<Query::Column*>  eCol;
};

#endif
