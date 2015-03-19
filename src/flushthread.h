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
    tupleContent_t * tupleContent;
    queriesToProcess_t * queriesToProcess;
    tuplesToIndex_t * tuplesToIndex;
    //Process the queries from the queue
    void processQueries();
    //Index the tuples from the queue
    void indexTuples();
    //Processing functions
    void processQuery_NoColumn();
    void processQuery_OneEqualOnly();
    void processQuery_WithEqualColumns();
    void processQuery_WithNoEqualColumns();
    //Attributes for query processing
    ValidationQueries * v;
    Query * q;
    vector<Query::Column> * columns;
    bool foundSomeone;
    Tuple tFrom{0,0};
    Tuple tTo{0,0};
    vector<Query::Column*>  eCol;
};

#endif
