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
    //Process the queries from the queue
    void processQueries();
    //Index the tuples from the queue
    void indexTuples();
};

#endif
