#ifndef FORGETTHREAD_H
#define FORGETTHREAD_H

#include "reference.h"

class ForgetThread {

public:
    ForgetThread(uint32_t thread){
        this->thread = thread;
    }
    ~ForgetThread(){
    }

    //Function to use for thread
    void launch();

private:
    uint32_t thread;
    //Aliases
    transactionHistory_t * transactionHistory;
    TupleCBuffer * tupleContent;
    //Function to process the forget
    void processForget();
};

#endif
