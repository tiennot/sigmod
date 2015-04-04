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
    //Function to process the forget
    void processForget();
};

#endif
