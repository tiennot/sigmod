#include "forgetthread.h"

void ForgetThread::launch(){
    //Aliases
    transactionHistory = transactionHistoryPtr[thread];
    tupleContent = tupleContentPtr[thread];

    mutexForget.lock();
    while(true){
        //Waits for the signal from main thread
        processingForgetThreadsNb++;
        conditionForget.wait(mutexForget);
        mutexForget.unlock();

        //Checks if the program is over
        if(referenceOver) break;

        //Actual forget work
        processForget();

        //When done processing decrements processingThreadNb
        if(--processingForgetThreadsNb==0){
            //Signals main thread
            mutexForget.lock();
            conditionForget.notify_all();
            mutexForget.unlock();
        }else{
            //waits for the releaser thread
            mutexForget.lock();
            conditionForget.wait(mutexForget);
        }
    }

    pthread_exit(EXIT_SUCCESS);
}

void ForgetThread::processForget(){
    Tuple bound{forgetTransactionId, 0};
    for(auto iter=transactionHistory->begin(); iter!=transactionHistory->end(); ++iter){
        auto * secondMap = &(iter->second);
        for(auto iter2=secondMap->begin(); iter2!=secondMap->end();){
            vector<Tuple> * tuples = &(iter2->second);
            tuples->erase(tuples->begin(), lower_bound(tuples->begin(), tuples->end(), bound));
            if(tuples->empty()){
                auto toErase = iter2;
                ++iter2;
                secondMap->erase(toErase);
            }else{
                ++iter2;
            }
        }
    }
    //Erase from tupleContent
    tupleContent->erase(tupleContent->begin(), tupleContent->lower_bound(bound));
}
