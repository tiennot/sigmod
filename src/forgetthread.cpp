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
    for(auto iterRel=transactionHistory->begin(); iterRel!=transactionHistory->end(); ++iterRel){
        for(auto iterCol=iterRel->begin(); iterCol!=iterRel->end(); ++iterCol){
            auto map = *iterCol;
            for(auto iterMap=map->begin(); iterMap!=map->end();){
                vector<Tuple> * tuples = &(iterMap->second);
                auto lwrBound = lower_bound(tuples->begin(), tuples->end(), bound);
                if(lwrBound!=tuples->end()){
                    tuples->erase(tuples->begin(), lwrBound);
                    ++iterMap;
                }else{
                    auto toErase = iterMap;
                    ++iterMap;
                    map->erase(toErase);
                }
            }
        }
    }
    //Erase from tupleContent
    tupleContent->erase(tupleContent->begin(), tupleContent->lower_bound(bound));
}
