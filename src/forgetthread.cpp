#include "forgetthread.h"

void ForgetThread::launch(){
    //Alias for transaction history
    auto& transactionHistory = *(transactionHistoryPtr[thread]);

    mutexForget.lock();
    while(true){
        //Waits for the signal from main thread
        processingForgetThreadsNb++;
        conditionForget.wait(mutexForget);
        mutexForget.unlock();

        if(referenceOver) break;

        /* Actual forget work */

        Tuple bound{forgetTransactionId, 0};
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

        /* End of actual forget work */

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
