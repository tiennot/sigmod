#include "forgetthread.h"

void ForgetThread::launch(){
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
    Tuple bound = forgetTupleBound;
    for(uint32_t rel=0; rel!=schema.size(); ++rel){
        if(assignedThread(rel)!=thread) continue;
        auto * colMaps = &(*transactionHistoryPtr)[rel];
        for(auto iterCol=colMaps->begin(); iterCol!=colMaps->end(); ++iterCol){
            auto map = iterCol->first;
            auto * mapVector = iterCol->second;
            mapVector->clear();
            for(auto iterMap=map->begin(); iterMap!=map->end();){
                vector<Tuple> * tuples = &(iterMap->second);
                auto lwrBound = lower_bound(tuples->begin(), tuples->end(), bound);
                if(lwrBound!=tuples->end()){
                    tuples->erase(tuples->begin(), lwrBound);
                    mapVector->push_back(iterMap->first);
                    ++iterMap;
                }else{
                    auto toErase = iterMap;
                    ++iterMap;
                    //Erase the pair in the map
                    map->erase(toErase);
                }
            }
        }
    }
}
