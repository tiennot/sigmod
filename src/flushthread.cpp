#include "flushthread.h"

/*
 * Launch the thread, i.e. called only once
 */
void FlushThread::launch(){
    //Maps aliases for variables according to thread
    transactionHistory = transactionHistoryPtr[thread];
    tupleContent = tupleContentPtr;
    queriesToProcess = queriesToProcessPtr[thread];
    tuplesToIndex = tuplesToIndexPtr[thread];
    uColIndicator = uColIndicatorPtr[thread];

    mutexFlush.lock();
    while(true){
        //Waits for the signal from main thread
        processingFlushThreadsNb++;
        conditionFlush.wait(mutexFlush);
        mutexFlush.unlock();

        if(referenceOver) break;

        //Indexes
        indexTuples();
        //Processes
        processQueries();

        //When done processing decrements processingThreadNb
        if(--processingFlushThreadsNb==0){
            //Signals main thread
            mutexFlush.lock();
            conditionFlush.notify_all();
            mutexFlush.unlock();
        }else{
            //waits for the releaser thread
            mutexFlush.lock();
            conditionFlush.wait(mutexFlush);
        }
    }

    pthread_exit(EXIT_SUCCESS);
}

/*
 * Index all the tuples from tuplesToIndex
 */
void FlushThread::indexTuples(){
    for(auto iter=tuplesToIndex->begin(), iterEnd=tuplesToIndex->end(); iter!=iterEnd; ++iter){
        auto relationId = iter->first;
        //For each column we add the value to the history
        for(uint32_t col=0, nbCol=iter->second.second.size(); col!=nbCol; ++col){
            uint64_t value = iter->second.second[col];
            auto tupleList = &(*(*transactionHistory)[relationId][col])[value];
            //Updates stats
            UColFigures  * uColFigures = &((*uColIndicator)[relationId][col]);
            if(value > uColFigures->maxValue) uColFigures->maxValue = value;
            if(value < uColFigures->minValue) uColFigures->minValue = value;
            if(tupleList->empty()) ++uColFigures->nbOfValues;
            ++uColFigures->nbOfTuples;
            //Adds the value to the list
            tupleList->push_back(iter->second.first);
        }
    }
    //Clear the queue of tuples to index
    tuplesToIndex->clear();
}

/*
 * Process all the queries from queriesToProcess
 */
void FlushThread::processQueries(){
    //Starts working
    while(!queriesToProcess->empty()){
        //Gets the query from the list
        auto& back = queriesToProcess->back();
        v = &(back.first);
        q = &(back.second.first);
        columns = &(back.second.second);

        //Bound tuples
        tFrom = tupleContentPtr->getTupleFrom(v->from);
        tTo = tupleContentPtr->getTupleTo(v->to);

        //Retrieves current result
        mutexQueryResults.lock();
        bool currentResult = queryResults.first[v->validationId]==true;
        mutexQueryResults.unlock();

        //Handles query only if current result is false
        if(!currentResult){

            foundSomeone = false;

            //If there is no column (shouldn't happen often)
            if(q->columnCount==0){
                foundSomeone = processQuery_NoColumn();
            }

            //The one predicate, == case
            else if(q->columnCount==1 && columns->begin()->op==Query::Column::Equal){
                foundSomeone = processQuery_OneEqualOnly();
            }

            //Other cases
            else{
                //Build vector of == predics
                eCol.clear();
                for(auto pIter=columns->begin(); pIter!=columns->end(); ++pIter){
                    if(pIter->op==Query::Column::Equal){
                        eCol.push_back(&(*pIter));
                    }
                }
                //Call according to number of == predic
                if(!eCol.empty()){
                    foundSomeone = processQuery_WithEqualColumns();
                }else{
                    foundSomeone = processQuery_WithNoEqualColumns();
                }
            }

            //Updates result if needed
            if(foundSomeone){
                mutexQueryResults.lock();
                queryResults.first[v->validationId]=true;
                mutexQueryResults.unlock();
            }
        }

        //Erase the query from the list
        queriesToProcess->pop_back();
    }
}

/*
 * Process the query when there is no column (shouldn't happen very often)
 */
bool FlushThread::processQuery_NoColumn() const{
    //Query conflicts if there is at least one transaction affecting the relation
    for(auto iter: *(*transactionHistory)[q->relationId][0]){
        auto lowerBound = lower_bound(iter.second.begin(), iter.second.end(), tFrom);
        if(lowerBound!=iter.second.end() && (*lowerBound)<tTo){
            return true;
        }
    }
    return false;
}

/*
 * Process the query when the only predic is ==
 */
bool FlushThread::processQuery_OneEqualOnly() const{
    auto filterPredic = &((*columns)[0]);
    auto map = (*transactionHistory)[q->relationId][filterPredic->column];
    auto tupleList = map->find(filterPredic->value);

    if(tupleList!= map->end()){
        auto tupleFrom = lower_bound(tupleList->second.begin(), tupleList->second.end(), tFrom);
        auto tupleTo = lower_bound(tupleFrom, tupleList->second.end(), tTo);
        if(tupleFrom!=tupleTo){
            return true;
        }
    }
    return false;
}

/*
 * Process the query when there is at least one == predicate
 */
bool FlushThread::processQuery_WithEqualColumns() const{
    vector<Tuple> * tupleList = NULL;
    uint32_t nbTuples = UINT32_MAX;

    for(uint32_t i=0; i!=eCol.size(); ++i){
        auto map = (*transactionHistory)[q->relationId][eCol[i]->column];
        auto tupleListCandidate = map->find(eCol[i]->value);
        if(tupleListCandidate!=map->end()){
            if(tupleListCandidate->second.size()<nbTuples){
                tupleList = &(tupleListCandidate->second);
                nbTuples = tupleList->size();
                if(nbTuples<128) break;
            }
        }else{
            //Empty list, returns
            return false;
        }
    }

    //Goes through the tupleList
    auto iterFrom = lower_bound(tupleList->begin(), tupleList->end(), tFrom);
    auto iterTo = lower_bound(iterFrom, tupleList->end(), tTo);
    for(auto iter=iterFrom; iter!=iterTo; ++iter){
        auto& tupleValues = tupleContent->at(*iter);
        if(tupleMatch(tupleValues, columns)){
            return true;
        }
    }
    return false;
}

/*
 * Process the query when there is no == predicate
 */
bool FlushThread::processQuery_WithNoEqualColumns() const{
    //We will iterate in the relevant values
    Query::Column * filterPredic = NULL;
    uint64_t estimatedNbOfTuples = UINT64_MAX;
    for(auto pIter=columns->begin(); pIter!=columns->end(); ++pIter){
        uint64_t newEstim = ((UColFigures) (*uColIndicator)[q->relationId][pIter->column]).estimateNbTuples(&(*pIter));
        if(newEstim<estimatedNbOfTuples){
            filterPredic = &(*pIter);
            estimatedNbOfTuples = newEstim;
            if(newEstim==0) break;
        }
    }
    if(filterPredic==NULL) filterPredic = &((*columns)[0]);

    auto map = (*transactionHistory)[q->relationId][filterPredic->column];

    const bool notEqualCase = filterPredic->op==Query::Column::NotEqual;
    auto tupleListStart = map->begin();
    auto tupleListEnd = map->end();

    if(filterPredic->op==Query::Column::Greater){
        tupleListStart = map->upper_bound(filterPredic->value);
    }else if(filterPredic->op==Query::Column::GreaterOrEqual){
        tupleListStart = map->lower_bound(filterPredic->value);
    }else if(filterPredic->op==Query::Column::Less){
        tupleListEnd = map->lower_bound(filterPredic->value);
    }else if(filterPredic->op==Query::Column::LessOrEqual){
        tupleListEnd = map->upper_bound(filterPredic->value);
    }

    //Iterates through the values
    for(auto iter=tupleListStart; iter!=tupleListEnd && !foundSomeone; ++iter){
        //The not equal special case
        if(notEqualCase && iter->first==filterPredic->value) continue;

        auto * tupleList = &(iter->second);
        auto tupleFrom = lower_bound(tupleList->begin(), tupleList->end(), tFrom);
        auto tupleTo = lower_bound(tupleFrom, tupleList->end(), tTo);
        //Loops through tuples and checks them
        for(auto iter2=tupleFrom; iter2!=tupleTo; ++iter2){
            auto& tupleValues = tupleContent->at(*iter2);
            if(tupleMatch(tupleValues, columns)==true){
                return true;
            }
        }
    }
    return false;
}
