#include "tuplecbuffer.h"

//In case capacity is not enough
void TupleCBuffer::reallocate(){
    //Reallocate with double capacity
    vector<vector<uint64_t>> * newContainer = new vector<vector<uint64_t>>;
    newContainer->resize(capacity*2);
    //TODO: vraiment pas optimal
    for(Tuple t=firstTuple; t!=firstTuple+size; ++t){
        (*newContainer)[t%(capacity*2)] = move((*container)[t%capacity]);
    }
    delete container;
    container = newContainer;
    capacity *= 2;
}

//Called from Forget
void TupleCBuffer::forget(uint64_t transactionId){
    //Looks for the forget in mapper
    Tuple firstTupleToKeep = (*tMapper)[transactionId];
    if(firstTupleToKeep > firstTuple && firstTupleToKeep < firstTuple+size){
        size = size - (firstTupleToKeep - firstTuple);
        firstTuple = firstTupleToKeep;
    }else if(firstTupleToKeep >= firstTuple+size){
        firstTuple = firstTuple + size;
        size = 0;
    }
}

//Adds a tupleValues to the buffer and returns tuple id
void TupleCBuffer::push_back(const vector<uint64_t> &tValues){
    if(size==capacity) reallocate();
    (*container)[(firstTuple+size)%capacity]=tValues;
    ++size;
}

//Access an element
const vector<uint64_t>& TupleCBuffer::at(const Tuple &t) const{
    return (*container)[t%capacity];
}

//Adds an entry to tMapper
void TupleCBuffer::addTEntry(const uint64_t &transactionId){
    if(transactionId >= tMapper->size()){
        tMapper->resize(transactionId+1);
    }
    //Shouldn't happen
    if(transactionId!=lastMappedTransaction+1 && transactionId!=0){
        //Fills the gap
        for(uint64_t t=lastMappedTransaction+1; t!=transactionId; ++t){
            (*tMapper)[t] = firstTuple+size;
        }
    }
    (*tMapper)[transactionId] = firstTuple+size;
    lastMappedTransaction = transactionId;
}

//Gets forget tuple correspondant to tid
Tuple TupleCBuffer::getForgetTuple(const uint64_t &transactionId) const{
    return (*tMapper)[transactionId];
}

Tuple TupleCBuffer::getTupleFrom(const uint64_t &transactionId) const{
    return (*tMapper)[transactionId];
}

Tuple TupleCBuffer::getTupleTo(const uint64_t &transactionId) const{
    if(transactionId >= lastMappedTransaction){
        return firstTuple+size;
    }
    return (*tMapper)[transactionId+1];
}
