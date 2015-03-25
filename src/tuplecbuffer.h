#ifndef TUPLECBUFFER_H
#define TUPLECBUFFER_H

#include <iostream>
#include <map>
#include <vector>
#include <cstdint>
#include <unistd.h>
#include <string.h>

typedef uint64_t Tuple;

using namespace std;

class TupleCBuffer {

public:
    //Constructor
    TupleCBuffer(uint64_t capacity){
        container = new vector<vector<uint64_t>>;
        tMapper = new vector<Tuple>;
        container->resize(capacity);
        firstTuple = 0;
        this->capacity = capacity;
        this->size = 0;
    }
    //Destructor
    ~TupleCBuffer(){
        delete container;
        delete tMapper;
    }

    //Called by ForgetThread
    void forget(uint64_t transactionId);
    //Returns the values for a tuple
    void push_back(const vector<uint64_t> &tValues);
    const vector<uint64_t>& at(const Tuple &t) const;

    //Adds an entry to tMapper
    void addTEntry(const uint64_t &transactionId);

    //Returns the tuple bound for forgetting
    Tuple getForgetTuple(const uint64_t &transactionId) const;

    //Returns tupleTo and TupleFrom (used in FlushThread)
    Tuple getTupleFrom(const uint64_t &transactionId) const;
    Tuple getTupleTo(const uint64_t &transactionId) const;

private:
    Tuple firstTuple;
    uint64_t size, capacity;
    //Used when number of tuples becomes greater than size
    void reallocate();
    //Underlying container
    vector<vector<uint64_t>> * container;
    //Keeps the equivalence transactionId <--> Tuple
    vector<Tuple> * tMapper;
    uint64_t lastMappedTransaction = 0;
};

#endif
