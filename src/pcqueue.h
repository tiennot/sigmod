#ifndef PCQUEUE_H
#define PCQUEUE_H

#include <iostream>
#include <vector>
#include <cstdint>
#include <thread>
#include <mutex>
#include <atomic>
#include <condition_variable>

using namespace std;

template <class T> class PCQueue {

public:
    //Constructor and destructor
    PCQueue(uint32_t capacity){
        this->capacity = capacity;
        this->size = 0;
        this->start = 0;
        this->end = 0;
        this->producerDone = false;
        //Allocates storage
        this->container = new vector<T>;
        this->container->resize(capacity);
    }
    ~PCQueue(){
        delete this->container;
    }
    //To be used by producer
    void push(const T element);
    void signalProducerDone();
    //To be used by consumer
    T * front() const;
    void pop();
    bool waitForItem();

private:
    atomic<bool> producerDone;
    //Position of first and last items in container
    uint32_t start, end;
    //Number of items, max number of items
    uint32_t size, capacity;
    mutex mutexSize;
    //Underlying container
    vector<T> * container;
    //Condition variables
    condition_variable_any pushCondition, waitCondition;
    //Resets all the parameters
    void reset();
};

//Should be used only by producer
template <class T> void PCQueue<T>::push(const T element){
    //If full waits for a pop
    mutexSize.lock();
    if(size==capacity){
        pushCondition.wait(mutexSize);
    }

    //Pushes at the end
    (*container)[end] = element;
    ++end;
    if(end==capacity){
        end = 0;
    }
    //Increases size
    ++size;
    //"Signals" to the consumer
    waitCondition.notify_one();
    mutexSize.unlock();
}

//Should be used only by producer
template <class T> void PCQueue<T>::signalProducerDone(){
    mutexSize.lock();
    producerDone = true;
    waitCondition.notify_one();
    mutexSize.unlock();
}

//Should be used only by consumer
template <class T> T * PCQueue<T>::front() const{
    //Returns pointer to first element
    return &(*container)[start];
}

//Should be used only by consumer
template <class T> void PCQueue<T>::pop(){
    mutexSize.lock();

    //Pops at the beginning
    ++start;
    if(start==capacity){
        start = 0;
    }

    //Decreases size
    if(size==capacity){
        pushCondition.notify_one();
    }
    --this->size;
    mutexSize.unlock();
}

/*
* Should be used only by consumer, waits and returns true
* if there is a new item to process, false if the producer
* is done adding items
*/
template <class T> bool PCQueue<T>::waitForItem(){
    mutexSize.lock();
    //Tests for item
    if(size!=0){
        mutexSize.unlock();
        return true;
    }
    //Test for end
    if(producerDone){
        mutexSize.unlock();
        this->reset();
        return false;
    }
    //Waits
    waitCondition.wait(mutexSize);
    mutexSize.unlock();
    return this->waitForItem();
}

//Resets everything
template <class T> void PCQueue<T>::reset(){
    mutexSize.lock();
    size = 0;
    producerDone = false;
    start = 0;
    end = 0;
    mutexSize.unlock();
}

#endif
