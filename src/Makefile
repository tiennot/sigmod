all: reference

reference: reference.cpp
	g++ -std=c++11 -pthread -O3 -g -Wall -Wextra -Werror reference.cpp tuplecbuffer.cpp flushthread.cpp forgetthread.cpp -o reference

clean:
	rm reference
