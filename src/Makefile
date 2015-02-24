all: reference

reference: reference.cpp
	g++ -pthread -std=c++11 -O3 -g -Wall -Wextra -Werror -o$@ reference.cpp

clean:
	rm reference
