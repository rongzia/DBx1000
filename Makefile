CC=g++
CFLAGS=-Wall -g -std=c++17

.SUFFIXES: .o .cpp .h

SRC_DIRS = ./ ./benchmarks/ ./concurrency_control/ ./storage/ ./system/ ./rr/include/rr/ ./rdb/
INCLUDE = -I. -I./benchmarks -I./concurrency_control -I./storage -I./system -I./rr/include -I./include -I./rdb/

CFLAGS += $(INCLUDE) -D NOGRAPHITE=1 -Werror -Wno-unused-variable -Wno-unused-but-set-variable -O3
LDFLAGS = -Wall -L. -L./libs -pthread -g -lrt -std=c++0x -O3 -ljemalloc -ltbb
LDFLAGS += $(CFLAGS)

CPPS = $(foreach dir, $(SRC_DIRS), $(wildcard $(dir)*.cpp))
OBJS = $(CPPS:.cpp=.o)
DEPS = $(CPPS:.cpp=.d)

all:rundb

rundb : $(OBJS)
	$(CC) -o $@ $^ $(LDFLAGS)

-include $(OBJS:%.o=%.d)

%.d: %.cpp
	$(CC) -MM -MT $*.o -MF $@ $(CFLAGS) $<

%.o: %.cpp
	$(CC) -c $(CFLAGS) -o $@ $<

.PHONY: clean
clean:
	rm -f rundb $(OBJS) $(DEPS)
