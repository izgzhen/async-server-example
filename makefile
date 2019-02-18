CC = gcc
CXX = g++
CFLAGS = -Wall -Werror
RM = rm -rf
LDFLAGS = -pthread

SRCS = 550server.cpp
OBJS = $(SRCS:.cpp=.o)
EXE = 550server

all: $(SRCS) $(EXE)

$(EXE): $(OBJS)
	$(CXX) $(CFLAGS) $(LDFLAGS) $(OBJS) -o $@

.cpp.o:
	$(CXX) -c $(CFLAGS) $< -o $@

client: client.c
	$(CC) -o client client.c

clean:
	$(RM) $(EXE) $(OBJS) client
