# makefile - CS 4390 P2P project, Group 17
# Usage:  make -f makefile   builds ./tracker and the same peer program into peer1/, peer2/, peer3/
#         make clean         removes the programs before zipping (no compiled files in the submission)

CC     = gcc
CFLAGS = -Wall -pthread

all: tracker peers

tracker: tracker.c protocol.h
	$(CC) $(CFLAGS) -o tracker tracker.c

peers: peer.c protocol.h
	$(CC) $(CFLAGS) -o peer1/peer peer.c
	$(CC) $(CFLAGS) -o peer2/peer peer.c
	$(CC) $(CFLAGS) -o peer3/peer peer.c

clean:
	rm -f tracker peer1/peer peer2/peer peer3/peer *.o

.PHONY: all peers clean
