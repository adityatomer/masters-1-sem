CC=gcc

CFLAGS=-c 
all: hw3

hw3: pbproxy.o
	$(CC) pbproxy.o -o pbproxy -lcrypto

pbproxy.o: pbproxy.c
	$(CC) $(CFLAGS) pbproxy.c

clean:
	rm -rf *o file