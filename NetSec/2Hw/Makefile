CC=gcc

CFLAGS=-c 
all: compile

compile: 2hw.o
	$(CC) 2hw.o -o 2hw -lpcap

2hw.o: 2hw.c
	$(CC) $(CFLAGS) 2hw.c

clean:
	rm -rf *o file