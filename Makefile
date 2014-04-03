###############################################
#	
#	Makefile for the burst trie project.
#	Amal Cao (amalcao@tju.edu.cn)
#	Since	Mar. 22nd. 2009
#	BSD OPEN SOURCE LICENSE
#
#	Copyright (c) 2009~???? TJU-FIT & TJU-CADC
#
###############################################

CC=gcc
CFLAGS?= -O2 

All:	contest test tags

tags:	contest test
	ctags -R *

test:	tests/speed_test.c	lib.so
	$(CC) $(CFLAGS)  tests/speed_test.c ./lib.so -lpthread -o tests/speed_test

contest:	lib.so	unittests.c
	$(CC) $(CFLAGS) unittests.c ./lib.so -lpthread -o contest

lib.so:	btimpl.o	burst_trie.o
	$(CC) $(CFLAGS) -shared -pthread *.o -o lib.so

%.o:	%.c
	$(CC) $(CFLAGS) -c -fPIC $<

clean:
	rm *.o *.so contest tests/speed_test tags 



