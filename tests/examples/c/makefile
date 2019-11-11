# Copyright (c) 2017 by PowerDB Technologies, Inc.
# todo: library dependency, header file dependency

ROOT=./
TARGET=exe
LFLAGS = '-Wl,-rpath,/usr/local/power/driver' -lpower -lpthread -lm -lrt
CFLAGS = -O3 -g -Wall -Wno-deprecated -fPIC -Wno-unused-result -Wconversion -Wno-char-subscripts -D_REENTRANT -Wno-format -D_REENTRANT -DLINUX -msse4.2 -Wno-unused-function -D_M_X64 -std=gnu99

all: $(TARGET)

exe:
	gcc $(CFLAGS) ./demo.c -o $(ROOT)/demo $(LFLAGS)

clean:
	rm $(ROOT)demo

	