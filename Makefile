CC=gcc
CFLAGS=-Wall -Wextra -fPIC -O2 -I./include
LFLAGS=-fPIC -shared -O2 -ldl -lrt
OBJS=obj/open.o obj/hook.o obj/dict.o obj/decode.o obj/itab.o obj/syn-att.o obj/syn-intel.o obj/syn.o obj/udis86.o obj/report_io.o obj/xxhash.o
HEADERS=include/udis86.h libudis86/decode.h libudis86/extern.h libudis86/itab.h libudis86/syn.h libudis86/types.h libudis86/udint.h src/hook_int.h
RM=rm -rf

# in cmd of windows
ifeq ($(SHELL),sh.exe)
    RM := del /f/q
endif

all: myopen.so

myopen.so: open.o hook.o dict.o decode.o itab.o syn-att.o syn-intel.o syn.o udis86.o xxhash.o
	$(CC) $(LFLAGS) -o myopen.so obj/open.o obj/hook.o obj/dict.o obj/xxhash.o obj/decode.o obj/itab.o obj/syn-att.o obj/syn-intel.o obj/syn.o obj/udis86.o

open.o: src/open.c $(HEADERS)
	$(CC) $(CFLAGS) -c -o obj/open.o $<

hook.o: src/hook.c $(HEADERS)
	$(CC) $(CFLAGS) -c -o obj/hook.o $<

dict.o: src/dict.c $(HEADERS)
	$(CC) $(CFLAGS) -c -o obj/dict.o $<
xxhash.o: src/xxhash.c $(HEADERS)
	$(CC) $(CFLAGS) -c -o obj/xxhash.o $<

decode.o: libudis86/decode.c $(HEADERS)
	$(CC) $(CFLAGS) -c -o obj/decode.o $<

itab.o: libudis86/itab.c $(HEADERS)
	$(CC) $(CFLAGS) -c -o obj/itab.o $<

syn-att.o: libudis86/syn-att.c $(HEADERS)
	$(CC) $(CFLAGS) -c -o obj/syn-att.o $<

syn-intel.o: libudis86/syn-intel.c $(HEADERS)
	$(CC) $(CFLAGS) -c -o obj/syn-intel.o $<

syn.o: libudis86/syn.c $(HEADERS)
	$(CC) $(CFLAGS) -c -o obj/syn.o $<

udis86.o: libudis86/udis86.c $(HEADERS)
	$(CC) $(CFLAGS) -c -o obj/udis86.o $<


clean:
	$(RM) obj/*.o myopen.so

$(shell   mkdir -p obj)
