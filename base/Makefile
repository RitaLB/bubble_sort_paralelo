FLAGS=-Wall -Wno-unused-result -std=c11 -O2

CC=gcc

RM=rm -f

EXEC=sort

all: $(EXEC)

sort.o: sort.c
	$(CC) $(FLAGS) -c sort.c -lpthread

$(EXEC): sort.o
	$(CC) $(FLAGS) sort.o -o $(EXEC) -lpthread
	$(RM) sort.o

run:
	./$(EXEC)

clean:
	$(RM) sort.o $(EXEC)
