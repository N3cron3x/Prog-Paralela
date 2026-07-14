NVCC = nvcc
CC = gcc

all: libnormalize.so

libnormalize.so: normalize_kernel.o normalize_omp.o
	$(NVCC) -Xcompiler -fopenmp -shared -o libnormalize.so normalize_kernel.o normalize_omp.o

normalize_kernel.o: normalize_kernel.cu normalize.h
	$(NVCC) -Xcompiler -fPIC -c normalize_kernel.cu -o normalize_kernel.o

normalize_omp.o: normalize_omp.c normalize.h
	$(CC) -fPIC -fopenmp -c normalize_omp.c -o normalize_omp.o

clean:
	rm -f *.o libnormalize.so
