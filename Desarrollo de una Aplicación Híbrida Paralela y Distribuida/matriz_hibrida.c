#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>
#include <omp.h>

#define N 1024

int main(int argc, char *argv[]) {
    int rank, size;
    double *A = NULL, *B = NULL, *C = NULL;
    double *local_A, *local_C;
    double start_time, end_time;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (N % size != 0) {
        if (rank == 0) {
            printf("Error: El tamano de la matriz N debe ser divisible por el numero de procesos MPI.\n");
        }
        MPI_Finalize();
        return 1;
    }

    int local_rows = N / size;
    local_A = (double *)malloc(local_rows * N * sizeof(double));
    local_C = (double *)malloc(local_rows * N * sizeof(double));
    B = (double *)malloc(N * N * sizeof(double));

    if (rank == 0) {
        A = (double *)malloc(N * N * sizeof(double));
        C = (double *)malloc(N * N * sizeof(double));

        for (int i = 0; i < N * N; i++) {
            A[i] = 1.0;
            B[i] = 2.0;
        }
        
        start_time = MPI_Wtime();
    }

    MPI_Bcast(B, N * N, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    MPI_Scatter(A, local_rows * N, MPI_DOUBLE, local_A, local_rows * N, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    #pragma omp parallel for
    for (int i = 0; i < local_rows; i++) {
        for (int j = 0; j < N; j++) {
            double suma_temporal = 0.0;
            for (int k = 0; k < N; k++) {
                suma_temporal += local_A[i * N + k] * B[k * N + j];
            }
            local_C[i * N + j] = suma_temporal;
        }
    }

    MPI_Gather(local_C, local_rows * N, MPI_DOUBLE, C, local_rows * N, MPI_DOUBLE, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        end_time = MPI_Wtime();
        printf("Matriz resultante recopilada con exito.\n");
        printf("Tiempo de ejecucion total: %f segundos\n", end_time - start_time);
        
        free(A);
        free(C);
    }

    free(local_A);
    free(local_C);
    free(B);

    MPI_Finalize();
    return 0;
}