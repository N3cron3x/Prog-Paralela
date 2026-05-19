#include <mpi.h>
#include <stdio.h>

int main(int argc, char** argv) {
    int rank, size;
    int numero = 100;

    // Inicializacion del entorno MPI y obtencion del identificador y tamaño
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (rank == 0) {
        MPI_Send(&numero, 1, MPI_INT, 1, 0, MPI_COMM_WORLD);
    } else if (rank == 1) {
        // Recepcion del mensaje enviado por el proceso emisor 0
        MPI_Recv(&numero, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("Proceso %d recibio el valor %d del proceso emisor 0\n", rank, numero);
    }

    MPI_Finalize();
    return 0;
}