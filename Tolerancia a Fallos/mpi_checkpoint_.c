/**
 * mpi_checkpoint.c
 * Checkpointing Coordinado con Rollback Recovery en MPIs
 * Flujo:
 *  - Si existe checkpoint en disco: recupera estado y reanuda.
 *  - Si no: inicia desde cero y guarda checkpoint inicial.
 *  - Cada CHECKPOINT_INTERVAL iteraciones: checkpoint coordinado
 *    (MPI_Barrier → todos escriben → MPI_Barrier).
 *  - En iteración FAIL_AT_ITER (primera ejecución): MPI_Abort simula fallo.
 */

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define VEC_SIZE             10
#define TOTAL_ITER           20
#define CHECKPOINT_INTERVAL   5
#define FAIL_AT_ITER          8

typedef struct {
    int    rank;
    int    iteration;       /* Última iteración completada */
    double partial_sum;
    double vector[VEC_SIZE];
} ProcessState;

static void checkpoint_filename(int rank, char *buf, size_t buf_size) {
    snprintf(buf, buf_size, "ckpt_rank_%d.bin", rank);
}

static void save_checkpoint(const ProcessState *st) {
    char fname[64];
    checkpoint_filename(st->rank, fname, sizeof(fname));

    FILE *f = fopen(fname, "wb");
    if (!f) {
        fprintf(stderr, "[Rank %d] ERROR: No se pudo abrir '%s'.\n", st->rank, fname);
        MPI_Abort(MPI_COMM_WORLD, 2);
    }
    fwrite(st, sizeof(ProcessState), 1, f);
    fclose(f);

    printf("[Rank %d]  Checkpoint guardado → %s  (iteracion=%d | suma=%.1f)\n",
           st->rank, fname, st->iteration, st->partial_sum);
    fflush(stdout);
}

/* Retorna 1 si se recuperó un checkpoint, 0 si no existe. */
static int load_checkpoint(ProcessState *st, int rank) {
    char fname[64];
    checkpoint_filename(rank, fname, sizeof(fname));

    FILE *f = fopen(fname, "rb");
    if (!f) return 0;

    size_t n = fread(st, sizeof(ProcessState), 1, f);
    fclose(f);

    if (n != 1) {
        fprintf(stderr, "[Rank %d] ADVERTENCIA: Checkpoint corrupto. Iniciando desde cero.\n", rank);
        return 0;
    }

    printf("[Rank %d]  Checkpoint recuperado ← %s  (reanudar desde iteracion=%d)\n",
           rank, fname, st->iteration);
    fflush(stdout);
    return 1;
}

static void init_state(ProcessState *st, int rank) {
    st->rank        = rank;
    st->iteration   = 0;
    st->partial_sum = 0.0;
    for (int i = 0; i < VEC_SIZE; i++)
        st->vector[i] = (double)(rank * 10 + i + 1);

    printf("[Rank %d]  Estado inicializado desde cero.\n", rank);
    fflush(stdout);
}

int main(int argc, char *argv[]) {
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    if (size < 3) {
        if (rank == 0)
            fprintf(stderr, "[ERROR] Se requieren >= 3 procesos. Uso: mpirun -n 3 ./mpi_checkpoint\n");
        MPI_Finalize();
        return 1;
    }

    if (rank == 0) {
        printf("\n======================================================\n");
        printf("  MPI Checkpoint / Rollback Recovery\n");
        printf("  Procesos: %d | Vec: %d | Iter: %d | Ckpt cada: %d | Fallo en: %d\n",
               size, VEC_SIZE, TOTAL_ITER, CHECKPOINT_INTERVAL, FAIL_AT_ITER);
        printf("======================================================\n\n");
        fflush(stdout);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    /* 1. Detectar checkpoint existente o iniciar desde cero */
    ProcessState state;
    int recovered = load_checkpoint(&state, rank);

    if (!recovered) {
        init_state(&state, rank);

        /* Checkpoint inicial coordinado */
        MPI_Barrier(MPI_COMM_WORLD);
        save_checkpoint(&state);
        MPI_Barrier(MPI_COMM_WORLD);

        if (rank == 0) {
            printf("\n[=== Checkpoint inicial coordinado completado ===]\n\n");
            fflush(stdout);
        }
    }

    /* 2. Bucle de cómputo */
    if (rank == 0) {
        printf("[Rank 0]  %s desde iteracion %d → %d\n\n",
               recovered ? "REANUDANDO" : "INICIANDO",
               state.iteration, TOTAL_ITER);
        fflush(stdout);
    }
    MPI_Barrier(MPI_COMM_WORLD);

    for (int iter = state.iteration; iter < TOTAL_ITER; iter++) {

        /* Simulación de fallo (solo en primera ejecución) */
        if (!recovered && iter == FAIL_AT_ITER) {
            MPI_Barrier(MPI_COMM_WORLD);
            printf("[Rank %d]  *** FALLO SIMULADO en iteracion %d — MPI_Abort ***\n", rank, iter);
            fflush(stdout);
            MPI_Abort(MPI_COMM_WORLD, 99);
        }

        /* Cómputo: actualizar vector y acumular suma */
        double iter_contribution = 0.0;
        for (int i = 0; i < VEC_SIZE; i++) {
            state.vector[i] += (double)(iter + 1) * 0.5 * (rank + 1);
            iter_contribution += state.vector[i];
        }
        state.partial_sum += iter_contribution;
        state.iteration    = iter + 1;

        printf("[Rank %d]  iter=%2d | contribucion=%8.1f | suma_parcial=%10.1f\n",
               rank, iter + 1, iter_contribution, state.partial_sum);
        fflush(stdout);

        /* Checkpoint coordinado: barrera → guardar → barrera */
        if (state.iteration % CHECKPOINT_INTERVAL == 0) {
            MPI_Barrier(MPI_COMM_WORLD);
            save_checkpoint(&state);
            MPI_Barrier(MPI_COMM_WORLD);

            if (rank == 0) {
                printf("\n[=== Checkpoint coordinado en iteracion %d completado ===]\n\n",
                       state.iteration);
                fflush(stdout);
            }
            MPI_Barrier(MPI_COMM_WORLD);
        }
    }

    /* 3. Reducción final: suma global en rank 0 */
    double global_sum = 0.0;
    MPI_Reduce(&state.partial_sum, &global_sum, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);

    if (rank == 0) {
        printf("\n======================================================\n");
        printf("  COMPUTO FINALIZADO\n");
        printf("  Suma global = %.2f\n", global_sum);
        printf("======================================================\n\n");
        fflush(stdout);
    }

    MPI_Finalize();
    return 0;
}
