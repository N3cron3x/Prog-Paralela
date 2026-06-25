#include <stdio.h>
#include <stdlib.h>
#include <math.h>
#include <omp.h>
#include <cuda_runtime.h>

// ─── Configuración ────────────────────────────────────────────────────────────

#define N          (1 << 20)   // 1 048 576 elementos
#define BLOCK_SIZE 256
#define TOL        1e-5f

// ─── Utilidades CUDA ─────────────────────────────────────────────────────────

#define CUDA_CHECK(call)                                                        \
    do {                                                                        \
        cudaError_t err = (call);                                               \
        if (err != cudaSuccess) {                                               \
            fprintf(stderr, "CUDA error in %s:%d — %s\n",                      \
                    __FILE__, __LINE__, cudaGetErrorString(err));               \
            exit(EXIT_FAILURE);                                                 \
        }                                                                       \
    } while (0)

// ─── Kernel GPU ──────────────────────────────────────────────────────────────

__global__ void add_vectors_gpu(const float* __restrict__ A,
                                const float* __restrict__ B,
                                float* __restrict__ C,
                                int n)
{
    int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) C[i] = A[i] + B[i];
}

// ─── Versión CPU con OpenMP ──────────────────────────────────────────────────

double run_cpu(const float* A, const float* B, float* C, int n)
{
    double t0 = omp_get_wtime();

    #pragma omp parallel for schedule(static)
    for (int i = 0; i < n; i++)
        C[i] = A[i] + B[i];

    return omp_get_wtime() - t0;
}

// ─── Versión GPU con CUDA ────────────────────────────────────────────────────

double run_gpu(const float* h_A, const float* h_B, float* h_C, int n)
{
    size_t bytes = (size_t)n * sizeof(float);
    float *d_A, *d_B, *d_C;

    CUDA_CHECK(cudaMalloc(&d_A, bytes));
    CUDA_CHECK(cudaMalloc(&d_B, bytes));
    CUDA_CHECK(cudaMalloc(&d_C, bytes));

    double t0 = omp_get_wtime();

    CUDA_CHECK(cudaMemcpy(d_A, h_A, bytes, cudaMemcpyHostToDevice));
    CUDA_CHECK(cudaMemcpy(d_B, h_B, bytes, cudaMemcpyHostToDevice));

    int blocks = (n + BLOCK_SIZE - 1) / BLOCK_SIZE;
    add_vectors_gpu<<<blocks, BLOCK_SIZE>>>(d_A, d_B, d_C, n);
    CUDA_CHECK(cudaGetLastError());
    CUDA_CHECK(cudaDeviceSynchronize());

    CUDA_CHECK(cudaMemcpy(h_C, d_C, bytes, cudaMemcpyDeviceToHost));

    double elapsed = omp_get_wtime() - t0;

    CUDA_CHECK(cudaFree(d_A));
    CUDA_CHECK(cudaFree(d_B));
    CUDA_CHECK(cudaFree(d_C));

    return elapsed;
}

// ─── Verificación de resultados ──────────────────────────────────────────────

int verify(const float* A, const float* B, const float* C, int n)
{
    for (int i = 0; i < n; i++) {
        if (fabsf(C[i] - (A[i] + B[i])) > TOL) {
            fprintf(stderr, "Error en índice %d: esperado %.6f, obtenido %.6f\n",
                    i, A[i] + B[i], C[i]);
            return 0;
        }
    }
    return 1;
}

// ─── Inicialización de vectores ──────────────────────────────────────────────

void init_vectors(float* A, float* B, int n)
{
    #pragma omp parallel for schedule(static)
    for (int i = 0; i < n; i++) {
        A[i] = (float)i * 0.5f;
        B[i] = (float)(n - i) * 0.5f;
    }
}

// ─── Main ────────────────────────────────────────────────────────────────────

int main(void)
{
    size_t bytes = (size_t)N * sizeof(float);

    float* A     = (float*)malloc(bytes);
    float* B     = (float*)malloc(bytes);
    float* C_cpu = (float*)malloc(bytes);
    float* C_gpu = (float*)malloc(bytes);

    if (!A || !B || !C_cpu || !C_gpu) {
        fprintf(stderr, "Error: no se pudo reservar memoria del host.\n");
        return EXIT_FAILURE;
    }

    init_vectors(A, B, N);

    // ── CPU ──────────────────────────────────────────────────────────────────
    printf("=== Versión CPU (OpenMP) ===\n");
    printf("  Hilos disponibles : %d\n", omp_get_max_threads());

    double t_cpu = run_cpu(A, B, C_cpu, N);
    printf("  Tiempo            : %.6f s\n\n", t_cpu);

    // ── GPU ──────────────────────────────────────────────────────────────────
    printf("=== Versión GPU (CUDA) ===\n");

    int device_count = 0;
    CUDA_CHECK(cudaGetDeviceCount(&device_count));
    if (device_count == 0) {
        fprintf(stderr, "No se encontró ninguna GPU compatible con CUDA.\n");
        return EXIT_FAILURE;
    }

    cudaDeviceProp prop;
    CUDA_CHECK(cudaGetDeviceProperties(&prop, 0));
    printf("  GPU               : %s\n", prop.name);
    printf("  Bloques           : %d  |  Hilos por bloque : %d\n",
           (N + BLOCK_SIZE - 1) / BLOCK_SIZE, BLOCK_SIZE);

    double t_gpu = run_gpu(A, B, C_gpu, N);
    int ok = verify(A, B, C_gpu, N);

    printf("  Verificación      : %s\n", ok ? "CORRECTA ✓" : "FALLIDA ✗");
    printf("  Tiempo            : %.6f s\n\n", t_gpu);

    // ── Comparación ──────────────────────────────────────────────────────────
    printf("=== Comparación de rendimiento ===\n");
    printf("  Speedup (CPU / GPU) : %.2fx\n\n", t_cpu / t_gpu);

    if (t_gpu < t_cpu)
        printf("  La GPU fue %.2fx más rápida que la CPU.\n", t_cpu / t_gpu);
    else
        printf("  La CPU fue %.2fx más rápida que la GPU (overhead de transferencia).\n",
               t_gpu / t_cpu);

    free(A);
    free(B);
    free(C_cpu);
    free(C_gpu);

    return EXIT_SUCCESS;
}
