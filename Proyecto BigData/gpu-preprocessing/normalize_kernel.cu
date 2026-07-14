#include <cuda_runtime.h>
#include "normalize.h"

__global__ void minMaxNormalizeKernel(float* data, int n, float minVal, float maxVal) {
    int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx < n) {
        float range = maxVal - minVal;
        data[idx] = range > 1e-8f ? (data[idx] - minVal) / range : 0.0f;
    }
}

extern "C" void normalizeGPU(float* hostData, int n, float minVal, float maxVal) {
    float* deviceData;
    size_t bytes = n * sizeof(float);

    cudaMalloc((void**)&deviceData, bytes);
    cudaMemcpy(deviceData, hostData, bytes, cudaMemcpyHostToDevice);

    int blockSize = 256;
    int gridSize = (n + blockSize - 1) / blockSize;
    minMaxNormalizeKernel<<<gridSize, blockSize>>>(deviceData, n, minVal, maxVal);
    cudaDeviceSynchronize();

    cudaMemcpy(hostData, deviceData, bytes, cudaMemcpyDeviceToHost);
    cudaFree(deviceData);
}
