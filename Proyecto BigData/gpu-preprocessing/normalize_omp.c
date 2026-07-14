#include <omp.h>
#include "normalize.h"

// Version CPU equivalente, usada como referencia comparativa contra la GPU
void normalizeOMP(float* data, int n, float minVal, float maxVal) {
    float range = maxVal - minVal;
    #pragma omp parallel for
    for (int i = 0; i < n; i++) {
        data[i] = range > 1e-8f ? (data[i] - minVal) / range : 0.0f;
    }
}
