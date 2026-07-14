#ifndef NORMALIZE_H
#define NORMALIZE_H

void normalizeGPU(float* hostData, int n, float minVal, float maxVal);
void normalizeOMP(float* data, int n, float minVal, float maxVal);

#endif
