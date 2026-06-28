#!/usr/bin/env bash
# ============================================================
# benchmark.sh — Ejecuta WordCount con 1, 2 y 4 reducers
#                y compara tiempos de ejecución.
# ============================================================
set -euo pipefail

HDFS_INPUT="/user/${USER}/wordcount/input"
HDFS_BASE="/user/${USER}/wordcount/benchmark"
JAR="target/wordcount-mapreduce-1.0.jar"
RESULTS_FILE="benchmark_results.tsv"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

echo -e "reducers\ttiempo_ms\tarchivo_salida" > "$RESULTS_FILE"

for N_REDUCERS in 1 2 4; do
    HDFS_OUT="${HDFS_BASE}/output_r${N_REDUCERS}"
    log "─────────────────────────────────────────"
    log "Ejecutando con $N_REDUCERS reducer(s)..."

    START=$(date +%s%3N)
    hadoop jar "$JAR" mapreduce.WordCount "$HDFS_INPUT" "$HDFS_OUT" "$N_REDUCERS" \
        2>/dev/null
    END=$(date +%s%3N)

    ELAPSED=$((END - START))
    log "Reducers=$N_REDUCERS | Tiempo=${ELAPSED}ms"

    # Guardar result parciales
    LOCAL_OUT="output/benchmark_r${N_REDUCERS}"
    mkdir -p "$LOCAL_OUT"
    hdfs dfs -get "${HDFS_OUT}/part-r-*" "$LOCAL_OUT/" 2>/dev/null || true

    echo -e "${N_REDUCERS}\t${ELAPSED}\t${LOCAL_OUT}" >> "$RESULTS_FILE"
done

log "─────────────────────────────────────────"
log "Resumen de benchmarks:"
column -t "$RESULTS_FILE"

log "Verificando consistencia de conteos entre ejecuciones..."
WORDS_R1=$(cat output/benchmark_r1/part-r-* 2>/dev/null | wc -l)
WORDS_R4=$(cat output/benchmark_r4/part-r-* 2>/dev/null | wc -l)
echo "Palabras únicas (1 reducer): $WORDS_R1"
echo "Palabras únicas (4 reducers): $WORDS_R4"

if [ "$WORDS_R1" -eq "$WORDS_R4" ]; then
    log "✓ Resultados consistentes entre configuraciones."
else
    log "⚠ Discrepancia detectada — revisar particionado."
fi
