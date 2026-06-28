#!/usr/bin/env bash
# ============================================================
# run.sh — Compila, sube archivos a HDFS y ejecuta WordCount
# Uso: ./run.sh [directorio_local_con_txts]
# ============================================================
set -euo pipefail

HDFS_INPUT="/user/${USER}/wordcount/input"
HDFS_OUTPUT="/user/${USER}/wordcount/output"
JAR="target/wordcount-mapreduce-1.0.jar"
INPUT_DIR="${1:-./input}"

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ── 1. Compilar ─────────────────────────────────────────────
log "Compilando con Maven..."
mvn -q clean package -DskipTests
log "JAR generado: $JAR"

# ── 2. Preparar HDFS ────────────────────────────────────────
log "Configurando directorios en HDFS..."
hdfs dfs -mkdir -p "$HDFS_INPUT"
hdfs dfs -rm -f "${HDFS_INPUT}/*" 2>/dev/null || true

TXT_COUNT=$(ls "$INPUT_DIR"/*.txt 2>/dev/null | wc -l)
if [ "$TXT_COUNT" -eq 0 ]; then
    log "ERROR: No se encontraron archivos .txt en $INPUT_DIR"
    exit 1
fi

log "Subiendo $TXT_COUNT archivo(s) a HDFS..."
hdfs dfs -put "$INPUT_DIR"/*.txt "$HDFS_INPUT/"
hdfs dfs -ls "$HDFS_INPUT"

# ── 3. Ejecutar job ─────────────────────────────────────────
log "Ejecutando WordCount con 1 reducer..."
hadoop jar "$JAR" mapreduce.WordCount "$HDFS_INPUT" "$HDFS_OUTPUT" 1

# ── 4. Recuperar y mostrar resultados ───────────────────────
log "Recuperando resultados desde HDFS..."
mkdir -p output
hdfs dfs -get "${HDFS_OUTPUT}/part-r-*" ./output/

echo ""
echo "══════════════════════════════════════"
echo "  Top 20 palabras más frecuentes"
echo "══════════════════════════════════════"
cat output/part-r-* | sort -k2 -rn | head -20
echo "══════════════════════════════════════"
TOTAL=$(cat output/part-r-* | wc -l)
log "Palabras únicas encontradas: $TOTAL"
log "Resultados completos en: ./output/"
