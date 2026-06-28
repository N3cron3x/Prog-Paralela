#!/usr/bin/env bash
# ============================================================
# hdfs_setup.sh — Verifica Hadoop, crea estructura en HDFS
#                 y sube PDFs/TXTs convertidos.
# Ejecutar UNA VEZ antes de run.sh o benchmark.sh.
# ============================================================
set -euo pipefail

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# ── Verificar que Hadoop esté disponible ────────────────────
if ! command -v hdfs &>/dev/null; then
    echo "ERROR: 'hdfs' no encontrado. Asegúrate de que HADOOP_HOME esté en PATH."
    echo "  export HADOOP_HOME=/opt/hadoop"
    echo "  export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin"
    exit 1
fi

log "Hadoop versión: $(hadoop version | head -1)"

# ── Iniciar HDFS si no está corriendo (modo local/pseudodistribuido) ────
if ! hdfs dfs -ls / &>/dev/null 2>&1; then
    log "Iniciando HDFS..."
    start-dfs.sh
    sleep 5
fi

# ── Crear estructura de directorios ─────────────────────────
log "Creando directorios en HDFS..."
hdfs dfs -mkdir -p "/user/${USER}/wordcount/input"
hdfs dfs -mkdir -p "/user/${USER}/wordcount/benchmark"

log "Estructura HDFS lista:"
hdfs dfs -ls "/user/${USER}/wordcount"

# ── Instrucciones para subir PDFs ───────────────────────────
cat <<'EOF'

─────────────────────────────────────────────────────────
  Para subir tus PDFs y convertirlos a texto:
  
  1. Instala pdftotext (si no lo tienes):
       sudo apt-get install -y poppler-utils   # Ubuntu/Debian
       brew install poppler                    # macOS

  2. Convierte cada PDF:
       for f in /ruta/tus/pdfs/*.pdf; do
           pdftotext "$f" "input/$(basename "${f%.pdf}").txt"
       done

  3. Sube al HDFS:
       hdfs dfs -put input/*.txt /user/$USER/wordcount/input/
─────────────────────────────────────────────────────────
EOF
