# ==============================================================
#  Makefile — MPI Checkpoint / Rollback Recovery
#  Universidad Iberoamericana (UNIBE)
#  Programación Paralela y Distribuida
# ==============================================================

CC       = mpicc
CFLAGS   = -Wall -Wextra -O2 -std=c99
TARGET   = mpi_checkpoint
SRCS     = mpi_checkpoint.c
NP       = 3                   # Número de procesos MPI

.PHONY: all clean clean-ckpt run run-recover run-full help

# ── Compilación ────────────────────────────────────────────────
all: $(TARGET)
	@echo "Compilado: ./$(TARGET)"

$(TARGET): $(SRCS)
	$(CC) $(CFLAGS) -o $@ $^

# ── Ejecuciones ────────────────────────────────────────────────

# Primera ejecución: genera checkpoints y simula fallo
# El '-' antes de mpirun hace que make ignore el código de salida no-cero
run: $(TARGET)
	@echo ""
	@echo "╔══════════════════════════════════════════════════╗"
	@echo "║  PRIMERA EJECUCIÓN — se espera fallo en iter 8  ║"
	@echo "╚══════════════════════════════════════════════════╝"
	@echo ""
	-mpirun -n $(NP) ./$(TARGET)
	@echo ""
	@echo "── Archivos de checkpoint en disco ──"
	@ls -lh ckpt_rank_*.bin 2>/dev/null || echo "(ninguno)"

# Segunda ejecución: recupera desde checkpoint y completa
run-recover: $(TARGET)
	@echo ""
	@echo "╔══════════════════════════════════════════════════╗"
	@echo "║  SEGUNDA EJECUCIÓN — recuperación desde ckpt    ║"
	@echo "╚══════════════════════════════════════════════════╝"
	@echo ""
	mpirun -n $(NP) ./$(TARGET)

# Demo completa: limpia estado, falla, y luego recupera
run-full: $(TARGET)
	$(MAKE) clean-ckpt
	$(MAKE) run
	@echo ""
	@echo ">> Pausa 1 segundo antes de la recuperación..."
	@sleep 1
	$(MAKE) run-recover

# ── Limpieza ───────────────────────────────────────────────────

# Elimina solo los archivos de checkpoint
clean-ckpt:
	@rm -f ckpt_rank_*.bin
	@echo "Checkpoints eliminados."

# Limpieza completa (binario + checkpoints)
clean:
	@rm -f $(TARGET) ckpt_rank_*.bin
	@echo "Limpieza completa."

# ── Ayuda ──────────────────────────────────────────────────────
help:
	@echo ""
	@echo "Targets disponibles:"
	@echo "  make             Compila el programa"
	@echo "  make run         Primera ejecución (fallo simulado)"
	@echo "  make run-recover Segunda ejecución (rollback + recuperación)"
	@echo "  make run-full    Demo completa: limpia + falla + recupera"
	@echo "  make clean-ckpt  Elimina archivos .bin de checkpoint"
	@echo "  make clean       Limpieza completa"
	@echo ""
