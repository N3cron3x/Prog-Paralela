"""
Simulación de consenso distribuido usando el algoritmo Raft.
Arquitectura de Software - Comparativa Paxos vs Raft.

Modela un clúster de nodos que se comunican mediante llamadas directas
(en vez de sockets reales) para poder controlar el tiempo, forzar fallos
y producir una traza de ejecución legible y reproducible.
"""

import random
import time

random.seed(7)

FOLLOWER = "FOLLOWER"
CANDIDATE = "CANDIDATE"
LEADER = "LEADER"


# =====================================================================
# Entrada de log replicado
# =====================================================================
class LogEntry:
    def __init__(self, term, command):
        self.term = term
        self.command = command

    def __repr__(self):
        return f"(term={self.term}, cmd='{self.command}')"


# =====================================================================
# Nodo Raft
# =====================================================================
class RaftNode:
    def __init__(self, node_id, cluster):
        self.id = node_id
        self.cluster = cluster
        self.state = FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.log = []
        self.commit_index = -1
        self.alive = True
        self.votes_received = set()

    def log_msg(self, msg):
        if self.cluster.verbose:
            print(f"[t={self.cluster.tick:03d}] Nodo {self.id} ({self.state}, term={self.current_term}): {msg}")

    # -----------------------------------------------------------------
    # Fase de elección de líder
    # -----------------------------------------------------------------
    def start_election(self):
        if not self.alive:
            return
        self.state = CANDIDATE
        self.current_term += 1
        self.voted_for = self.id
        self.votes_received = {self.id}
        self.log_msg(f"inicia elección para el term {self.current_term}")

        for peer in self.cluster.other_nodes(self.id):
            if peer.alive:
                granted = peer.request_vote(self.current_term, self.id, len(self.log) - 1)
                if granted:
                    self.votes_received.add(peer.id)

        if len(self.votes_received) >= self.cluster.majority():
            self.become_leader()
        else:
            self.log_msg(f"no obtuvo mayoría ({len(self.votes_received)} votos), vuelve a FOLLOWER")
            self.state = FOLLOWER

    def request_vote(self, term, candidate_id, candidate_last_log_index):
        if not self.alive:
            return False
        if term < self.current_term:
            return False
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self.state = FOLLOWER
        if self.voted_for in (None, candidate_id) and candidate_last_log_index >= len(self.log) - 1:
            self.voted_for = candidate_id
            self.log_msg(f"vota por Nodo {candidate_id} en term {term}")
            return True
        return False

    def become_leader(self):
        self.state = LEADER
        self.log_msg(f"ELEGIDO LÍDER con {len(self.votes_received)} votos en term {self.current_term}")
        self.cluster.leader_id = self.id

    # -----------------------------------------------------------------
    # Fase de replicación de log (AppendEntries)
    # -----------------------------------------------------------------
    def propose(self, command):
        if self.state != LEADER or not self.alive:
            self.log_msg("no puede proponer: no es líder o está caído")
            return False

        entry = LogEntry(self.current_term, command)
        self.log.append(entry)
        self.log_msg(f"propone entrada {entry} (índice {len(self.log) - 1})")

        acks = 1
        for peer in self.cluster.other_nodes(self.id):
            if peer.append_entries(self.current_term, self.id, list(self.log)):
                acks += 1

        if acks >= self.cluster.majority():
            self.commit_index = len(self.log) - 1
            self.log_msg(f"entrada '{command}' CONFIRMADA por mayoría ({acks}/{len(self.cluster.nodes)})")
            for peer in self.cluster.other_nodes(self.id):
                if peer.alive:
                    peer.commit_index = self.commit_index
            return True

        self.log_msg(f"entrada '{command}' NO alcanzó mayoría ({acks}/{len(self.cluster.nodes)})")
        return False

    def append_entries(self, term, leader_id, entries):
        if not self.alive:
            return False
        if term < self.current_term:
            return False
        self.current_term = term
        self.state = FOLLOWER
        self.voted_for = leader_id
        self.log = list(entries)
        self.log_msg(f"replica log del líder Nodo {leader_id}, longitud={len(self.log)}")
        return True

    def kill(self):
        self.alive = False
        self.log_msg("*** FALLO SIMULADO: el nodo deja de responder ***")

    def revive(self):
        self.alive = True
        self.state = FOLLOWER
        self.log_msg("nodo se reincorpora al clúster como FOLLOWER")


# =====================================================================
# Clúster
# =====================================================================
class RaftCluster:
    def __init__(self, n_nodes=5, verbose=True):
        self.tick = 0
        self.verbose = verbose
        self.nodes = [RaftNode(i, self) for i in range(1, n_nodes + 1)]
        self.leader_id = None

    def other_nodes(self, node_id):
        return [n for n in self.nodes if n.id != node_id]

    def majority(self):
        return (len(self.nodes) // 2) + 1

    def get_leader(self):
        for n in self.nodes:
            if n.alive and n.state == LEADER:
                return n
        return None

    def elect_leader(self, starter_id):
        self.tick += 1
        starter = next(n for n in self.nodes if n.id == starter_id)
        starter.start_election()

    def print_state(self):
        print("\n--- Estado del clúster ---")
        for n in self.nodes:
            estado = n.state if n.alive else "CAÍDO"
            print(f"  Nodo {n.id}: {estado}, term={n.current_term}, log={n.log}, commit_index={n.commit_index}")
        print("---------------------------\n")


# =====================================================================
# Escenario de simulación
# =====================================================================
def run_simulation():
    print("=" * 70)
    print("SIMULACIÓN DE CONSENSO RAFT - 5 NODOS")
    print("=" * 70)

    cluster = RaftCluster(n_nodes=5)

    # 1. Elección inicial de líder ------------------------------------
    print("\n>>> FASE 1: Elección inicial de líder\n")
    cluster.elect_leader(starter_id=1)
    cluster.print_state()

    # 2. Replicación de una entrada mientras el clúster está sano -----
    print(">>> FASE 2: Propuesta y replicación de 'A=1'\n")
    leader = cluster.get_leader()
    leader.propose("A=1")
    cluster.print_state()

    # 3. Simulación de fallo del líder --------------------------------
    print(f">>> FASE 3: Simulación de fallo del líder (Nodo {leader.id})\n")
    leader.kill()

    # 4. Nueva elección entre los nodos restantes ---------------------
    print("\n>>> FASE 4: Nueva elección de líder tras el fallo\n")
    candidatos_vivos = [n.id for n in cluster.nodes if n.alive]
    cluster.elect_leader(starter_id=candidatos_vivos[0])
    cluster.print_state()

    # 5. El clúster sigue operando y confirma una nueva entrada -------
    print(">>> FASE 5: Propuesta y replicación de 'B=2' tras la recuperación\n")
    nuevo_lider = cluster.get_leader()
    if nuevo_lider:
        nuevo_lider.propose("B=2")
    cluster.print_state()

    # 6. Reincorporación del nodo caído --------------------------------
    print(">>> FASE 6: El nodo caído se reincorpora y se pone al día\n")
    nodo_caido = next(n for n in cluster.nodes if not n.alive)
    nodo_caido.revive()
    nodo_caido.log = list(nuevo_lider.log)
    nodo_caido.commit_index = nuevo_lider.commit_index
    nodo_caido.current_term = nuevo_lider.current_term
    cluster.print_state()

    print("=" * 70)
    print("RESUMEN FINAL")
    print("=" * 70)
    for n in cluster.nodes:
        print(f"Nodo {n.id}: log final = {n.log}")
    print(f"\nValor consensuado del clúster: {[e.command for e in nuevo_lider.log]}")


if __name__ == "__main__":
    run_simulation()