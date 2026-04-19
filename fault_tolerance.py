"""
Tolerancia a fallos y reconfiguración:
  - FailureDetector: monitorea heartbeats, marca nodos SUSPECTED → FAILED
  - ReconfigurationManager: reasigna tareas de nodos caídos, notifica topología
  - simulate_crash(): simula caída de un nodo para testing
"""
import json
import time
import logging
import threading
from datetime import datetime
from typing import Set, Dict, Optional
from enum import Enum

import redis

from config import CONFIG
from metrics import Metrics

# Reasignar tareas de un nodo caído a otro nodo vivo (atómico)
_LUA_REASSIGN = """
local dead_queue = KEYS[1]
local alive_queue = KEYS[2]
local new_assignee = ARGV[1]
local count = 0
while true do
    local task_id = redis.call('RPOP', dead_queue)
    if not task_id then break end
    redis.call('HSET', task_id, 'assigned_to', new_assignee, 'status', 'reassigned')
    redis.call('LPUSH', alive_queue, task_id)
    count = count + 1
end
return count
"""


class NodeState(Enum):
    ALIVE = "ALIVE"
    SUSPECTED = "SUSPECTED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class FailureDetector:
    """
    Detector de fallos basado en heartbeats.
    Transiciones: ALIVE → SUSPECTED (>5s) → FAILED (>10s)
    """

    def __init__(self, r: redis.Redis, node_id: str, metrics: Metrics):
        self._r = r
        self._node_id = node_id
        self._metrics = metrics
        self._log = logging.getLogger("FAULTS")
        self._running = threading.Event()
        self._running.set()
        self._node_states: Dict[str, NodeState] = {}
        self._thread: Optional[threading.Thread] = None

    @property
    def node_states(self) -> Dict[str, NodeState]:
        return dict(self._node_states)

    def get_alive_nodes(self) -> Set[str]:
        return {nid for nid, st in self._node_states.items() if st == NodeState.ALIVE}

    def get_failed_nodes(self) -> Set[str]:
        return {nid for nid, st in self._node_states.items() if st == NodeState.FAILED}

    def start(self):
        self._thread = threading.Thread(target=self._detection_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running.clear()
        if self._thread:
            self._thread.join(timeout=5)

    def _detection_loop(self):
        """Chequea heartbeats cada fault_check_interval segundos."""
        while self._running.is_set():
            try:
                self._check_all_nodes()
            except redis.RedisError as e:
                self._log.warning("Error en detección: %s", e)
            self._running.wait(timeout=CONFIG.fault_check_interval)

    def _check_all_nodes(self):
        now = datetime.now()
        discovered = set()

        for k in self._r.scan_iter(match="node:*", count=100):
            data = self._r.hgetall(k)
            nid = k.split(":", 1)[1]
            discovered.add(nid)

            if nid == self._node_id:
                self._node_states[nid] = NodeState.ALIVE
                continue

            try:
                last_hb = datetime.fromisoformat(data['last_heartbeat'])
                age = (now - last_hb).total_seconds()
            except (KeyError, ValueError):
                age = 999.0

            old_state = self._node_states.get(nid, NodeState.UNKNOWN)

            if age <= CONFIG.failure_threshold:
                new_state = NodeState.ALIVE
            elif age <= CONFIG.failure_confirmed:
                new_state = NodeState.SUSPECTED
            else:
                new_state = NodeState.FAILED

            # Registrar transiciones de estado
            if old_state != new_state:
                self._log.warning(
                    "Nodo '%s': %s → %s (heartbeat hace %.1fs)",
                    nid, old_state.value, new_state.value, age
                )
                self._metrics.record_event('node_state_change', {
                    'node': nid,
                    'from': old_state.value,
                    'to': new_state.value,
                    'heartbeat_age': round(age, 1),
                })
                if new_state == NodeState.FAILED:
                    self._metrics.inc_failures_detected()

            self._node_states[nid] = new_state

        # Nodos que desaparecieron completamente de Redis
        known = set(self._node_states.keys())
        vanished = known - discovered - {self._node_id}
        for nid in vanished:
            if self._node_states.get(nid) != NodeState.FAILED:
                self._log.warning("Nodo '%s' desapareció de Redis → FAILED", nid)
                self._node_states[nid] = NodeState.FAILED
                self._metrics.record_event('node_vanished', {'node': nid})
                self._metrics.inc_failures_detected()


class ReconfigurationManager:
    """
    Gestiona la reconfiguración del clúster ante fallos:
      - Reasigna tareas de nodos caídos
      - Notifica cambios de topología via Pub/Sub
      - Registra eventos de reconfiguración
    """

    def __init__(self, r: redis.Redis, node_id: str,
                 failure_detector: FailureDetector, metrics: Metrics):
        self._r = r
        self._node_id = node_id
        self._fd = failure_detector
        self._metrics = metrics
        self._log = logging.getLogger("RECONF")
        self._reassign_script = r.register_script(_LUA_REASSIGN)
        self._running = threading.Event()
        self._running.set()
        self._handled_failures: Set[str] = set()  # Evitar reasignación duplicada
        self._thread: Optional[threading.Thread] = None

    def start(self):
        self._thread = threading.Thread(target=self._reconfig_loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._running.clear()
        if self._thread:
            self._thread.join(timeout=5)

    def _reconfig_loop(self):
        """Chequea nodos fallidos y reasigna sus tareas."""
        while self._running.is_set():
            try:
                failed = self._fd.get_failed_nodes()
                for failed_node in failed:
                    if failed_node in self._handled_failures:
                        continue
                    self.handle_node_failure(failed_node)
            except redis.RedisError as e:
                self._log.warning("Error en reconfiguración: %s", e)
            self._running.wait(timeout=CONFIG.fault_check_interval + 1)

    def handle_node_failure(self, failed_node_id: str):
        """Reasigna tareas pendientes del nodo caído al mejor nodo vivo."""
        dead_queue = f"queue:{failed_node_id}"
        pending = self._r.llen(dead_queue)

        if pending == 0:
            self._log.info("Nodo '%s' caído pero sin tareas pendientes", failed_node_id)
            self._handled_failures.add(failed_node_id)
            self.broadcast_topology_change("node_failure", failed_node_id)
            return

        # Elegir nodo destino (el propio nodo si es el que detecta)
        alive_nodes = self._fd.get_alive_nodes() - {failed_node_id}
        target_node = self._node_id if self._node_id in alive_nodes else None
        if not target_node and alive_nodes:
            target_node = next(iter(alive_nodes))

        if not target_node:
            self._log.error("No hay nodos vivos para reasignar tareas de '%s'",
                             failed_node_id)
            return

        # Reasignar atómicamente
        count = self._reassign_script(
            keys=[dead_queue, f"queue:{target_node}"],
            args=[target_node]
        )

        self._log.warning(
            "⚠ RECONFIGURACIÓN: nodo '%s' caído → %d tareas reasignadas a '%s'",
            failed_node_id, count, target_node
        )

        self._metrics.inc_reassigned(count)
        self._metrics.record_event('tasks_reassigned', {
            'failed_node': failed_node_id,
            'target_node': target_node,
            'tasks_count': count,
        })

        self._handled_failures.add(failed_node_id)
        self.broadcast_topology_change("node_failure", failed_node_id, {
            'tasks_reassigned': count,
            'reassigned_to': target_node,
        })

    def broadcast_topology_change(self, change_type: str,
                                   affected_node: str, extra: Optional[dict] = None):
        """Notifica a todos los nodos sobre un cambio en la topología."""
        event = {
            'type': change_type,
            'affected_node': affected_node,
            'detected_by': self._node_id,
            'timestamp': datetime.now().isoformat(),
            'alive_nodes': list(self._fd.get_alive_nodes()),
        }
        if extra:
            event.update(extra)
        self._r.publish("cluster:topology", json.dumps(event))
        self._log.info("Topología broadcast: %s para nodo '%s'",
                        change_type, affected_node)

    def on_node_recovery(self, node_id: str):
        """Maneja la recuperación de un nodo previamente caído."""
        if node_id in self._handled_failures:
            self._handled_failures.discard(node_id)
            self._log.info("✔ Nodo '%s' recuperado", node_id)
            self._metrics.record_event('node_recovery', {'node': node_id})
            self.broadcast_topology_change("node_recovery", node_id)

    def listen_topology_changes(self, callback) -> threading.Thread:
        """Suscribe a cambios de topología del clúster."""
        def _listener():
            pubsub = self._r.pubsub()
            pubsub.subscribe("cluster:topology")
            for msg in pubsub.listen():
                if msg['type'] != 'message':
                    continue
                try:
                    event = json.loads(msg['data'])
                    if event.get('detected_by') != self._node_id:
                        callback(event)
                except Exception:
                    pass

        t = threading.Thread(target=_listener, daemon=True)
        t.start()
        return t


def simulate_crash(r: redis.Redis, node_id: str):
    """
    SIMULACIÓN: simula la caída de un nodo eliminando su heartbeat.
    Sus tareas quedarán en la cola para ser reasignadas.
    """
    log = logging.getLogger("SIM")
    r.delete(f"node:{node_id}")
    log.warning("💀 CRASH SIMULADO: nodo '%s' eliminado de Redis", node_id)
