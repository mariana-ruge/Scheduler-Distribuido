"""
Scheduler distribuido que asigna tareas considerando:
  - Reputación (0-100): +5 éxito, -10 fallo, decae 0.5/hora
  - Carga simulada (0-100): patrón cíclico diferente por nodo
  - Disponibilidad: heartbeat con TTL, offline si >5s sin respuesta
  - Fórmula: score = rep*0.4 + (100-carga)*0.5 + (vivo?10:0) - penalización_cola
"""
import json
import math
import time
import uuid
import random
import logging
import threading
from datetime import datetime
from typing import Optional

import redis

from config import CONFIG
from metrics import Metrics

# ── Scripts Lua ──────────────────────────────────────────────────────────────

# Asignación atómica: crea hash + encola en una operación
_LUA_ASSIGN = """
local task_id = KEYS[1]
local queue_key = KEYS[2]
for i = 1, #ARGV, 2 do
    redis.call('HSET', task_id, ARGV[i], ARGV[i+1])
end
redis.call('LPUSH', queue_key, task_id)
return 1
"""

# Completar tarea + ajustar reputación atómicamente
_LUA_COMPLETE = """
local task_id = KEYS[1]
local node_key = KEYS[2]
redis.call('HSET', task_id, 'status', ARGV[1], 'completed_at', ARGV[2], 'result', ARGV[3])
local delta = tonumber(ARGV[4])
local rep = tonumber(redis.call('HGET', node_key, 'reputation') or 70)
rep = rep + delta
if rep > 100 then rep = 100 end
if rep < 0 then rep = 0 end
redis.call('HSET', node_key, 'reputation', rep)
return rep
"""


class DistributedScheduler:

    def __init__(self, r: redis.Redis, node_id: str, metrics: Metrics):
        self._r = r
        self._node_id = node_id
        self._metrics = metrics
        self._log = logging.getLogger("SCHED")
        self._running = threading.Event()
        self._running.set()
        self._lock = threading.Lock()
        self._current_load = 0

        # Estado local
        self.reputation = CONFIG.reputation_initial
        self.available = True

        # Registrar scripts Lua
        self._assign_script = r.register_script(_LUA_ASSIGN)
        self._complete_script = r.register_script(_LUA_COMPLETE)

        # Hilos de servicio
        self._threads = []
        self._start_thread(self._heartbeat_loop)
        self._start_thread(self._simulated_load_loop)
        self._start_thread(self._reputation_decay_loop)

    def _start_thread(self, target):
        t = threading.Thread(target=target, daemon=True)
        t.start()
        self._threads.append(t)

    # ── Propiedad thread-safe ────────────────────────────────────────

    @property
    def current_load(self) -> int:
        with self._lock:
            return self._current_load

    @current_load.setter
    def current_load(self, value: int):
        with self._lock:
            self._current_load = max(0, min(100, value))

    # ── Heartbeat ────────────────────────────────────────────────────

    def _heartbeat_loop(self):
        """Publica estado del nodo cada heartbeat_interval segundos."""
        node_key = f"node:{self._node_id}"
        while self._running.is_set():
            try:
                pipe = self._r.pipeline(transaction=False)
                pipe.hset(node_key, mapping={
                    'last_heartbeat': datetime.now().isoformat(),
                    'available': int(self.available),
                    'reputation': self.reputation,
                    'current_load': self.current_load,
                    'tasks_in_queue': self._r.llen(f"queue:{self._node_id}"),
                })
                pipe.expire(node_key, CONFIG.heartbeat_ttl)
                pipe.execute()
            except redis.RedisError as e:
                self._log.warning("Heartbeat fallido: %s", e)
            self._running.wait(timeout=CONFIG.heartbeat_interval)

    # ── Carga simulada ───────────────────────────────────────────────

    def _simulated_load_loop(self):
        """
        SIMULACIÓN: genera carga artificial con patrón cíclico.
        Cada nodo tiene un perfil distinto (base, amplitud, periodo)
        para que el scheduler vea métricas diferentes.
        """
        profile = CONFIG.load_profiles.get(self._node_id, {
            'base': random.randint(20, 50),
            'amplitude': random.randint(15, 35),
            'period': random.randint(15, 40),
        })
        self._log.info("Perfil de carga simulada: base=%d, amp=%d, periodo=%ds",
                        profile['base'], profile['amplitude'], profile['period'])

        while self._running.is_set():
            t = time.time()
            # Onda sinusoidal + ruido aleatorio
            sine = math.sin(2 * math.pi * t / profile['period'])
            noise = random.uniform(-5, 5)
            load = profile['base'] + profile['amplitude'] * sine + noise
            self.current_load = int(load)
            self._running.wait(timeout=CONFIG.simulated_load_update_interval)

    # ── Decaimiento de reputación ────────────────────────────────────

    def _reputation_decay_loop(self):
        """La reputación decae 0.5 puntos por hora de forma natural."""
        # Chequeamos cada 60 segundos
        while self._running.is_set():
            self._running.wait(timeout=60)
            decay = CONFIG.reputation_decay_per_hour / 60  # por minuto
            node_key = f"node:{self._node_id}"
            try:
                current = self._r.hget(node_key, 'reputation')
                if current is not None:
                    new_rep = max(0, float(current) - decay)
                    self._r.hset(node_key, 'reputation', int(new_rep))
                    self.reputation = int(new_rep)
            except redis.RedisError:
                pass

    # ── Selección del mejor nodo ─────────────────────────────────────

    def get_best_node(self) -> Optional[str]:
        """
        Selecciona el nodo óptimo usando la fórmula:
          score = rep*0.4 + (100-carga)*0.5 + (vivo?10:0) - penalización_cola
        """
        node_keys = [k for k in self._r.scan_iter(match="node:*", count=100)]
        if not node_keys:
            return None

        pipe = self._r.pipeline(transaction=False)
        for key in node_keys:
            pipe.hgetall(key)
        results = pipe.execute()

        now = datetime.now()
        best_node = None
        best_score = -999.0
        scores_debug = []

        for node_key, data in zip(node_keys, results):
            if not data:
                continue
            nid = node_key.split(":", 1)[1]

            if data.get('available', '0') == '0':
                continue

            # Comprobar si está vivo
            alive = False
            try:
                last_hb = datetime.fromisoformat(data['last_heartbeat'])
                age = (now - last_hb).total_seconds()
                alive = age <= CONFIG.failure_threshold
            except (KeyError, ValueError):
                continue

            rep = int(data.get('reputation', CONFIG.reputation_initial))
            load = int(data.get('current_load', 50))
            queue_len = int(data.get('tasks_in_queue', 0))
            queue_penalty = min(queue_len * CONFIG.queue_penalty_per_task,
                                CONFIG.queue_penalty_max)

            score = (rep * CONFIG.weight_reputation
                     + (100 - load) * CONFIG.weight_load
                     + (CONFIG.weight_availability if alive else 0)
                     - queue_penalty)

            scores_debug.append(f"{nid}={score:.1f}")

            if score > best_score:
                best_score = score
                best_node = nid

        if scores_debug:
            self._log.debug("Scores: %s → mejor: %s", ", ".join(scores_debug), best_node)
        return best_node

    # ── Asignación de tareas ─────────────────────────────────────────

    def assign_task(self, task_payload: dict) -> Optional[str]:
        """Asigna una tarea al mejor nodo disponible de forma atómica."""
        best_node = self.get_best_node()
        if not best_node:
            self._log.warning("No hay nodos disponibles para asignar tarea")
            return None

        task_id = f"task:{uuid.uuid4().hex[:12]}"
        task = {
            'id': task_id,
            'assigned_to': best_node,
            'data': json.dumps(task_payload),
            'status': 'pending',
            'created_at': datetime.now().isoformat(),
        }

        args = [v for pair in task.items() for v in pair]
        self._assign_script(keys=[task_id, f"queue:{best_node}"], args=args)

        self._metrics.inc_assigned(best_node)
        self._metrics.record_event('task_assigned', {
            'task_id': task_id, 'node': best_node,
            'payload_type': task_payload.get('type', '?')
        })

        self._log.info("→ Tarea %s asignada a '%s'", task_id, best_node)
        return task_id

    # ── Obtención y completado ───────────────────────────────────────

    def get_pending_task(self):
        """Extrae la siguiente tarea de la cola FIFO de este nodo."""
        task_id = self._r.rpop(f"queue:{self._node_id}")
        if not task_id:
            return None, None
        return task_id, self._r.hgetall(task_id)

    def complete_task(self, task_id: str, result: Optional[dict] = None,
                      success: bool = True, exec_time_ms: float = 0):
        """Completa tarea y ajusta reputación atómicamente."""
        delta = CONFIG.reputation_on_success if success else CONFIG.reputation_on_failure
        new_rep = self._complete_script(
            keys=[task_id, f"node:{self._node_id}"],
            args=[
                'completed' if success else 'failed',
                datetime.now().isoformat(),
                json.dumps(result) if result else '',
                delta,
            ]
        )
        self.reputation = int(new_rep)

        if success:
            self._metrics.inc_completed(exec_time_ms)
        else:
            self._metrics.inc_failed()

        event_type = 'task_completed' if success else 'task_failed'
        self._metrics.record_event(event_type, {
            'task_id': task_id, 'exec_time_ms': round(exec_time_ms),
        })

    # ── Actualización de métricas externas ───────────────────────────

    def update_node_metrics(self, node_id: str, load: Optional[int] = None,
                            reputation_delta: Optional[int] = None):
        """Actualiza métricas de un nodo remoto (para testing/admin)."""
        node_key = f"node:{node_id}"
        if load is not None:
            self._r.hset(node_key, 'current_load', max(0, min(100, load)))
        if reputation_delta is not None:
            current = int(self._r.hget(node_key, 'reputation') or 70)
            new_rep = max(0, min(100, current + reputation_delta))
            self._r.hset(node_key, 'reputation', new_rep)

    # ── Ciclo de vida ────────────────────────────────────────────────

    def stop(self):
        self._running.clear()
        for t in self._threads:
            t.join(timeout=5)
        self._r.delete(f"node:{self._node_id}")
