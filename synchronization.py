"""
Primitivas de sincronización distribuida:
  - DistributedLock: Mutex distribuido (SETNX + EX + UUID)
  - DistributedBarrier: Espera a que N nodos lleguen al mismo punto
  - LogicalClock: Reloj de Lamport para ordenamiento causal de eventos
"""
import uuid
import time
import json
import logging
import threading
from datetime import datetime
from typing import Optional

import redis

from config import CONFIG

# ── Scripts Lua atómicos ─────────────────────────────────────────────────────

_LUA_ACQUIRE = """
if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'PX', ARGV[2]) then
    return 1
end
return 0
"""

_LUA_RELEASE = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
end
return 0
"""


class DistributedLock:
    """
    Mutex distribuido basado en Redis.
    Usa SETNX con TTL para adquisición y verificación de UUID para release seguro.
    Solo el proceso que adquirió el lock puede liberarlo.
    """

    def __init__(self, r: redis.Redis, name: str,
                 timeout_ms: int = CONFIG.lock_default_timeout_ms):
        self._r = r
        self._name = name
        self._timeout_ms = timeout_ms
        self._key = f"lock:{name}"
        self._token: Optional[str] = None
        self._local_lock = threading.Lock()
        self._log = logging.getLogger(f"LOCK")
        self._acquire_script = r.register_script(_LUA_ACQUIRE)
        self._release_script = r.register_script(_LUA_RELEASE)

    @property
    def name(self) -> str:
        return self._name

    @property
    def is_held(self) -> bool:
        return self._token is not None

    def acquire(self, blocking: bool = False, retry_interval: float = 0.2,
                max_retries: int = 50) -> bool:
        """
        Intenta adquirir el lock.
        Si blocking=True, reintenta hasta max_retries veces.
        Retorna True si se adquirió.
        """
        token = uuid.uuid4().hex
        attempts = 1 if not blocking else max_retries

        for _ in range(attempts):
            with self._local_lock:
                result = self._acquire_script(
                    keys=[self._key], args=[token, self._timeout_ms]
                )
                if result:
                    self._token = token
                    self._log.info("✔ Lock '%s' adquirido", self._name)
                    return True
            if blocking:
                time.sleep(retry_interval)

        self._log.info("✘ Lock '%s' ocupado", self._name)
        return False

    def release(self) -> bool:
        """Libera el lock solo si el token coincide (release seguro)."""
        with self._local_lock:
            if not self._token:
                return False
            result = self._release_script(keys=[self._key], args=[self._token])
            if result:
                self._log.info("✔ Lock '%s' liberado", self._name)
                self._token = None
                return True
            self._log.warning("Lock '%s' ya expiró o fue tomado por otro", self._name)
            self._token = None
            return False

    def __enter__(self):
        if not self.acquire(blocking=True):
            raise TimeoutError(f"No se pudo adquirir lock '{self._name}'")
        return self

    def __exit__(self, *args):
        self.release()


class DistributedBarrier:
    """
    Barrera distribuida: espera a que N nodos lleguen al mismo punto.
    Usa un SET de Redis + polling para coordinación.
    """

    def __init__(self, r: redis.Redis, name: str, count: int,
                 timeout: int = CONFIG.barrier_default_timeout):
        self._r = r
        self._name = name
        self._count = count
        self._timeout = timeout
        self._key = f"barrier:{name}"
        self._log = logging.getLogger("BARRIER")

    def wait(self, node_id: str) -> bool:
        """
        Registra este nodo en la barrera y espera a que lleguen todos.
        Retorna True si todos llegaron, False si hubo timeout.
        """
        self._r.sadd(self._key, node_id)
        self._r.expire(self._key, self._timeout + 10)
        self._log.info("Barrera '%s': esperando %d/%d nodos...",
                        self._name, self._r.scard(self._key), self._count)

        deadline = time.time() + self._timeout
        while time.time() < deadline:
            current = self._r.scard(self._key)
            if current >= self._count:
                self._log.info("Barrera '%s': ✔ todos los nodos llegaron (%d/%d)",
                                self._name, current, self._count)
                return True
            time.sleep(CONFIG.barrier_poll_interval)

        current = self._r.scard(self._key)
        self._log.warning("Barrera '%s': ✘ timeout (%d/%d)",
                           self._name, current, self._count)
        return False

    def reset(self):
        """Resetea la barrera eliminando todos los participantes."""
        self._r.delete(self._key)


class LogicalClock:
    """
    Reloj de Lamport para ordenamiento causal de eventos distribuidos.
    Garantiza que si evento A causa evento B, entonces timestamp(A) < timestamp(B).
    """

    def __init__(self, r: redis.Redis, node_id: str):
        self._r = r
        self._node_id = node_id
        self._key = f"lclock:{node_id}"
        self._local_lock = threading.Lock()
        self._log = logging.getLogger("LCLOCK")
        # Inicializar si no existe
        if not self._r.exists(self._key):
            self._r.set(self._key, 0)

    @property
    def time(self) -> int:
        """Lee el timestamp actual del reloj."""
        val = self._r.get(self._key)
        return int(val) if val else 0

    def tick(self) -> int:
        """Evento local: incrementa el reloj en 1."""
        with self._local_lock:
            new_val = self._r.incr(self._key)
            return new_val

    def update(self, received_ts: int) -> int:
        """
        Evento de recepción: max(local, recibido) + 1.
        Garantiza causalidad en el reloj.
        """
        with self._local_lock:
            current = int(self._r.get(self._key) or 0)
            new_val = max(current, received_ts) + 1
            self._r.set(self._key, new_val)
            return new_val

    def send_event(self, target_node: str, data: dict) -> int:
        """
        Envía un mensaje a otro nodo con timestamp de Lamport.
        Publica en el canal del nodo destino.
        """
        ts = self.tick()
        message = {
            'from': self._node_id,
            'to': target_node,
            'lamport_ts': ts,
            'data': data,
            'wall_time': datetime.now().isoformat()
        }
        self._r.publish(f"msg:{target_node}", json.dumps(message))
        self._log.info("→ Mensaje a '%s' (Lamport=%d)", target_node, ts)
        return ts

    def start_listener(self, callback) -> threading.Thread:
        """
        Inicia un hilo que escucha mensajes entrantes y actualiza el reloj.
        callback(sender, data, lamport_ts) se llama por cada mensaje.
        """
        def _listen():
            pubsub = self._r.pubsub()
            pubsub.subscribe(f"msg:{self._node_id}")
            for msg in pubsub.listen():
                if msg['type'] != 'message':
                    continue
                try:
                    payload = json.loads(msg['data'])
                    remote_ts = payload.get('lamport_ts', 0)
                    local_ts = self.update(remote_ts)
                    self._log.info("← Mensaje de '%s' (Lamport remoto=%d, local=%d)",
                                    payload.get('from'), remote_ts, local_ts)
                    callback(payload.get('from'), payload.get('data', {}), local_ts)
                except Exception as e:
                    self._log.error("Error procesando mensaje: %s", e)

        t = threading.Thread(target=_listen, daemon=True)
        t.start()
        return t
