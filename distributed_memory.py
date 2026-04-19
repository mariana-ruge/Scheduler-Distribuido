import json
import time
import random
import logging
import threading
from typing import Any, Optional, Tuple, List

import redis

from config import CONFIG


class DistributedMemory:

    PREFIX = "dmem:"

    def __init__(self, r: redis.Redis, node_id: str):
        self._r = r
        self._node_id = node_id
        self._log = logging.getLogger(f"MEMORY")
        self._local_cache: dict = {}  # Cache local para lecturas rápidas
        self._cache_lock = threading.Lock()

    def _simulate_latency(self):
        """SIMULACIÓN: latencia artificial de red entre nodos."""
        if CONFIG.simulate_network_latency:
            time.sleep(random.uniform(CONFIG.network_latency_min,
                                       CONFIG.network_latency_max))

    def _full_key(self, key: str) -> str:
        return f"{self.PREFIX}{key}"

    # ── Escritura ────────────────────────────────────────────────────

    def store(self, key: str, value: Any, ttl: int = CONFIG.memory_default_ttl) -> int:
        """
        Guarda un dato en memoria compartida con versionado.
        Retorna la nueva versión del dato.
        """
        self._simulate_latency()
        full_key = self._full_key(key)
        serialized = json.dumps(value)

        pipe = self._r.pipeline(transaction=True)
        pipe.hincrby(full_key, "version", 1)
        pipe.hset(full_key, mapping={
            'data': serialized,
            'updated_by': self._node_id,
            'updated_at': time.time(),
        })
        if ttl > 0:
            pipe.expire(full_key, ttl)
        # Notificar a otros nodos
        pipe.publish("dmem:changes", json.dumps({
            'action': 'store',
            'key': key,
            'node': self._node_id,
            'timestamp': time.time()
        }))
        results = pipe.execute()
        version = results[0]  # resultado de hincrby

        # Actualizar cache local
        with self._cache_lock:
            self._local_cache[key] = (value, version)

        self._log.info("STORE '%s' = %s (v%d, ttl=%ds)", key,
                        str(value)[:50], version, ttl)
        return version

    # ── Replicación ──────────────────────────────────────────────────

    def replicate(self, key: str, replicas: int = CONFIG.memory_default_replicas) -> int:
        """
        Crea N réplicas del dato en claves separadas.
        Retorna el número de réplicas creadas.
        """
        self._simulate_latency()
        full_key = self._full_key(key)
        source_data = self._r.hgetall(full_key)
        if not source_data:
            self._log.warning("REPLICATE '%s': clave no existe", key)
            return 0

        pipe = self._r.pipeline(transaction=False)
        for i in range(1, replicas + 1):
            replica_key = f"{full_key}:replica:{i}"
            pipe.hset(replica_key, mapping=source_data)
            ttl = self._r.ttl(full_key)
            if ttl > 0:
                pipe.expire(replica_key, ttl)
        pipe.execute()

        self._log.info("REPLICATE '%s': %d réplicas creadas", key, replicas)
        return replicas

    # ── Lectura ──────────────────────────────────────────────────────

    def get(self, key: str, fallback_to_replica: bool = True) -> Tuple[Optional[Any], int]:
        """
        Lee un dato. Si la clave principal no existe y fallback_to_replica=True,
        busca en réplicas automáticamente.
        Retorna (valor, versión) o (None, 0).
        """
        self._simulate_latency()
        full_key = self._full_key(key)
        data = self._r.hgetall(full_key)

        if data and 'data' in data:
            value = json.loads(data['data'])
            version = int(data.get('version', 0))
            with self._cache_lock:
                self._local_cache[key] = (value, version)
            return value, version

        # Fallback a réplicas
        if fallback_to_replica:
            for i in range(1, CONFIG.memory_default_replicas + 1):
                replica_key = f"{full_key}:replica:{i}"
                rdata = self._r.hgetall(replica_key)
                if rdata and 'data' in rdata:
                    value = json.loads(rdata['data'])
                    version = int(rdata.get('version', 0))
                    self._log.info("GET '%s': recuperado de réplica %d (v%d)",
                                    key, i, version)
                    # Restaurar clave principal desde réplica
                    self._r.hset(full_key, mapping=rdata)
                    return value, version

        # Último recurso: cache local
        with self._cache_lock:
            if key in self._local_cache:
                val, ver = self._local_cache[key]
                self._log.info("GET '%s': recuperado de cache local (v%d)", key, ver)
                return val, ver

        return None, 0

    def get_with_metadata(self, key: str) -> Optional[dict]:
        """Lee el dato con todos sus metadatos (versión, quién lo escribió, cuándo)."""
        data = self._r.hgetall(self._full_key(key))
        if not data or 'data' not in data:
            return None
        return {
            'value': json.loads(data['data']),
            'version': int(data.get('version', 0)),
            'updated_by': data.get('updated_by', '?'),
            'updated_at': float(data.get('updated_at', 0)),
        }

    # ── Invalidación ─────────────────────────────────────────────────

    def invalidate(self, key: str, propagate: bool = True) -> bool:
        """
        Elimina un dato y todas sus réplicas.
        Si propagate=True, notifica a otros nodos via Pub/Sub.
        """
        self._simulate_latency()
        full_key = self._full_key(key)
        pipe = self._r.pipeline(transaction=False)
        pipe.delete(full_key)
        for i in range(1, CONFIG.memory_default_replicas + 1):
            pipe.delete(f"{full_key}:replica:{i}")
        if propagate:
            pipe.publish("dmem:changes", json.dumps({
                'action': 'invalidate',
                'key': key,
                'node': self._node_id,
                'timestamp': time.time()
            }))
        pipe.execute()

        with self._cache_lock:
            self._local_cache.pop(key, None)

        self._log.info("INVALIDATE '%s' (propagate=%s)", key, propagate)
        return True

    # ── Utilidades ───────────────────────────────────────────────────

    def list_keys(self) -> List[str]:
        """Lista todas las claves principales (sin réplicas)."""
        keys = []
        for k in self._r.scan_iter(match=f"{self.PREFIX}*", count=100):
            if ":replica:" not in k:
                keys.append(k.replace(self.PREFIX, ""))
        return sorted(keys)

    def watch_changes(self, callback) -> threading.Thread:
        """
        Suscribe a cambios en la memoria distribuida.
        callback(event_dict) se llama cuando otro nodo modifica un dato.
        """
        def _listener():
            pubsub = self._r.pubsub()
            pubsub.subscribe("dmem:changes")
            for msg in pubsub.listen():
                if msg['type'] != 'message':
                    continue
                try:
                    event = json.loads(msg['data'])
                    if event.get('node') != self._node_id:
                        callback(event)
                except Exception:
                    pass

        t = threading.Thread(target=_listener, daemon=True)
        t.start()
        return t
