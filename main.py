"""
Punto de entrada del sistema distribuido.

Uso:
  Terminal 1:  python main.py --node-id scheduler
  Terminal 2:  python main.py --node-id worker-1
  Terminal 3:  python main.py --node-id worker-2

El nodo 'scheduler' asigna tareas y monitorea.
Los nodos 'worker-*' procesan tareas y reportan resultados.
"""
import sys
import json
import time
import random
import signal
import logging
import argparse
import threading
from datetime import datetime

import redis

from config import CONFIG
from synchronization import DistributedLock, DistributedBarrier, LogicalClock
from distributed_memory import DistributedMemory
from metrics import Metrics
from scheduler import DistributedScheduler
from fault_tolerance import FailureDetector, ReconfigurationManager, simulate_crash


def setup_logging(node_id: str):
    logging.basicConfig(
        level=getattr(logging, CONFIG.log_level),
        format=f"%(asctime)s [{node_id:>10}|%(name)-8s] %(message)s",
        datefmt=CONFIG.log_datefmt,
    )


def create_redis_pool() -> redis.Redis:
    pool = redis.ConnectionPool(
        host=CONFIG.redis_host,
        port=CONFIG.redis_port,
        db=CONFIG.redis_db,
        max_connections=CONFIG.redis_max_connections,
        socket_timeout=CONFIG.redis_socket_timeout,
        retry_on_timeout=CONFIG.redis_retry_on_timeout,
        decode_responses=True,
    )
    r = redis.Redis(connection_pool=pool)
    r.ping()
    return r


# ═══════════════════════════════════════════════════════════════════════════
#  WORKER: procesa tareas de su cola
# ═══════════════════════════════════════════════════════════════════════════

class WorkerLoop:
    """
    Ciclo de trabajo: extrae tareas de la cola del nodo,
    las ejecuta (simulado) y reporta resultados.
    """

    def __init__(self, sched: DistributedScheduler, metrics: Metrics, node_id: str):
        self._sched = sched
        self._metrics = metrics
        self._node_id = node_id
        self._log = logging.getLogger("WORKER")
        self._running = threading.Event()
        self._running.set()

    def start(self):
        t = threading.Thread(target=self._loop, daemon=True)
        t.start()
        return t

    def stop(self):
        self._running.clear()

    def _loop(self):
        """Bucle principal del worker."""
        self._log.info("Worker iniciado, esperando tareas en queue:%s", self._node_id)
        while self._running.is_set():
            try:
                task_id, task_data = self._sched.get_pending_task()
                if task_id and task_data:
                    self._execute_task(task_id, task_data)
                else:
                    self._running.wait(timeout=CONFIG.worker_poll_interval)
            except Exception as e:
                self._log.error("Error en worker: %s", e)
                self._running.wait(timeout=CONFIG.worker_poll_interval)

    def _execute_task(self, task_id: str, task_data: dict):
        """Simula ejecución de tarea con probabilidad de fallo."""
        payload_str = task_data.get('data', '{}')
        try:
            payload = json.loads(payload_str)
        except json.JSONDecodeError:
            payload = {}

        task_type = payload.get('type', 'generic')
        duration = payload.get('duration', random.uniform(0.5, 3.0))
        fail_chance = payload.get('fail_chance', 0.1)

        self._log.info(
            "⚡ Ejecutando %s [tipo=%s, duración=%.1fs]",
            task_id, task_type, duration
        )

        start = time.time()
        # Simular trabajo
        time.sleep(duration)
        elapsed_ms = (time.time() - start) * 1000

        # Simular posible fallo
        success = random.random() > fail_chance
        result = {
            'output': f"Resultado de {task_type}" if success else "ERROR: ejecución fallida",
            'elapsed_ms': round(elapsed_ms),
        }

        self._sched.complete_task(task_id, result, success, elapsed_ms)

        if success:
            self._log.info("✔ %s completada en %dms", task_id, round(elapsed_ms))
        else:
            self._log.warning("✖ %s FALLIDA tras %dms", task_id, round(elapsed_ms))


# ═══════════════════════════════════════════════════════════════════════════
#  CLI interactivo (solo para el scheduler)
# ═══════════════════════════════════════════════════════════════════════════

class SchedulerCLI:
    """Comandos interactivos para el nodo scheduler."""

    HELP = """
╔══════════════════════════════════════════════════════════╗
║  COMANDOS DISPONIBLES (Nodo Scheduler)                  ║
╠══════════════════════════════════════════════════════════╣
║  task [tipo] [N]     → Crear N tareas (default: 1)      ║
║  status              → Estado de todos los nodos         ║
║  scores              → Mostrar puntuación de nodos       ║
║  metrics             → Resumen de métricas del clúster   ║
║  report [json|text]  → Exportar reporte completo         ║
║  events [N]          → Últimos N eventos (default: 10)   ║
║  crash <nodo>        → Simular caída de un nodo          ║
║  mem set <k> <v>     → Escribir en memoria distribuida   ║
║  mem get <k>         → Leer de memoria distribuida       ║
║  mem keys            → Listar claves en memoria          ║
║  lock <nombre>       → Adquirir lock distribuido         ║
║  unlock <nombre>     → Liberar lock distribuido          ║
║  barrier <N>         → Esperar barrera con N nodos       ║
║  flush               → Limpiar todo Redis (¡cuidado!)    ║
║  help                → Mostrar esta ayuda                ║
║  exit / quit         → Salir                             ║
╚══════════════════════════════════════════════════════════╝
"""

    def __init__(self, r: redis.Redis, sched: DistributedScheduler,
                 metrics: Metrics, memory: DistributedMemory,
                 clock: LogicalClock, fd: FailureDetector):
        self._r = r
        self._sched = sched
        self._metrics = metrics
        self._memory = memory
        self._clock = clock
        self._fd = fd
        self._log = logging.getLogger("CLI")
        self._locks: dict = {}

    def run(self):
        print(self.HELP)
        while True:
            try:
                raw = input("\n[scheduler] > ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not raw:
                continue
            parts = raw.split()
            cmd = parts[0].lower()
            args = parts[1:]

            try:
                if cmd in ('exit', 'quit'):
                    break
                elif cmd == 'help':
                    print(self.HELP)
                elif cmd == 'task':
                    self._cmd_task(args)
                elif cmd == 'status':
                    self._cmd_status()
                elif cmd == 'scores':
                    self._cmd_scores()
                elif cmd == 'metrics':
                    self._cmd_metrics()
                elif cmd == 'report':
                    self._cmd_report(args)
                elif cmd == 'events':
                    self._cmd_events(args)
                elif cmd == 'crash':
                    self._cmd_crash(args)
                elif cmd == 'mem':
                    self._cmd_mem(args)
                elif cmd == 'lock':
                    self._cmd_lock(args)
                elif cmd == 'unlock':
                    self._cmd_unlock(args)
                elif cmd == 'barrier':
                    self._cmd_barrier(args)
                elif cmd == 'flush':
                    self._cmd_flush()
                else:
                    print(f"Comando desconocido: '{cmd}'. Escribe 'help'.")
            except Exception as e:
                print(f"Error: {e}")

    def _cmd_task(self, args):
        task_type = args[0] if args else 'compute'
        count = int(args[1]) if len(args) > 1 else 1
        for i in range(count):
            payload = {
                'type': task_type,
                'duration': round(random.uniform(0.5, 3.0), 1),
                'fail_chance': 0.1,
                'priority': random.choice(['low', 'medium', 'high']),
            }
            tid = self._sched.assign_task(payload)
            if tid:
                self._clock.send_event(f"task_created:{tid}")
                print(f"  [{i+1}/{count}] {tid} → tipo={task_type}")
            else:
                print(f"  [{i+1}/{count}] No hay nodos disponibles")

    def _cmd_status(self):
        print("\n" + "=" * 62)
        print("  ESTADO DEL CLÚSTER")
        print("=" * 62)
        states = self._fd.node_states
        for k in self._r.scan_iter(match="node:*", count=100):
            data = self._r.hgetall(k)
            nid = k.split(":", 1)[1]
            state = states.get(nid, "UNKNOWN")
            if hasattr(state, 'value'):
                state = state.value
            q_len = self._r.llen(f"queue:{nid}")
            print(f"  {nid:>12} │ estado={state:<10} rep={data.get('reputation', '?'):>3}  "
                  f"carga={data.get('current_load', '?'):>3}  cola={q_len}")
        print("=" * 62)

    def _cmd_scores(self):
        print("\n  Puntuaciones (rep×0.4 + (100-carga)×0.5 + bonus_vivo - pen_cola):")
        for k in self._r.scan_iter(match="node:*", count=100):
            data = self._r.hgetall(k)
            nid = k.split(":", 1)[1]
            rep = int(data.get('reputation', 70))
            load = int(data.get('current_load', 50))
            alive = data.get('available', '0') == '1'
            q_len = self._r.llen(f"queue:{nid}")
            q_pen = min(q_len * CONFIG.queue_penalty_per_task, CONFIG.queue_penalty_max)
            score = (rep * CONFIG.weight_reputation
                     + (100 - load) * CONFIG.weight_load
                     + (CONFIG.weight_availability if alive else 0)
                     - q_pen)
            print(f"    {nid:>12} │ score={score:>6.1f}  "
                  f"(rep={rep}, carga={load}, vivo={'SÍ' if alive else 'NO'}, "
                  f"cola={q_len}, pen={q_pen:.0f})")
        best = self._sched.get_best_node()
        print(f"\n  → Mejor nodo: {best or 'ninguno'}")

    def _cmd_metrics(self):
        summary = self._metrics.get_cluster_summary()
        print(f"\n  Asignadas: {summary['total_assigned']}  "
              f"Completadas: {summary['total_completed']}  "
              f"Fallidas: {summary['total_failed']}  "
              f"Reasignadas: {summary['total_reassigned']}  "
              f"Éxito global: {summary['global_success_rate']}")
        for nid, stats in summary['nodes'].items():
            print(f"    {nid}: asign={stats['tasks_assigned']} "
                  f"comp={stats['tasks_completed']} "
                  f"fall={stats['tasks_failed']} "
                  f"éxito={stats['success_rate']} "
                  f"avg_lat={stats['avg_exec_time_ms']}ms")

    def _cmd_report(self, args):
        fmt = args[0] if args else 'text'
        print(self._metrics.export_report(format=fmt))

    def _cmd_events(self, args):
        count = int(args[0]) if args else 10
        events = self._metrics.get_recent_events(count)
        print(f"\n  Últimos {len(events)} eventos:")
        for ev in events:
            ts = ev.get('timestamp', '?')[:19]
            print(f"    {ts} [{ev.get('type', '?'):>20}] "
                  f"nodo={ev.get('node', '?')}  "
                  f"{ev.get('meta', '')}")

    def _cmd_crash(self, args):
        if not args:
            print("  Uso: crash <node-id>  (ej: crash worker-1)")
            return
        target = args[0]
        simulate_crash(self._r, target)
        self._clock.send_event(f"crash_simulated:{target}")
        print(f"  Crash simulado para '{target}'. El detector lo marcará como FAILED.")

    def _cmd_mem(self, args):
        if not args:
            print("  Uso: mem set <key> <value> | mem get <key> | mem keys")
            return
        sub = args[0].lower()
        if sub == 'set' and len(args) >= 3:
            key = args[1]
            value = " ".join(args[2:])
            self._memory.store(key, value)
            print(f"  Almacenado: {key} = {value}")
        elif sub == 'get' and len(args) >= 2:
            result = self._memory.get_with_metadata(args[1])
            if result:
                print(f"  {args[1]} = {result.get('value')}")
                print(f"  Versión: {result.get('version')}  "
                      f"Actualizado por: {result.get('updated_by')}")
            else:
                print(f"  Clave '{args[1]}' no encontrada")
        elif sub == 'keys':
            keys = self._memory.list_keys()
            print(f"  Claves ({len(keys)}): {', '.join(keys) if keys else '(ninguna)'}")
        else:
            print("  Uso: mem set <key> <value> | mem get <key> | mem keys")

    def _cmd_lock(self, args):
        if not args:
            print("  Uso: lock <nombre>")
            return
        name = args[0]
        lock = DistributedLock(self._r, name, "scheduler")
        if lock.acquire():
            self._locks[name] = lock
            self._clock.send_event(f"lock_acquired:{name}")
            print(f"  Lock '{name}' adquirido")
        else:
            print(f"  No se pudo adquirir lock '{name}'")

    def _cmd_unlock(self, args):
        if not args:
            print("  Uso: unlock <nombre>")
            return
        name = args[0]
        lock = self._locks.pop(name, None)
        if lock:
            lock.release()
            print(f"  Lock '{name}' liberado")
        else:
            print(f"  No tienes el lock '{name}'")

    def _cmd_barrier(self, args):
        expected = int(args[0]) if args else 3
        barrier = DistributedBarrier(self._r, "manual_barrier", expected)
        barrier.signal("scheduler")
        print(f"  Esperando barrera ({expected} nodos)...")
        if barrier.wait():
            print("  ✔ Barrera completada, todos los nodos sincronizados")
        else:
            print("  ✖ Timeout esperando barrera")

    def _cmd_flush(self):
        confirm = input("  ¿Borrar TODO en Redis? (si/no): ").strip().lower()
        if confirm == 'si':
            self._r.flushdb()
            print("  Redis limpiado")
        else:
            print("  Cancelado")


# ═══════════════════════════════════════════════════════════════════════════
#  CLI para Workers (más simple)
# ═══════════════════════════════════════════════════════════════════════════

class WorkerCLI:
    """Comandos disponibles para nodos worker."""

    HELP = """
╔══════════════════════════════════════════════════════════╗
║  COMANDOS DISPONIBLES (Worker)                          ║
╠══════════════════════════════════════════════════════════╣
║  status            → Mi estado actual                   ║
║  stats             → Mis estadísticas                   ║
║  queue             → Tareas en mi cola                  ║
║  mem set <k> <v>   → Escribir en memoria distribuida    ║
║  mem get <k>       → Leer de memoria distribuida        ║
║  barrier <N>       → Señalar barrera con N nodos        ║
║  help              → Mostrar esta ayuda                 ║
║  exit / quit       → Salir                              ║
╚══════════════════════════════════════════════════════════╝
"""

    def __init__(self, r: redis.Redis, node_id: str, sched: DistributedScheduler,
                 metrics: Metrics, memory: DistributedMemory, clock: LogicalClock):
        self._r = r
        self._node_id = node_id
        self._sched = sched
        self._metrics = metrics
        self._memory = memory
        self._clock = clock

    def run(self):
        print(self.HELP)
        while True:
            try:
                raw = input(f"\n[{self._node_id}] > ").strip()
            except (EOFError, KeyboardInterrupt):
                break
            if not raw:
                continue
            parts = raw.split()
            cmd = parts[0].lower()
            args = parts[1:]

            try:
                if cmd in ('exit', 'quit'):
                    break
                elif cmd == 'help':
                    print(self.HELP)
                elif cmd == 'status':
                    data = self._r.hgetall(f"node:{self._node_id}")
                    print(f"  Reputación: {data.get('reputation', '?')}  "
                          f"Carga: {data.get('current_load', '?')}  "
                          f"Cola: {self._r.llen(f'queue:{self._node_id}')}")
                elif cmd == 'stats':
                    s = self._metrics.get_node_stats(self._node_id)
                    for k, v in s.items():
                        print(f"  {k}: {v}")
                elif cmd == 'queue':
                    q_len = self._r.llen(f"queue:{self._node_id}")
                    print(f"  Tareas en cola: {q_len}")
                elif cmd == 'mem':
                    self._cmd_mem(args)
                elif cmd == 'barrier':
                    expected = int(args[0]) if args else 3
                    barrier = DistributedBarrier(self._r, "manual_barrier", expected)
                    barrier.signal(self._node_id)
                    print(f"  Barrera señalada. Esperando {expected} nodos...")
                    if barrier.wait():
                        print("  Barrera completada")
                    else:
                        print("  Timeout")
                else:
                    print(f"Comando desconocido: '{cmd}'. Escribe 'help'.")
            except Exception as e:
                print(f"Error: {e}")

    def _cmd_mem(self, args):
        if not args:
            print("  Uso: mem set <key> <value> | mem get <key>")
            return
        sub = args[0].lower()
        if sub == 'set' and len(args) >= 3:
            self._memory.store(args[1], " ".join(args[2:]))
            print(f"  Almacenado: {args[1]}")
        elif sub == 'get' and len(args) >= 2:
            result = self._memory.get_with_metadata(args[1])
            if result:
                print(f"  {args[1]} = {result.get('value')} (v{result.get('version')})")
            else:
                print(f"  No encontrado: {args[1]}")


# ═══════════════════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Scheduler Distribuido - Simulación")
    parser.add_argument('--node-id', required=True,
                        help="Identificador del nodo: 'scheduler', 'worker-1', 'worker-2'")
    args = parser.parse_args()
    node_id = args.node_id

    setup_logging(node_id)
    log = logging.getLogger("MAIN")

    # ── Conexión Redis ───────────────────────────────────────────────
    try:
        r = create_redis_pool()
        log.info("Conectado a Redis %s:%d", CONFIG.redis_host, CONFIG.redis_port)
    except redis.ConnectionError:
        print(f"ERROR: No se puede conectar a Redis en "
              f"{CONFIG.redis_host}:{CONFIG.redis_port}")
        print("Asegúrate de que Redis está corriendo (redis-server)")
        sys.exit(1)

    # ── Inicializar subsistemas ──────────────────────────────────────
    metrics = Metrics(r, node_id)
    sched = DistributedScheduler(r, node_id, metrics)
    memory = DistributedMemory(r, node_id)
    clock = LogicalClock(r, node_id)
    fd = FailureDetector(r, node_id, metrics)
    reconf = ReconfigurationManager(r, node_id, fd, metrics)

    # Iniciar subsistemas
    fd.start()
    reconf.start()
    clock.start_listener(lambda src, ts, evt: log.debug("Clock event: %s from %s @%d", evt, src, ts))

    # Listener de topología
    def on_topology_change(event):
        log.warning("Cambio de topología: %s → nodo '%s'",
                     event.get('type'), event.get('affected_node'))
    reconf.listen_topology_changes(on_topology_change)

    # Listener de cambios en memoria
    def on_memory_change(key, action):
        log.debug("Memoria: %s → %s", action, key)
    memory.watch_changes(on_memory_change)

    # ── Worker loop (todos los nodos procesan tareas) ────────────────
    worker = WorkerLoop(sched, metrics, node_id)
    worker.start()

    is_scheduler = node_id == "scheduler"

    print("=" * 62)
    print(f"  Nodo '{node_id}' iniciado como {'SCHEDULER' if is_scheduler else 'WORKER'}")
    print(f"  Redis: {CONFIG.redis_host}:{CONFIG.redis_port}")
    print(f"  Heartbeat cada {CONFIG.heartbeat_interval}s, TTL {CONFIG.heartbeat_ttl}s")
    print("=" * 62)

    # ── CLI ──────────────────────────────────────────────────────────
    try:
        if is_scheduler:
            cli = SchedulerCLI(r, sched, metrics, memory, clock, fd)
        else:
            cli = WorkerCLI(r, node_id, sched, metrics, memory, clock)
        cli.run()
    except KeyboardInterrupt:
        pass
    finally:
        print("\nCerrando subsistemas...")
        worker.stop()
        fd.stop()
        reconf.stop()
        sched.stop()
        log.info("Nodo '%s' apagado correctamente", node_id)
        print("Adiós.")


if __name__ == "__main__":
    main()
