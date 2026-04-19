"""
Configuración centralizada para la simulación del clúster distribuido.
Todos los parámetros ajustables están aquí para facilitar el tuning.
"""
from dataclasses import dataclass, field


@dataclass
class SimConfig:
    # ── Redis ────────────────────────────────────────────────────────
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    redis_max_connections: int = 10
    redis_socket_timeout: float = 5.0
    redis_retry_on_timeout: bool = True

    # ── Heartbeat y detección de fallos ──────────────────────────────
    heartbeat_interval: float = 2.0       # Segundos entre heartbeats
    heartbeat_ttl: int = 10               # TTL de la clave node:{id} en Redis
    failure_threshold: float = 5.0        # Segundos sin heartbeat → SUSPECTED
    failure_confirmed: float = 10.0       # Segundos sin heartbeat → FAILED
    fault_check_interval: float = 3.0     # Cada cuánto chequear nodos caídos

    # ── Score del scheduler ──────────────────────────────────────────
    weight_reputation: float = 0.4
    weight_load: float = 0.5
    weight_availability: float = 10.0     # Bonus si el nodo está vivo
    queue_penalty_per_task: float = 5.0   # Penalización por tarea en cola
    queue_penalty_max: float = 30.0

    # ── Reputación ───────────────────────────────────────────────────
    reputation_initial: int = 70
    reputation_max: int = 100
    reputation_min: int = 0
    reputation_on_success: int = 5        # +5 por tarea exitosa
    reputation_on_failure: int = -10      # -10 por tarea fallida/timeout
    reputation_decay_per_hour: float = 0.5

    # ── Carga simulada ───────────────────────────────────────────────
    # SIMULACIÓN: como todos corren en la misma máquina, generamos
    # carga artificial diferente por nodo
    simulated_load_min: int = 10
    simulated_load_max: int = 90
    simulated_load_update_interval: float = 3.0
    # Perfiles de carga por nodo (patrón cíclico)
    load_profiles: dict = field(default_factory=lambda: {
        'worker-1': {'base': 25, 'amplitude': 20, 'period': 30},
        'worker-2': {'base': 45, 'amplitude': 30, 'period': 20},
        'scheduler': {'base': 15, 'amplitude': 10, 'period': 40},
    })

    # ── Memoria distribuida ──────────────────────────────────────────
    memory_default_ttl: int = 300         # TTL por defecto en segundos
    memory_default_replicas: int = 3
    # SIMULACIÓN: latencia artificial de red
    simulate_network_latency: bool = True
    network_latency_min: float = 0.01
    network_latency_max: float = 0.05

    # ── Sincronización ───────────────────────────────────────────────
    lock_default_timeout_ms: int = 10000
    barrier_default_timeout: int = 30
    barrier_poll_interval: float = 0.3

    # ── Worker ───────────────────────────────────────────────────────
    worker_poll_interval: float = 1.0     # Cada cuánto buscar tareas
    task_timeout: float = 30.0            # Timeout de ejecución de tarea

    # ── Métricas ─────────────────────────────────────────────────────
    metrics_stream_maxlen: int = 1000     # Máx eventos en stream de Redis
    metrics_summary_interval: float = 10.0

    # ── Logging ──────────────────────────────────────────────────────
    log_level: str = "INFO"
    log_format: str = "%(asctime)s [%(name)-12s] %(message)s"
    log_datefmt: str = "%H:%M:%S"


# Instancia global por defecto
CONFIG = SimConfig()
