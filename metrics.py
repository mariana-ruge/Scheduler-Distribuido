import json
import time
import logging
from datetime import datetime
from typing import Optional

import redis

from config import CONFIG


class Metrics:

    def __init__(self, r: redis.Redis, node_id: str):
        self._r = r
        self._node_id = node_id
        self._log = logging.getLogger("METRICS")
        self._node_key = f"metrics:node:{node_id}"
        # Inicializar contadores si no existen
        if not self._r.exists(self._node_key):
            self._r.hset(self._node_key, mapping={
                'tasks_assigned': 0,
                'tasks_completed': 0,
                'tasks_failed': 0,
                'tasks_reassigned': 0,
                'total_exec_time_ms': 0,
                'failures_detected': 0,
                'started_at': datetime.now().isoformat(),
            })

    # ── Registro de eventos ──────────────────────────────────────────

    def record_event(self, event_type: str, meta: Optional[dict] = None,
                     node_id: Optional[str] = None):
    
        event = {
            'type': event_type,
            'node': node_id or self._node_id,
            'timestamp': datetime.now().isoformat(),
            'epoch': time.time(),
        }
        if meta:
            event['meta'] = json.dumps(meta)

        self._r.lpush("metrics:events", json.dumps(event))
        self._r.ltrim("metrics:events", 0, CONFIG.metrics_stream_maxlen - 1)

    # ── Contadores ───────────────────────────────────────────────────

    def inc_assigned(self, node_id: Optional[str] = None):
        key = f"metrics:node:{node_id or self._node_id}"
        self._r.hincrby(key, 'tasks_assigned', 1)

    def inc_completed(self, exec_time_ms: float = 0):
        self._r.hincrby(self._node_key, 'tasks_completed', 1)
        if exec_time_ms > 0:
            self._r.hincrby(self._node_key, 'total_exec_time_ms', int(exec_time_ms))
            self._r.lpush(f"metrics:latency:{self._node_id}", int(exec_time_ms))
            self._r.ltrim(f"metrics:latency:{self._node_id}", 0, 99)

    def inc_failed(self):
        self._r.hincrby(self._node_key, 'tasks_failed', 1)

    def inc_reassigned(self, count: int = 1):
        self._r.hincrby(self._node_key, 'tasks_reassigned', count)

    def inc_failures_detected(self):
        self._r.hincrby(self._node_key, 'failures_detected', 1)

    # ── Consultas ────────────────────────────────────────────────────

    def get_node_stats(self, node_id: Optional[str] = None) -> dict:
        """Retorna estadísticas de un nodo específico."""
        key = f"metrics:node:{node_id or self._node_id}"
        raw = self._r.hgetall(key)
        if not raw:
            return {}

        assigned = int(raw.get('tasks_assigned', 0))
        completed = int(raw.get('tasks_completed', 0))
        failed = int(raw.get('tasks_failed', 0))
        total_time = int(raw.get('total_exec_time_ms', 0))

        stats = {
            'tasks_assigned': assigned,
            'tasks_completed': completed,
            'tasks_failed': failed,
            'tasks_reassigned': int(raw.get('tasks_reassigned', 0)),
            'failures_detected': int(raw.get('failures_detected', 0)),
            'success_rate': f"{(completed / assigned * 100):.1f}%" if assigned > 0 else "N/A",
            'avg_exec_time_ms': round(total_time / completed) if completed > 0 else 0,
            'started_at': raw.get('started_at', '?'),
        }
        return stats

    def get_cluster_summary(self) -> dict:
        """Retorna un resumen global del clúster con KPIs."""
        summary = {
            'total_assigned': 0,
            'total_completed': 0,
            'total_failed': 0,
            'total_reassigned': 0,
            'nodes': {},
        }

        for k in self._r.scan_iter(match="metrics:node:*", count=100):
            nid = k.split(":")[-1]
            stats = self.get_node_stats(nid)
            summary['nodes'][nid] = stats
            summary['total_assigned'] += stats.get('tasks_assigned', 0)
            summary['total_completed'] += stats.get('tasks_completed', 0)
            summary['total_failed'] += stats.get('tasks_failed', 0)
            summary['total_reassigned'] += stats.get('tasks_reassigned', 0)

        total = summary['total_assigned']
        summary['global_success_rate'] = (
            f"{(summary['total_completed'] / total * 100):.1f}%" if total > 0 else "N/A"
        )
        return summary

    def get_recent_events(self, count: int = 20) -> list:
        """Retorna los últimos N eventos del clúster."""
        raw_events = self._r.lrange("metrics:events", 0, count - 1)
        events = []
        for raw in raw_events:
            try:
                events.append(json.loads(raw))
            except json.JSONDecodeError:
                pass
        return events

    def get_latency_stats(self, node_id: Optional[str] = None) -> dict:
        """Retorna estadísticas de latencia de las últimas 100 tareas."""
        key = f"metrics:latency:{node_id or self._node_id}"
        raw = self._r.lrange(key, 0, 99)
        if not raw:
            return {'count': 0, 'avg_ms': 0, 'min_ms': 0, 'max_ms': 0, 'p95_ms': 0}

        values = sorted([int(v) for v in raw])
        count = len(values)
        p95_idx = int(count * 0.95)
        return {
            'count': count,
            'avg_ms': round(sum(values) / count),
            'min_ms': values[0],
            'max_ms': values[-1],
            'p95_ms': values[min(p95_idx, count - 1)],
        }

    # ── Exportación ──────────────────────────────────────────────────

    def export_report(self, format: str = "json") -> str:
        """Genera un reporte estructurado del clúster."""
        report = {
            'generated_at': datetime.now().isoformat(),
            'generated_by': self._node_id,
            'cluster': self.get_cluster_summary(),
            'recent_events': self.get_recent_events(50),
        }
        # Añadir latencia por nodo
        for nid in report['cluster']['nodes']:
            report['cluster']['nodes'][nid]['latency'] = self.get_latency_stats(nid)

        if format == "json":
            return json.dumps(report, indent=2, ensure_ascii=False)
        # Formato texto legible
        lines = [
            "=" * 60,
            f"  REPORTE DEL CLÚSTER - {report['generated_at'][:19]}",
            "=" * 60,
            f"  Total asignadas:  {report['cluster']['total_assigned']}",
            f"  Total completadas: {report['cluster']['total_completed']}",
            f"  Total fallidas:   {report['cluster']['total_failed']}",
            f"  Tasa de éxito:    {report['cluster']['global_success_rate']}",
            "-" * 60,
        ]
        for nid, stats in report['cluster']['nodes'].items():
            lines.append(f"  [{nid}]")
            lines.append(f"    Asignadas: {stats['tasks_assigned']}  "
                          f"Completadas: {stats['tasks_completed']}  "
                          f"Fallidas: {stats['tasks_failed']}")
            lines.append(f"    Éxito: {stats['success_rate']}  "
                          f"Latencia avg: {stats['avg_exec_time_ms']}ms")
            lat = stats.get('latency', {})
            if lat.get('count', 0) > 0:
                lines.append(f"    Latencia p95: {lat['p95_ms']}ms  "
                              f"min: {lat['min_ms']}ms  max: {lat['max_ms']}ms")
        lines.append("=" * 60)
        return "\n".join(lines)
