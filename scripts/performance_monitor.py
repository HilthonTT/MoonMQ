import argparse
import logging
import time
import psutil
import requests
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway, start_http_server
from prometheus_client.parser import text_string_to_metric_families

# Own registry so we control exactly what gets pushed/exposed (and don't drag in
# the default process/GC collectors when pushing to a gateway).
REGISTRY = CollectorRegistry()

# Host-level gauges (psutil). Labelled by `instance` (the broker's host:port) so
# several monitored brokers don't collide in a shared Pushgateway/Prometheus.
host_cpu_percent = Gauge(
    "moonmq_host_cpu_usage_percent",
    "Host CPU usage percentage",
    ["instance"],
    registry=REGISTRY,
)

host_memory_bytes = Gauge(
    "moonmq_host_memory_usage_bytes",
    "Host memory used in bytes",
    ["instance"],
    registry=REGISTRY,
)

host_memory_percent = Gauge(
    "moonmq_host_memory_usage_percent",
    "Host memory usage percentage",
    ["instance"],
    registry=REGISTRY,
)

data_disk_percent = Gauge(
    "moonmq_data_disk_usage_percent",
    "Disk usage percentage of the broker data directory",
    ["instance", "path"],
    registry=REGISTRY,
)

host_network_in = Gauge(
    "moonmq_host_network_in_bytes_per_sec",
    "Host network input bytes per second",
    ["instance"],
    registry=REGISTRY,
)

host_network_out = Gauge(
    "moonmq_host_network_out_bytes_per_sec",
    "Host network output bytes per second",
    ["instance"],
    registry=REGISTRY,
)

# Broker-level gauges derived from MoonMQ's own counters. MoonMQ exposes these as
# monotonic counters only; we re-export per-second rates, which it does not.
broker_bytes_sent_rate = Gauge(
    "moonmq_broker_bytes_sent_per_sec",
    "Bytes sent to clients per second",
    ["instance"],
    registry=REGISTRY,
)

broker_produce_rate = Gauge(
    "moonmq_broker_produce_records_per_sec",
    "Records produced per second across all topics",
    ["instance"],
    registry=REGISTRY,
)

broker_fetch_rate = Gauge(
    "moonmq_broker_fetch_records_per_sec",
    "Records delivered to consumers per second across all topics",
    ["instance"],
    registry=REGISTRY,
)

broker_connections_open = Gauge(
    "moonmq_broker_connections_open",
    "Currently open client connections",
    ["instance"],
    registry=REGISTRY,
)

broker_topic_count = Gauge(
    "moonmq_broker_topic_count",
    "Number of topics on the broker",
    ["instance"],
    registry=REGISTRY,
)

# Counter series (literal names — MoonMQ emits no # TYPE lines, so the parser
# keeps the `_total` suffix verbatim) summed across labels into a broker rate.
_RATE_COUNTERS = {
    "moonmq_bytes_sent_total": broker_bytes_sent_rate,
    "moonmq_produce_records_total": broker_produce_rate,
    "moonmq_fetch_records_total": broker_fetch_rate,
}


class MoonMQAdminClient:
    """Minimal admin/observability client for a MoonMQ broker.

    MoonMQ has no Kafka-style admin RPC; its administrative surface is the HTTP
    metrics server (src/server/metrics_http.lua). This client wraps the /stats,
    /metrics and /health endpoints behind a small Kafka-flavoured API. Methods
    raise requests.RequestException on transport failure — callers decide how to
    degrade.
    """

    def __init__(self, base_url, request_timeout=5):
        self.base_url = base_url.rstrip("/")
        self.metrics_url = self.base_url + "/metrics"
        self.stats_url = self.base_url + "/stats"
        self.health_url = self.base_url + "/health"
        self.request_timeout = request_timeout

    def _get(self, url):
        response = requests.get(url, timeout=self.request_timeout)
        response.raise_for_status()
        return response

    def stats(self):
        """Return the parsed /stats JSON snapshot."""
        return self._get(self.stats_url).json()

    def describe_cluster(self):
        """Cluster metadata, Kafka-style.

        MoonMQ is single-broker, so `brokers` always holds exactly one entry,
        derived from the /stats `server` section. The raw snapshot is attached
        as `stats` so callers don't have to re-fetch it.
        """
        stats = self.stats()
        srv = (stats or {}).get("server") or {}
        broker = {
            "node_id": "{}:{}".format(srv.get("host", "?"), srv.get("port", "?")),
            "host": srv.get("host"),
            "port": srv.get("port"),
            "version": srv.get("version"),
            "protocol": srv.get("protocol"),
        }
        return {"brokers": [broker], "stats": stats}

    def list_topics(self):
        """Return the /stats `topics` section (count, max, top_by_bytes, ...)."""
        return (self.stats() or {}).get("topics") or {}

    def health(self):
        """True when /health reports the broker is alive."""
        try:
            return self._get(self.health_url).text.strip() == "ok"
        except requests.RequestException:
            return False

    def scrape_counters(self, names):
        """Sum the named counter families across all label sets.

        Returns a dict: counter name -> summed value, for the series present.
        """
        text = self._get(self.metrics_url).text
        sums = {}
        for family in text_string_to_metric_families(text):
            if family.name in names:
                sums[family.name] = sum(s.value for s in family.samples)
        return sums


class MoonMQPerformanceMonitor:
    def __init__(
        self,
        admin: MoonMQAdminClient,
        data_dir,
        push_gateway=None,
        cpu_threshold=80,
        memory_threshold=85,
        disk_threshold=85,
        saturation_threshold=0.8,
    ):
        self.admin = admin
        self.data_dir = data_dir
        self.push_gateway = push_gateway
        self.cpu_threshold = cpu_threshold
        self.memory_threshold = memory_threshold
        self.disk_threshold = disk_threshold
        self.saturation_threshold = saturation_threshold
        self.logger = logging.getLogger(__name__)

        # Counters are monotonic; to get a rate we remember the previous scrape.
        self._prev_counters = {}  # counter name -> summed value
        self._prev_net = None     # psutil net counters from the previous cycle
        self._prev_time = None    # time.time() of the previous scrape

    def collect_host_metrics(self, instance, cpu_percent, dt):
        """Record host-level psutil metrics under the given instance label."""
        host_cpu_percent.labels(instance=instance).set(cpu_percent)

        memory = psutil.virtual_memory()
        host_memory_bytes.labels(instance=instance).set(memory.used)
        host_memory_percent.labels(instance=instance).set(memory.percent)

        try:
            disk = psutil.disk_usage(self.data_dir)
            data_disk_percent.labels(instance=instance, path=self.data_dir).set(disk.percent)
        except OSError as exc:
            self.logger.warning("disk usage for %s unavailable: %s", self.data_dir, exc)

        net = psutil.net_io_counters()
        if net is not None and self._prev_net is not None and dt and dt > 0:
            in_rate = max(0.0, (net.bytes_recv - self._prev_net.bytes_recv) / dt)
            out_rate = max(0.0, (net.bytes_sent - self._prev_net.bytes_sent) / dt)
            host_network_in.labels(instance=instance).set(in_rate)
            host_network_out.labels(instance=instance).set(out_rate)
        self._prev_net = net

    def collect_broker_metrics(self, instance, stats, dt):
        """Scrape MoonMQ counters and re-export per-second rates + gauges."""
        try:
            counters = self.admin.scrape_counters(_RATE_COUNTERS.keys())
        except requests.RequestException as exc:
            self.logger.error("failed to scrape %s: %s", self.admin.metrics_url, exc)
            counters = {}

        for name, gauge in _RATE_COUNTERS.items():
            current = counters.get(name)
            previous = self._prev_counters.get(name)
            if current is not None and previous is not None and dt and dt > 0:
                # max(0, ...) clamps counter resets (broker restart) to 0.
                gauge.labels(instance=instance).set(max(0.0, (current - previous) / dt))
            if current is not None:
                self._prev_counters[name] = current

        # Point-in-time broker gauges come straight from /stats.
        if stats:
            conns = stats.get("connections") or {}
            topics = stats.get("topics") or {}
            if conns.get("open") is not None:
                broker_connections_open.labels(instance=instance).set(conns["open"])
            if topics.get("count") is not None:
                broker_topic_count.labels(instance=instance).set(topics["count"])

    def analyze_performance(self, cpu_percent, stats):
        """Return a list of {severity, issue, recommendation} dicts."""
        recommendations = []

        if cpu_percent > self.cpu_threshold:
            recommendations.append({
                "severity": "warning",
                "issue": "High CPU usage",
                "recommendation": "The broker is single-threaded (one reactor loop); "
                                  "consider sharding topics across more broker processes",
            })

        memory = psutil.virtual_memory()
        if memory.percent > self.memory_threshold:
            recommendations.append({
                "severity": "warning",
                "issue": "High memory usage",
                "recommendation": "Reduce in-flight batching/buffer sizes or move to a larger host",
            })

        try:
            disk = psutil.disk_usage(self.data_dir)
            if disk.percent > self.disk_threshold:
                recommendations.append({
                    "severity": "critical",
                    "issue": f"Data directory {self.data_dir} is {disk.percent:.0f}% full",
                    "recommendation": "Lower segment retention or attach more storage",
                })
        except OSError:
            pass

        if stats:
            self._saturation_check(
                recommendations, stats.get("connections"), "open", "max", "Connection")
            self._saturation_check(
                recommendations, stats.get("topics"), "count", "max", "Topic")

        return recommendations

    def _saturation_check(self, recommendations: list, section, used_key, max_key, label):
        if not isinstance(section, dict):
            return
        used, limit = section.get(used_key), section.get(max_key)
        if used is None or not limit:
            return
        if used / limit > self.saturation_threshold:
            recommendations.append({
                "severity": "warning",
                "issue": f"{label} count near limit ({used}/{limit})",
                "recommendation": f"Raise the configured {label.lower()} maximum or shed load",
            })

    def monitor_once(self):
        """Run a single collect/analyze/export cycle."""
        # One cpu_percent sample per cycle (blocks ~1s); reused for analysis.
        cpu_percent = psutil.cpu_percent(interval=1)

        now = time.time()
        dt = (now - self._prev_time) if self._prev_time is not None else None

        try:
            cluster = self.admin.describe_cluster()
            stats = cluster["stats"]
        except (requests.RequestException, ValueError) as exc:
            self.logger.error("failed to describe cluster via %s: %s", self.admin.stats_url, exc)
            cluster, stats = None, None

        # Single broker per process; the loop mirrors a Kafka cluster sweep and
        # degrades to the base URL when /stats is unreachable.
        brokers = cluster["brokers"] if cluster else [{"node_id": self.admin.base_url}]
        for broker in brokers:
            instance = broker["node_id"]
            self.collect_host_metrics(instance, cpu_percent, dt)
            self.collect_broker_metrics(instance, stats, dt)

        self._prev_time = now

        if self.push_gateway:
            try:
                push_to_gateway(
                    self.push_gateway, job="moonmq_performance_monitor", registry=REGISTRY
                )
            except (requests.RequestException, OSError) as exc:
                self.logger.error("failed to push to %s: %s", self.push_gateway, exc)

        for rec in self.analyze_performance(cpu_percent, stats):
            self.logger.warning("[%s] %s: %s", rec["severity"], rec["issue"], rec["recommendation"])


def main():
    parser = argparse.ArgumentParser(description="MoonMQ performance monitor")
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:9090",
        help="MoonMQ metrics server base URL (default: %(default)s)",
    )
    parser.add_argument(
        "--data-dir",
        default="./data_server",
        help="Broker data directory to measure disk usage of (default: %(default)s)",
    )
    parser.add_argument(
        "--push-gateway",
        default=None,
        help="Prometheus Pushgateway address, e.g. localhost:9091 (optional)",
    )
    parser.add_argument(
        "--expose-port",
        type=int,
        default=None,
        help="If set, expose the derived gauges on this local port for scraping",
    )
    parser.add_argument(
        "--cpu-threshold",
        type=float,
        default=80,
        help="Warn when host CPU exceeds this percent (default: %(default)s)",
    )
    parser.add_argument(
        "--memory-threshold",
        type=float,
        default=85,
        help="Warn when host memory exceeds this percent (default: %(default)s)",
    )
    parser.add_argument(
        "--disk-threshold",
        type=float,
        default=85,
        help="Warn when the data directory exceeds this percent full (default: %(default)s)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=30,
        help="Seconds between collection cycles (default: %(default)s)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.expose_port:
        start_http_server(args.expose_port, registry=REGISTRY)
        logging.getLogger(__name__).info(
            "exposing derived performance gauges on :%d/metrics", args.expose_port
        )

    admin = MoonMQAdminClient(base_url=args.base_url)
    monitor = MoonMQPerformanceMonitor(
        admin=admin,
        data_dir=args.data_dir,
        push_gateway=args.push_gateway,
        cpu_threshold=args.cpu_threshold,
        memory_threshold=args.memory_threshold,
        disk_threshold=args.disk_threshold,
    )

    while True:
        monitor.monitor_once()
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
