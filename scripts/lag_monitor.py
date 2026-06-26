"""MoonMQ lag monitor.

MoonMQ does NOT expose a Kafka-Manager-style REST API (no /api/consumer-groups,
no per-group committed offsets). Its only HTTP surface is the metrics server in
``src/server/metrics_http.lua`` — by default on ``127.0.0.1:9090`` — which serves:

    GET /metrics   Prometheus text exposition
    GET /stats     JSON broker snapshot
    GET /health    liveness probe

The lag-relevant series MoonMQ emits today are per-topic counters:

    moonmq_produce_records_total{topic="..."}      records accepted by the broker
    moonmq_fetch_records_total{topic="..."}        records delivered to consumers
    moonmq_partition_log_bytes{topic,partition}    partition log-end position (bytes)

MoonMQ keeps consumer-group offsets in-process (see src/consumer.lua) and does not
surface them over HTTP, so a true per-group committed-offset lag is not available.
This monitor therefore computes the broker-side *backlog*:

    backlog(topic) = produce_records_total - fetch_records_total

i.e. records produced but not yet delivered to any consumer, plus a time-based
lag estimate derived from the delivery rate observed between scrapes.

The derived gauges are re-exported either by pushing to a Prometheus Pushgateway
(--push-gateway) or by exposing a local scrape endpoint (--expose-port).
"""

import argparse
import logging
import time

import requests
from prometheus_client import CollectorRegistry, Gauge, push_to_gateway, start_http_server
from prometheus_client.parser import text_string_to_metric_families

# Own registry so we control exactly what gets pushed/exposed (and don't drag in
# the default process/GC collectors when pushing to a gateway).
REGISTRY = CollectorRegistry()

# Derived lag gauges. MoonMQ does not break delivery down by partition or
# consumer group over HTTP, so these are labelled by topic only — claiming a
# partition/consumer_group dimension we can't actually observe would be a lie.
consumer_lag_messages = Gauge(
    "moonmq_consumer_lag_messages",
    "Broker backlog: records produced but not yet delivered to a consumer",
    ["topic"],
    registry=REGISTRY,
)

consumer_lag_seconds = Gauge(
    "moonmq_consumer_lag_seconds",
    "Estimated time to drain the backlog at the recently observed delivery rate",
    ["topic"],
    registry=REGISTRY,
)

delivery_rate_gauge = Gauge(
    "moonmq_delivery_records_per_second",
    "Records delivered per second, measured between the last two scrapes",
    ["topic"],
    registry=REGISTRY,
)


class MoonMQLagMonitor:
    def __init__(
        self,
        metrics_url,
        push_gateway=None,
        alertmanager_url=None,
        backlog_threshold=100_000,
        time_threshold=300,
        request_timeout=5,
    ):
        self.metrics_url = metrics_url
        self.push_gateway = push_gateway
        self.alertmanager_url = alertmanager_url
        self.backlog_threshold = backlog_threshold
        self.time_threshold = time_threshold
        self.request_timeout = request_timeout
        self.logger = logging.getLogger(__name__)

        # Counters are monotonic; to get a rate we remember the previous scrape.
        self._prev_fetched = {}   # topic -> fetch_records_total
        self._prev_time = None    # time.time() of the previous scrape

    def scrape_counts(self):
        """Scrape /metrics and return per-topic {produced, fetched} totals.

        Returns a dict: topic -> {"produced": float, "fetched": float}.
        """
        response = requests.get(self.metrics_url, timeout=self.request_timeout)
        response.raise_for_status()

        topics = {}

        def bucket(topic):
            return topics.setdefault(topic, {"produced": 0.0, "fetched": 0.0})

        for family in text_string_to_metric_families(response.text):
            if family.name == "moonmq_produce_records_total":
                for sample in family.samples:
                    topic = sample.labels.get("topic")
                    if topic is not None:
                        bucket(topic)["produced"] = sample.value
            elif family.name == "moonmq_fetch_records_total":
                for sample in family.samples:
                    topic = sample.labels.get("topic")
                    if topic is not None:
                        bucket(topic)["fetched"] = sample.value

        return topics

    @staticmethod
    def estimate_time_lag(backlog, records_per_second):
        """Seconds to drain `backlog` at the given delivery rate.

        Returns 0 when there's nothing to drain or no rate to project from.
        """
        if backlog <= 0 or records_per_second <= 0:
            return 0.0
        return backlog / records_per_second

    def monitor_once(self):
        """Run a single scrape/compute/export cycle."""
        try:
            counts = self.scrape_counts()
        except requests.RequestException as exc:
            self.logger.error("failed to scrape %s: %s", self.metrics_url, exc)
            return

        now = time.time()
        dt = (now - self._prev_time) if self._prev_time is not None else None

        for topic, totals in counts.items():
            produced = totals["produced"]
            fetched = totals["fetched"]

            # Backlog can't be negative; a consumer can't fetch more than was
            # produced. Counter resets (broker restart) are clamped to 0 here.
            backlog = max(0.0, produced - fetched)

            rate = 0.0
            prev_fetched = self._prev_fetched.get(topic)
            if dt and dt > 0 and prev_fetched is not None:
                rate = max(0.0, (fetched - prev_fetched) / dt)

            time_lag = self.estimate_time_lag(backlog, rate)

            consumer_lag_messages.labels(topic=topic).set(backlog)
            delivery_rate_gauge.labels(topic=topic).set(rate)
            consumer_lag_seconds.labels(topic=topic).set(time_lag)

            if backlog > self.backlog_threshold or time_lag > self.time_threshold:
                self.send_alert(topic, backlog, time_lag)

            self._prev_fetched[topic] = fetched

        self._prev_time = now

        if self.push_gateway:
            try:
                push_to_gateway(
                    self.push_gateway, job="moonmq_lag_monitor", registry=REGISTRY
                )
            except (requests.RequestException, OSError) as exc:
                self.logger.error("failed to push to %s: %s", self.push_gateway, exc)

    def send_alert(self, topic, backlog, time_lag):
        """Log a high-backlog alert and, if configured, POST it to Alertmanager."""
        severity = "critical" if backlog > 5 * self.backlog_threshold else "warning"
        description = (
            f"Topic {topic} has {int(backlog)} undelivered records "
            f"(~{time_lag:.1f}s to drain at the current rate)"
        )
        self.logger.warning("[%s] %s", severity, description)

        if not self.alertmanager_url:
            return

        alert = {
            "labels": {
                "alertname": "MoonMQConsumerLag",
                "severity": severity,
                "topic": topic,
            },
            "annotations": {
                "summary": f"High MoonMQ backlog on {topic}",
                "description": description,
            },
        }
        try:
            requests.post(
                self.alertmanager_url, json=[alert], timeout=self.request_timeout
            )
        except requests.RequestException as exc:
            self.logger.error("failed to send alert to %s: %s", self.alertmanager_url, exc)


def main():
    parser = argparse.ArgumentParser(description="MoonMQ consumer-lag monitor")
    parser.add_argument(
        "--metrics-url",
        default="http://127.0.0.1:9090/metrics",
        help="MoonMQ /metrics endpoint (default: %(default)s)",
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
        "--alertmanager-url",
        default=None,
        help="Alertmanager API URL for POSTing alerts, e.g. "
        "http://alertmanager:9093/api/v1/alerts (optional)",
    )
    parser.add_argument(
        "--backlog-threshold",
        type=int,
        default=100_000,
        help="Alert when undelivered records exceed this (default: %(default)s)",
    )
    parser.add_argument(
        "--time-threshold",
        type=float,
        default=300,
        help="Alert when estimated drain time (s) exceeds this (default: %(default)s)",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=30,
        help="Seconds between scrapes (default: %(default)s)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if args.expose_port:
        start_http_server(args.expose_port, registry=REGISTRY)
        logging.getLogger(__name__).info(
            "exposing derived lag gauges on :%d/metrics", args.expose_port
        )

    monitor = MoonMQLagMonitor(
        metrics_url=args.metrics_url,
        push_gateway=args.push_gateway,
        alertmanager_url=args.alertmanager_url,
        backlog_threshold=args.backlog_threshold,
        time_threshold=args.time_threshold,
    )

    while True:
        monitor.monitor_once()
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
