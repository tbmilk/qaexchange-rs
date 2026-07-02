//! Prometheus 指标导出模块
//!
//! @yutiansut @quantaxis
//!
//! 提供系统级监控指标，包括：
//! - 订单处理延迟
//! - 因子计算性能
//! - 存储层性能
//! - 网络连接状态

use prometheus::{
    self, Counter, CounterVec, Gauge, GaugeVec, Histogram, HistogramOpts, HistogramVec,
    IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};
use std::sync::Arc;

/// 全局 Prometheus Registry
pub static REGISTRY: std::sync::LazyLock<Registry> = std::sync::LazyLock::new(Registry::new);

// ═══════════════════════════════════════════════════════════════════
// 订单处理指标
// ═══════════════════════════════════════════════════════════════════

/// 订单处理总数
pub static ORDER_TOTAL: std::sync::LazyLock<IntCounterVec> = std::sync::LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("qaexchange_order_total", "Total number of orders processed")
            .namespace("qaexchange"),
        &["direction", "offset", "status"],
    )
    .expect("Failed to create ORDER_TOTAL metric")
});

/// 订单处理延迟 (微秒)
pub static ORDER_LATENCY: std::sync::LazyLock<HistogramVec> = std::sync::LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "qaexchange_order_latency_us",
            "Order processing latency in microseconds",
        )
        .namespace("qaexchange")
        .buckets(vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0]),
        &["operation"],
    )
    .expect("Failed to create ORDER_LATENCY metric")
});

/// 当前挂单数量
pub static PENDING_ORDERS: std::sync::LazyLock<IntGaugeVec> = std::sync::LazyLock::new(|| {
    IntGaugeVec::new(
        Opts::new("qaexchange_pending_orders", "Current number of pending orders")
            .namespace("qaexchange"),
        &["instrument_id"],
    )
    .expect("Failed to create PENDING_ORDERS metric")
});

// ═══════════════════════════════════════════════════════════════════
// 成交指标
// ═══════════════════════════════════════════════════════════════════

/// 成交总数
pub static TRADE_TOTAL: std::sync::LazyLock<IntCounterVec> = std::sync::LazyLock::new(|| {
    IntCounterVec::new(
        Opts::new("qaexchange_trade_total", "Total number of trades executed")
            .namespace("qaexchange"),
        &["instrument_id"],
    )
    .expect("Failed to create TRADE_TOTAL metric")
});

/// 成交金额
pub static TRADE_VOLUME: std::sync::LazyLock<CounterVec> = std::sync::LazyLock::new(|| {
    CounterVec::new(
        Opts::new("qaexchange_trade_volume", "Total trade volume").namespace("qaexchange"),
        &["instrument_id"],
    )
    .expect("Failed to create TRADE_VOLUME metric")
});

// ═══════════════════════════════════════════════════════════════════
// 因子计算指标
// ═══════════════════════════════════════════════════════════════════

/// 因子更新总数
pub static FACTOR_UPDATE_TOTAL: std::sync::LazyLock<IntCounterVec> =
    std::sync::LazyLock::new(|| {
        IntCounterVec::new(
            Opts::new("qaexchange_factor_update_total", "Total number of factor updates")
                .namespace("qaexchange"),
            &["factor_type"],
        )
        .expect("Failed to create FACTOR_UPDATE_TOTAL metric")
    });

/// 因子计算延迟 (微秒)
pub static FACTOR_LATENCY: std::sync::LazyLock<HistogramVec> = std::sync::LazyLock::new(|| {
    HistogramVec::new(
        HistogramOpts::new(
            "qaexchange_factor_latency_us",
            "Factor computation latency in microseconds",
        )
        .namespace("qaexchange")
        .buckets(vec![0.1, 0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0]),
        &["factor_name"],
    )
    .expect("Failed to create FACTOR_LATENCY metric")
});

/// 当前活跃因子数量
pub static ACTIVE_FACTORS: std::sync::LazyLock<IntGauge> = std::sync::LazyLock::new(|| {
    IntGauge::new("qaexchange_active_factors", "Number of active factors")
        .expect("Failed to create ACTIVE_FACTORS metric")
});

/// DAG更新传播延迟 (微秒)
pub static DAG_PROPAGATION_LATENCY: std::sync::LazyLock<Histogram> =
    std::sync::LazyLock::new(|| {
        Histogram::with_opts(
            HistogramOpts::new(
                "qaexchange_dag_propagation_latency_us",
                "DAG update propagation latency",
            )
            .namespace("qaexchange")
            .buckets(vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 500.0]),
        )
        .expect("Failed to create DAG_PROPAGATION_LATENCY metric")
    });

// ═══════════════════════════════════════════════════════════════════
// 存储层指标
// ═══════════════════════════════════════════════════════════════════

/// WAL 写入延迟 (微秒)
pub static WAL_WRITE_LATENCY: std::sync::LazyLock<Histogram> = std::sync::LazyLock::new(|| {
    Histogram::with_opts(
        HistogramOpts::new(
            "qaexchange_wal_write_latency_us",
            "WAL write latency in microseconds",
        )
        .namespace("qaexchange")
        .buckets(vec![10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 5000.0]),
    )
    .expect("Failed to create WAL_WRITE_LATENCY metric")
});

/// WAL 写入字节数
pub static WAL_BYTES_WRITTEN: std::sync::LazyLock<IntCounter> = std::sync::LazyLock::new(|| {
    IntCounter::new("qaexchange_wal_bytes_written", "Total bytes written to WAL")
        .expect("Failed to create WAL_BYTES_WRITTEN metric")
});

/// MemTable 查询延迟 (微秒)
pub static MEMTABLE_QUERY_LATENCY: std::sync::LazyLock<Histogram> =
    std::sync::LazyLock::new(|| {
        Histogram::with_opts(
            HistogramOpts::new("qaexchange_memtable_query_latency_us", "MemTable query latency")
                .namespace("qaexchange")
                .buckets(vec![0.5, 1.0, 2.0, 5.0, 10.0, 25.0, 50.0]),
        )
        .expect("Failed to create MEMTABLE_QUERY_LATENCY metric")
    });

/// SSTable 查询延迟 (微秒)
pub static SSTABLE_QUERY_LATENCY: std::sync::LazyLock<Histogram> =
    std::sync::LazyLock::new(|| {
        Histogram::with_opts(
            HistogramOpts::new("qaexchange_sstable_query_latency_us", "SSTable query latency")
                .namespace("qaexchange")
                .buckets(vec![10.0, 25.0, 50.0, 100.0, 250.0, 500.0]),
        )
        .expect("Failed to create SSTABLE_QUERY_LATENCY metric")
    });

/// Compaction 次数
pub static COMPACTION_TOTAL: std::sync::LazyLock<IntCounterVec> =
    std::sync::LazyLock::new(|| {
        IntCounterVec::new(
            Opts::new("qaexchange_compaction_total", "Total number of compactions")
                .namespace("qaexchange"),
            &["level"],
        )
        .expect("Failed to create COMPACTION_TOTAL metric")
    });

// ═══════════════════════════════════════════════════════════════════
// 网络层指标
// ═══════════════════════════════════════════════════════════════════

/// WebSocket 连接数
pub static WEBSOCKET_CONNECTIONS: std::sync::LazyLock<IntGauge> =
    std::sync::LazyLock::new(|| {
        IntGauge::new(
            "qaexchange_websocket_connections",
            "Current WebSocket connections",
        )
        .expect("Failed to create WEBSOCKET_CONNECTIONS metric")
    });

/// WebSocket 消息吞吐量
pub static WEBSOCKET_MESSAGES: std::sync::LazyLock<IntCounterVec> =
    std::sync::LazyLock::new(|| {
        IntCounterVec::new(
            Opts::new("qaexchange_websocket_messages", "WebSocket messages")
                .namespace("qaexchange"),
            &["direction"], // "inbound" or "outbound"
        )
        .expect("Failed to create WEBSOCKET_MESSAGES metric")
    });

/// 行情延迟 (微秒)
pub static TICK_LATENCY: std::sync::LazyLock<Histogram> = std::sync::LazyLock::new(|| {
    Histogram::with_opts(
        HistogramOpts::new("qaexchange_tick_latency_us", "Tick data latency")
            .namespace("qaexchange")
            .buckets(vec![1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0]),
    )
    .expect("Failed to create TICK_LATENCY metric")
});

// ═══════════════════════════════════════════════════════════════════
// 系统资源指标
// ═══════════════════════════════════════════════════════════════════

/// 内存使用量 (bytes)
pub static MEMORY_USAGE: std::sync::LazyLock<IntGauge> = std::sync::LazyLock::new(|| {
    IntGauge::new("qaexchange_memory_usage_bytes", "Memory usage in bytes")
        .expect("Failed to create MEMORY_USAGE metric")
});

/// 活跃账户数
pub static ACTIVE_ACCOUNTS: std::sync::LazyLock<IntGauge> = std::sync::LazyLock::new(|| {
    IntGauge::new("qaexchange_active_accounts", "Number of active accounts")
        .expect("Failed to create ACTIVE_ACCOUNTS metric")
});

/// 活跃合约数
pub static ACTIVE_INSTRUMENTS: std::sync::LazyLock<IntGauge> = std::sync::LazyLock::new(|| {
    IntGauge::new("qaexchange_active_instruments", "Number of active instruments")
        .expect("Failed to create ACTIVE_INSTRUMENTS metric")
});

// ═══════════════════════════════════════════════════════════════════
// 复制/集群指标
// ═══════════════════════════════════════════════════════════════════

/// 复制延迟 (毫秒)
pub static REPLICATION_LAG: std::sync::LazyLock<GaugeVec> = std::sync::LazyLock::new(|| {
    GaugeVec::new(
        Opts::new("qaexchange_replication_lag_ms", "Replication lag in milliseconds")
            .namespace("qaexchange"),
        &["peer"],
    )
    .expect("Failed to create REPLICATION_LAG metric")
});

/// 节点角色
pub static NODE_ROLE: std::sync::LazyLock<IntGaugeVec> = std::sync::LazyLock::new(|| {
    IntGaugeVec::new(
        Opts::new(
            "qaexchange_node_role",
            "Node role (0=follower, 1=candidate, 2=leader)",
        )
        .namespace("qaexchange"),
        &["node_id"],
    )
    .expect("Failed to create NODE_ROLE metric")
});

/// 初始化所有指标到 Registry
pub fn init_metrics() {
    // 订单指标
    REGISTRY.register(Box::new(ORDER_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(ORDER_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(PENDING_ORDERS.clone())).ok();

    // 成交指标
    REGISTRY.register(Box::new(TRADE_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(TRADE_VOLUME.clone())).ok();

    // 因子指标
    REGISTRY.register(Box::new(FACTOR_UPDATE_TOTAL.clone())).ok();
    REGISTRY.register(Box::new(FACTOR_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(ACTIVE_FACTORS.clone())).ok();
    REGISTRY.register(Box::new(DAG_PROPAGATION_LATENCY.clone())).ok();

    // 存储指标
    REGISTRY.register(Box::new(WAL_WRITE_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(WAL_BYTES_WRITTEN.clone())).ok();
    REGISTRY.register(Box::new(MEMTABLE_QUERY_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(SSTABLE_QUERY_LATENCY.clone())).ok();
    REGISTRY.register(Box::new(COMPACTION_TOTAL.clone())).ok();

    // 网络指标
    REGISTRY.register(Box::new(WEBSOCKET_CONNECTIONS.clone())).ok();
    REGISTRY.register(Box::new(WEBSOCKET_MESSAGES.clone())).ok();
    REGISTRY.register(Box::new(TICK_LATENCY.clone())).ok();

    // 系统指标
    REGISTRY.register(Box::new(MEMORY_USAGE.clone())).ok();
    REGISTRY.register(Box::new(ACTIVE_ACCOUNTS.clone())).ok();
    REGISTRY.register(Box::new(ACTIVE_INSTRUMENTS.clone())).ok();

    // 集群指标
    REGISTRY.register(Box::new(REPLICATION_LAG.clone())).ok();
    REGISTRY.register(Box::new(NODE_ROLE.clone())).ok();

    log::info!("Prometheus metrics initialized");
}

/// 导出指标为 Prometheus 格式
pub fn export_metrics() -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer).unwrap();
    String::from_utf8(buffer).unwrap()
}

/// 计时器辅助结构
pub struct Timer {
    start: std::time::Instant,
    histogram: Histogram,
}

impl Timer {
    pub fn new(histogram: Histogram) -> Self {
        Self {
            start: std::time::Instant::now(),
            histogram,
        }
    }

    pub fn observe(self) {
        let elapsed = self.start.elapsed().as_micros() as f64;
        self.histogram.observe(elapsed);
    }
}

/// 带标签的计时器
pub struct LabeledTimer {
    start: std::time::Instant,
    histogram: HistogramVec,
    labels: Vec<String>,
}

impl LabeledTimer {
    pub fn new(histogram: HistogramVec, labels: Vec<String>) -> Self {
        Self {
            start: std::time::Instant::now(),
            histogram,
            labels,
        }
    }

    pub fn observe(self) {
        let elapsed = self.start.elapsed().as_micros() as f64;
        let label_refs: Vec<&str> = self.labels.iter().map(|s| s.as_str()).collect();
        self.histogram.with_label_values(&label_refs).observe(elapsed);
    }
}

// ═══════════════════════════════════════════════════════════════════════
// 便捷宏
// ═══════════════════════════════════════════════════════════════════════

/// 记录订单处理
#[macro_export]
macro_rules! record_order {
    ($direction:expr, $offset:expr, $status:expr) => {
        $crate::observability::ORDER_TOTAL
            .with_label_values(&[$direction, $offset, $status])
            .inc();
    };
}

/// 记录成交
#[macro_export]
macro_rules! record_trade {
    ($instrument_id:expr, $volume:expr) => {
        $crate::observability::TRADE_TOTAL
            .with_label_values(&[$instrument_id])
            .inc();
        $crate::observability::TRADE_VOLUME
            .with_label_values(&[$instrument_id])
            .inc_by($volume as f64);
    };
}

/// 记录因子更新
#[macro_export]
macro_rules! record_factor_update {
    ($factor_type:expr) => {
        $crate::observability::FACTOR_UPDATE_TOTAL
            .with_label_values(&[$factor_type])
            .inc();
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_init() {
        init_metrics();

        // 记录一些指标
        ORDER_TOTAL.with_label_values(&["BUY", "OPEN", "FILLED"]).inc();
        ORDER_LATENCY.with_label_values(&["submit"]).observe(50.0);

        TRADE_TOTAL.with_label_values(&["cu2501"]).inc();
        TRADE_VOLUME.with_label_values(&["cu2501"]).inc_by(100.0);

        FACTOR_UPDATE_TOTAL.with_label_values(&["rolling"]).inc();
        FACTOR_LATENCY.with_label_values(&["ma_20"]).observe(5.0);

        WAL_WRITE_LATENCY.observe(25.0);
        WAL_BYTES_WRITTEN.inc_by(1024);

        WEBSOCKET_CONNECTIONS.set(10);
        ACTIVE_ACCOUNTS.set(100);

        // 导出并验证
        let output = export_metrics();
        assert!(output.contains("qaexchange_order_total"));
        assert!(output.contains("qaexchange_trade_total"));
        assert!(output.contains("qaexchange_factor_update_total"));
    }

    #[test]
    fn test_timer() {
        init_metrics();

        let timer = Timer::new(WAL_WRITE_LATENCY.clone());
        std::thread::sleep(std::time::Duration::from_micros(100));
        timer.observe();
    }
}
