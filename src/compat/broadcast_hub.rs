//! qars2 `qadata::broadcast_hub` 最小兼容实现
//!
//! 三个源（qa-rs / 当前 qapro-rs / 归档）均无此模块，确认为 qars2 特有（任务书 §0b）。
//! 库内无调用点，仅 lib.rs 保留公开 re-export；按 tokio broadcast channel 封装还原。

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketDataType {
    Tick,
    Kline,
    Depth,
    Snapshot,
    Trade,
}

#[derive(Debug, Clone)]
pub struct BroadcastConfig {
    pub capacity: usize,
}

impl Default for BroadcastConfig {
    fn default() -> Self {
        Self { capacity: 1024 }
    }
}

/// 市场数据广播器：单生产者多消费者
pub struct DataBroadcaster {
    sender: broadcast::Sender<(MarketDataType, Vec<u8>)>,
}

impl DataBroadcaster {
    pub fn new(config: BroadcastConfig) -> Self {
        let (sender, _) = broadcast::channel(config.capacity);
        Self { sender }
    }

    pub fn broadcast(&self, data_type: MarketDataType, payload: Vec<u8>) -> usize {
        self.sender.send((data_type, payload)).unwrap_or(0)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<(MarketDataType, Vec<u8>)> {
        self.sender.subscribe()
    }

    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}
