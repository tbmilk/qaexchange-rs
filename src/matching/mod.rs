//! 撮合引擎模块
//!
//! 撮合引擎核心来自 qars（qapro-rs::qamarket::matchengine，Phase D3 去重后直接引用），
//! 并提供交易所特定扩展

// 重导出撮合引擎核心类型（直接来自 qars/qapro-rs，Phase D3 去重）
pub use qars::qamarket::matchengine::{
    domain::{Order as MatchOrder, OrderDirection, OrderTrait, OrderType},
    orderbook::{Failed, OrderProcessingResult, Orderbook, Success, TradingState},
    orders::{self, OrderRequest},
};

/// 交易所撮合引擎封装
pub mod engine;

/// 集合竞价增强
pub mod auction;

/// 成交记录器
pub mod trade_recorder;

/// 撮合引擎核心（独立进程版本）
pub mod core;

/// 高性能撮合引擎（Phase 5.2 优化）
pub mod high_perf;

pub use high_perf::{HighPerfMatchingConfig, HighPerfMatchingEngine, MatchingStats};
