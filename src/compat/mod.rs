//! qars2 兼容层（Phase A，任务书 D3/D16）
//!
//! qaexchange 原依赖 qars2（已丢失）；切换到 qars3 谱系（QUANTAXIS/qapro-rs）后，
//! qars2 专有 API 在此以最小实现补齐，不改动 qapro-rs 非协议层。

pub mod account_ext;
pub mod broadcast_hub;

pub use account_ext::AccountQars2Ext;
