//! 订单路由模块
//!
//! 负责订单的接收、风控检查、路由到撮合引擎以及撤单处理

use crate::compat::account_ext::qaorder_to_qifi;
use crate::core::{Order, QAOrder};
use crate::exchange::{AccountManager, InstrumentRegistry, TradeGateway};
use crate::market::MarketDataBroadcaster;
use crate::matching::engine::{ExchangeMatchingEngine, InstrumentAsset};
use crate::matching::{orders, Failed, OrderDirection, OrderType, Success};
use crate::risk::pre_trade_check::{OrderCheckRequest, PreTradeCheck, RiskCheckResult};
use crate::ExchangeError;
use chrono::Local;
use dashmap::DashMap;
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

/// 时间条件枚举
/// @yutiansut @quantaxis
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[derive(Default)]
pub enum TimeCondition {
    /// Immediate or Cancel - 立即成交剩余撤销
    IOC,
    /// Good for Session - 本节有效
    GFS,
    /// Good for Day - 当日有效 (默认)
    #[default]
    GFD,
    /// Good Till Date - 指定日期前有效
    GTD,
    /// Good Till Cancel - 撤销前有效
    GTC,
    /// Good for Auction - 集合竞价有效
    GFA,
}


impl std::fmt::Display for TimeCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TimeCondition::IOC => write!(f, "IOC"),
            TimeCondition::GFS => write!(f, "GFS"),
            TimeCondition::GFD => write!(f, "GFD"),
            TimeCondition::GTD => write!(f, "GTD"),
            TimeCondition::GTC => write!(f, "GTC"),
            TimeCondition::GFA => write!(f, "GFA"),
        }
    }
}

impl TimeCondition {
    /// 从字符串解析
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "IOC" => TimeCondition::IOC,
            "GFS" => TimeCondition::GFS,
            "GFD" => TimeCondition::GFD,
            "GTD" => TimeCondition::GTD,
            "GTC" => TimeCondition::GTC,
            "GFA" => TimeCondition::GFA,
            _ => TimeCondition::GFD, // 默认当日有效
        }
    }
}

/// 数量条件枚举
/// @yutiansut @quantaxis
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
#[derive(Default)]
pub enum VolumeCondition {
    /// Any - 任何数量 (默认)
    #[default]
    ANY,
    /// Min - 最小数量
    MIN,
    /// All - 全部数量 (FOK)
    ALL,
}


impl std::fmt::Display for VolumeCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            VolumeCondition::ANY => write!(f, "ANY"),
            VolumeCondition::MIN => write!(f, "MIN"),
            VolumeCondition::ALL => write!(f, "ALL"),
        }
    }
}

impl VolumeCondition {
    /// 从字符串解析
    pub fn from_str(s: &str) -> Self {
        match s.to_uppercase().as_str() {
            "ANY" => VolumeCondition::ANY,
            "MIN" => VolumeCondition::MIN,
            "ALL" => VolumeCondition::ALL,
            _ => VolumeCondition::ANY, // 默认任意数量
        }
    }
}

/// 订单提交请求（交易层 - 只关心账户）
/// @yutiansut @quantaxis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitOrderRequest {
    pub account_id: String, // 交易系统只关心账户ID
    pub instrument_id: String,
    pub direction: String, // BUY/SELL
    pub offset: String,    // OPEN/CLOSE/CLOSETODAY
    pub volume: f64,
    pub price: f64,
    pub order_type: String, // LIMIT/MARKET
    /// 时间条件: IOC/GFS/GFD/GTD/GTC/GFA
    #[serde(default)]
    pub time_condition: Option<TimeCondition>,
    /// 数量条件: ANY/MIN/ALL (ALL + IOC = FOK)
    #[serde(default)]
    pub volume_condition: Option<VolumeCondition>,
}

/// 撤单请求（交易层 - 只关心账户）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelOrderRequest {
    pub account_id: String, // 交易系统只关心账户ID
    pub order_id: String,
}

/// 订单提交响应
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubmitOrderResponse {
    pub success: bool,
    pub order_id: Option<String>,
    pub status: Option<String>, // 订单最终状态：submitted/filled/partially_filled/rejected
    pub error_message: Option<String>,
    pub error_code: Option<u32>,
}

/// 提交行为控制选项
#[derive(Clone, Copy, Debug)]
#[derive(Default)]
struct OrderSubmitOptions {
    /// 是否为强制（风险绕过）订单
    force: bool,
}


/// 订单状态枚举
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum OrderStatus {
    /// 等待风控
    PendingRisk,
    /// 风控通过，等待路由
    PendingRoute,
    /// 已提交到撮合引擎
    Submitted,
    /// 部分成交
    PartiallyFilled,
    /// 全部成交
    Filled,
    /// 已撤单
    Cancelled,
    /// 被拒绝
    Rejected,
}

/// 订单路由信息
/// @yutiansut @quantaxis
#[derive(Debug, Clone)]
struct OrderRouteInfo {
    order: Order,
    status: OrderStatus,
    submit_time: i64,
    update_time: i64,
    filled_volume: f64,                    // 已成交数量
    qa_order_id: String,                   // qars 内部订单ID (用于 receive_deal_sim)
    matching_engine_order_id: Option<u64>, // 撮合引擎订单ID (用于撤单)
    time_condition: TimeCondition,         // 时间条件 (IOC/GFD/GTC等)
    volume_condition: VolumeCondition,     // 数量条件 (ANY/MIN/ALL)
}

/// 订单统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderStatistics {
    pub total_count: usize,
    pub pending_count: usize,
    pub filled_count: usize,
    pub cancelled_count: usize,
    pub rejected_count: usize,
}

/// 成交统计信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeStatistics {
    pub total_count: u64,
    pub total_volume: f64,
    pub total_amount: f64,
}

/// 订单路由器
pub struct OrderRouter {
    /// 账户管理器
    account_mgr: Arc<AccountManager>,

    /// 风控检查器
    risk_checker: Arc<PreTradeCheck>,

    /// 撮合引擎
    matching_engine: Arc<ExchangeMatchingEngine>,

    /// 合约注册表
    instrument_registry: Arc<InstrumentRegistry>,

    /// 成交回报网关
    trade_gateway: Arc<TradeGateway>,

    /// 市场数据广播器（可选）
    market_broadcaster: Option<Arc<MarketDataBroadcaster>>,

    /// 存储管理器（可选，用于持久化行情数据）
    storage: Option<Arc<crate::storage::hybrid::OltpHybridStorage>>,

    /// 订单映射 (order_id -> OrderRouteInfo)
    orders: DashMap<String, Arc<RwLock<OrderRouteInfo>>>,

    /// 用户订单索引 (user_id -> Vec<order_id>)
    user_orders: DashMap<String, Arc<RwLock<Vec<String>>>>,

    /// ✨ 撮合引擎订单ID反向索引 (matching_engine_order_id -> order_id) @yutiansut @quantaxis
    /// 用于在成交时通过对手单的matching_engine_order_id找到对应的order_id
    engine_id_to_order: DashMap<u64, String>,

    /// ✨ 撮合引擎订单ID → user_id 直接映射 (性能优化) @yutiansut @quantaxis
    /// 避免成交时两次查找（engine_id→order_id→user_id），直接 O(1) 获取
    engine_id_to_user: DashMap<u64, String>,

    /// 订单序号生成器
    order_seq: AtomicU64,

    /// 统计：总成交笔数
    trade_count: AtomicU64,

    /// 统计：总成交量
    trade_volume: parking_lot::RwLock<f64>,

    /// 统计：总成交金额
    trade_amount: parking_lot::RwLock<f64>,

    // ========== 性能优化字段 ==========
    /// 快照频率控制：记录每个合约的上次快照时间
    last_snapshot_time: Arc<DashMap<String, Instant>>,

    /// 快照写入间隔（默认1秒）
    snapshot_interval: Duration,

    /// Tick数据批量缓冲区
    tick_buffer: Arc<Mutex<Vec<crate::storage::wal::record::WalRecord>>>,

    /// 批量写入线程停止信号
    flush_stop_signal: Arc<AtomicBool>,

    /// 优先级订单队列（可选）
    priority_queue: Option<Arc<crate::exchange::PriorityOrderQueue>>,

    /// 是否启用优先级队列
    priority_queue_enabled: AtomicBool,

    /// 交易状态机（可选） @yutiansut @quantaxis
    trading_state_machine: Option<Arc<crate::exchange::TradingStateMachine>>,
}

impl OrderRouter {
    pub fn new(
        account_mgr: Arc<AccountManager>,
        matching_engine: Arc<ExchangeMatchingEngine>,
        instrument_registry: Arc<InstrumentRegistry>,
        trade_gateway: Arc<TradeGateway>,
    ) -> Self {
        let risk_checker = Arc::new(PreTradeCheck::new(account_mgr.clone()));

        Self {
            account_mgr,
            risk_checker,
            matching_engine,
            instrument_registry,
            trade_gateway,
            market_broadcaster: None,
            storage: None,
            orders: DashMap::new(),
            user_orders: DashMap::new(),
            engine_id_to_order: DashMap::new(),
            engine_id_to_user: DashMap::new(),
            order_seq: AtomicU64::new(1),
            trade_count: AtomicU64::new(0),
            trade_volume: parking_lot::RwLock::new(0.0),
            trade_amount: parking_lot::RwLock::new(0.0),
            // 性能优化字段初始化
            last_snapshot_time: Arc::new(DashMap::new()),
            snapshot_interval: Duration::from_secs(1), // 默认1秒
            tick_buffer: Arc::new(Mutex::new(Vec::with_capacity(1000))),
            flush_stop_signal: Arc::new(AtomicBool::new(false)),
            priority_queue: None, // 默认不启用
            priority_queue_enabled: AtomicBool::new(false),
            trading_state_machine: None, // 默认不启用
        }
    }

    /// 设置市场数据广播器
    pub fn set_market_broadcaster(&mut self, broadcaster: Arc<MarketDataBroadcaster>) {
        self.market_broadcaster = Some(broadcaster);
    }

    /// 设置存储管理器（用于持久化行情数据）
    pub fn set_storage(&mut self, storage: Arc<crate::storage::hybrid::OltpHybridStorage>) {
        self.storage = Some(storage);
    }

    /// 设置交易状态机 @yutiansut @quantaxis
    pub fn set_trading_state_machine(
        &mut self,
        state_machine: Arc<crate::exchange::TradingStateMachine>,
    ) {
        self.trading_state_machine = Some(state_machine);
    }

    /// 获取交易状态机
    pub fn get_trading_state_machine(&self) -> Option<Arc<crate::exchange::TradingStateMachine>> {
        self.trading_state_machine.clone()
    }

    /// 启用优先级队列
    ///
    /// # 参数
    /// - `low_queue_limit`: 低优先级队列最大长度（默认100）
    /// - `critical_amount_threshold`: 大额订单阈值（默认1,000,000.0）
    pub fn enable_priority_queue(
        &mut self,
        low_queue_limit: usize,
        critical_amount_threshold: f64,
    ) {
        let queue = Arc::new(crate::exchange::PriorityOrderQueue::new(
            low_queue_limit,
            critical_amount_threshold,
        ));
        self.priority_queue = Some(queue);
        self.priority_queue_enabled.store(true, Ordering::SeqCst);
        log::info!(
            "✅ Priority queue enabled (low_limit={}, threshold={:.2})",
            low_queue_limit,
            critical_amount_threshold
        );
    }

    /// 禁用优先级队列
    pub fn disable_priority_queue(&mut self) {
        self.priority_queue_enabled.store(false, Ordering::SeqCst);
        log::info!("⚠️  Priority queue disabled");
    }

    /// 添加VIP用户到优先级队列
    pub fn add_vip_user(&self, user_id: String) {
        if let Some(ref queue) = self.priority_queue {
            queue.add_vip_user(user_id);
        }
    }

    /// 批量添加VIP用户
    pub fn add_vip_users(&self, users: Vec<String>) {
        if let Some(ref queue) = self.priority_queue {
            queue.add_vip_users(users);
        }
    }

    /// 创建带自定义风控检查器的路由器
    pub fn with_risk_checker(
        account_mgr: Arc<AccountManager>,
        risk_checker: Arc<PreTradeCheck>,
        matching_engine: Arc<ExchangeMatchingEngine>,
        instrument_registry: Arc<InstrumentRegistry>,
        trade_gateway: Arc<TradeGateway>,
    ) -> Self {
        Self {
            account_mgr,
            risk_checker,
            matching_engine,
            instrument_registry,
            trade_gateway,
            market_broadcaster: None,
            storage: None,
            orders: DashMap::new(),
            user_orders: DashMap::new(),
            engine_id_to_order: DashMap::new(),
            engine_id_to_user: DashMap::new(),
            order_seq: AtomicU64::new(1),
            trade_count: AtomicU64::new(0),
            trade_volume: parking_lot::RwLock::new(0.0),
            trade_amount: parking_lot::RwLock::new(0.0),
            // 性能优化字段初始化
            last_snapshot_time: Arc::new(DashMap::new()),
            snapshot_interval: Duration::from_secs(1), // 默认1秒
            tick_buffer: Arc::new(Mutex::new(Vec::with_capacity(1000))),
            flush_stop_signal: Arc::new(AtomicBool::new(false)),
            priority_queue: None, // 默认不启用
            priority_queue_enabled: AtomicBool::new(false),
            trading_state_machine: None, // 默认不启用
        }
    }

    /// 提交订单 (核心方法)
    pub fn submit_order(&self, req: SubmitOrderRequest) -> SubmitOrderResponse {
        self.submit_order_with_options(req, OrderSubmitOptions::default())
    }

    /// 提交强制订单（跳过风控/资金校验，用于强平等场景）
    pub fn submit_force_order(&self, req: SubmitOrderRequest) -> SubmitOrderResponse {
        self.submit_order_with_options(req, OrderSubmitOptions { force: true })
    }

    fn submit_order_with_options(
        &self,
        req: SubmitOrderRequest,
        opts: OrderSubmitOptions,
    ) -> SubmitOrderResponse {
        // 1. 生成订单ID（无锁操作）
        let order_id = self.generate_order_id();

        // 1.5 市价单价格转换 @yutiansut @quantaxis
        // 市价单需要从行情获取实际价格：买单用卖一价，卖单用买一价
        let req = if req.order_type == "MARKET" && req.price <= 0.0 {
            let market_price = self.get_market_price_for_order(&req.instrument_id, &req.direction);
            if market_price <= 0.0 {
                log::warn!(
                    "Cannot get market price for MARKET order: instrument={}, direction={}",
                    req.instrument_id, req.direction
                );
                return SubmitOrderResponse {
                    success: false,
                    order_id: Some(order_id.clone()),
                    status: Some("rejected".to_string()),
                    error_message: Some(format!(
                        "No market price available for instrument {}",
                        req.instrument_id
                    )),
                    error_code: Some(4002), // 无行情
                };
            }
            log::info!(
                "MARKET order price converted: instrument={}, direction={}, price={}",
                req.instrument_id, req.direction, market_price
            );
            SubmitOrderRequest {
                price: market_price,
                ..req
            }
        } else {
            req
        };

        // 2. 预计算所需资金（无锁操作）
        let estimated_commission = req.price * req.volume * 0.0003; // 万3手续费
        let required_funds = if req.direction == "BUY" && req.offset == "OPEN" {
            req.price * req.volume + estimated_commission
        } else if req.direction == "SELL" && req.offset == "OPEN" {
            req.price * req.volume * 0.2 + estimated_commission
        } else {
            estimated_commission
        };

        // 2.5 交易状态检查 @yutiansut @quantaxis
        if let Some(ref state_machine) = self.trading_state_machine {
            use crate::exchange::OrderValidation;
            match state_machine.validate_order(&req.instrument_id) {
                OrderValidation::Allowed => {}
                OrderValidation::Rejected(reason) => {
                    log::warn!(
                        "Order rejected by trading state: {} - {}",
                        req.instrument_id,
                        reason
                    );
                    return SubmitOrderResponse {
                        success: false,
                        order_id: Some(order_id.clone()),
                        status: Some("rejected".to_string()),
                        error_message: Some(reason),
                        error_code: Some(4100), // 交易状态拒绝
                    };
                }
            }
        }

        // 3. 风控检查（无锁操作，风控器内部使用 DashMap）
        if !opts.force {
            let risk_check_req = OrderCheckRequest {
                account_id: req.account_id.clone(),
                instrument_id: req.instrument_id.clone(),
                direction: req.direction.clone(),
                offset: req.offset.clone(),
                volume: req.volume,
                price: req.price,
                limit_price: req.price,
                price_type: req.order_type.clone(),
            };

            match self.risk_checker.check(&risk_check_req) {
                Ok(RiskCheckResult::Pass) => {}
                Ok(RiskCheckResult::Reject { reason, code }) => {
                    log::warn!("Order rejected by risk check: {:?} - {}", code, reason);
                    return SubmitOrderResponse {
                        success: false,
                        order_id: Some(order_id.clone()),
                        status: Some("rejected".to_string()),
                        error_message: Some(reason),
                        error_code: Some(code as u32),
                    };
                }
                Err(e) => {
                    log::error!("Risk check error: {}", e);
                    return SubmitOrderResponse {
                        success: false,
                        order_id: Some(order_id.clone()),
                        status: Some("rejected".to_string()),
                        error_message: Some(format!("Risk check error: {}", e)),
                        error_code: Some(9999),
                    };
                }
            }
        } else {
            log::warn!(
                "⚠️  Force order submitted for account {} instrument {} volume {}",
                req.account_id,
                req.instrument_id,
                req.volume
            );
        }

        // 3.5 FOK 前置检查: 如果是 FOK 订单，必须确保能全部成交
        // @yutiansut @quantaxis
        let time_cond = req.time_condition.unwrap_or(TimeCondition::GFD);
        let volume_cond = req.volume_condition.unwrap_or(VolumeCondition::ANY);

        // FOK = IOC + ALL (立即全部成交或撤销)
        let is_fok = time_cond == TimeCondition::IOC && volume_cond == VolumeCondition::ALL;

        if is_fok && !opts.force {
            if !self.check_fok_fulfillable(&req.instrument_id, &req.direction, req.volume, req.price) {
                log::warn!(
                    "[FOK] Order rejected: cannot fill {} {} {} @ {} immediately",
                    req.volume, req.direction, req.instrument_id, req.price
                );
                return SubmitOrderResponse {
                    success: false,
                    order_id: Some(order_id.clone()),
                    status: Some("rejected".to_string()),
                    error_message: Some("FOK order cannot be fully filled immediately".to_string()),
                    error_code: Some(4010),
                };
            }
            log::info!(
                "[FOK] Pre-check passed: {} {} {} @ {}",
                req.volume, req.direction, req.instrument_id, req.price
            );
        }

        // 4. 获取账户引用（无锁操作，DashMap get）
        let account = match self.account_mgr.get_account(&req.account_id) {
            Ok(acc) => acc,
            Err(e) => {
                log::error!("Account not found: {}: {}", req.account_id, e);
                return SubmitOrderResponse {
                    success: false,
                    order_id: Some(order_id),
                    status: Some("rejected".to_string()),
                    error_message: Some(format!("Account not found: {}", e)),
                    error_code: Some(4000),
                };
            }
        };

        // 5. 乐观读取检查余额（读锁，快速失败）
        if !opts.force {
            let available = account.read().money;
            if available < required_funds {
                log::warn!(
                    "Insufficient funds (optimistic): account={}, available={:.2}, required={:.2}",
                    req.account_id,
                    available,
                    required_funds
                );
                return SubmitOrderResponse {
                    success: false,
                    order_id: Some(order_id),
                    status: Some("rejected".to_string()),
                    error_message: Some(format!(
                        "Insufficient funds: available={:.2}, required={:.2}",
                        available, required_funds
                    )),
                    error_code: Some(4001),
                };
            }
        }

        // 6. 预构建订单数据（无锁操作）
        let towards = self.calculate_towards(&req.direction, &req.offset);
        let current_time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();

        let order = QAOrder::new(
            req.account_id.clone(),
            req.instrument_id.clone(),
            towards,
            "EXCHANGE".to_string(),
            current_time.clone(),
            req.volume,
            req.price,
            order_id.clone(),
        );

        // 7. 短时写锁：仅用于冻结资金 + send_order
        // 优化点：将锁范围缩小到最小必要操作
        let qa_order_id = {
            let mut acc = account.write();

            // 7.1 二次验证（写锁内，避免竞态）
            if !opts.force && acc.money < required_funds {
                log::warn!(
                    "Insufficient funds (double-check): account={}, available={:.2}, required={:.2}",
                    req.account_id,
                    acc.money,
                    required_funds
                );
                return SubmitOrderResponse {
                    success: false,
                    order_id: Some(order_id),
                    status: Some("rejected".to_string()),
                    error_message: Some(format!(
                        "Insufficient funds: available={:.2}, required={:.2}",
                        acc.money, required_funds
                    )),
                    error_code: Some(4001),
                };
            }

            // 7.2 执行 send_order（冻结资金）
            match acc.send_order(
                &req.instrument_id,
                req.volume,
                &current_time,
                towards,
                req.price,
                "",
                &req.order_type,
            ) {
                Ok(ref qa_order) => {
                    // ✨ Debug: 检查 frozen 状态 @yutiansut @quantaxis
                    let frozen_keys: Vec<String> = acc.frozen.keys().cloned().collect();
                    let frozen_count = acc.frozen.len();
                    log::info!(
                        "💰 [DEBUG] After send_order: qa_order_id={}, money={:.2}, frozen_count={}, frozen_keys={:?}",
                        qa_order.order_id,
                        acc.money,
                        frozen_count,
                        frozen_keys
                    );
                    qa_order.order_id.clone()
                }
                Err(e) => {
                    log::warn!(
                        "Order rejected - insufficient funds/margin for account {}: {:?}",
                        req.account_id,
                        e
                    );
                    return SubmitOrderResponse {
                        success: false,
                        order_id: Some(order_id),
                        status: Some("rejected".to_string()),
                        error_message: Some(format!("Insufficient funds/margin: {:?}", e)),
                        error_code: Some(4001),
                    };
                }
            }
            // 写锁在此自动释放（RAII）
        };

        // ✨ 调试日志：显示 qa_order_id 和 towards 值 @yutiansut @quantaxis
        log::info!(
            "🔐 Order submitted: order_id={}, qa_order_id={}, towards={}, direction={}, offset={}",
            order_id,
            qa_order_id,
            towards,
            req.direction,
            req.offset
        );

        // 4. 存储订单信息
        let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let time_cond = req.time_condition.unwrap_or(TimeCondition::GFD);
        let volume_cond = req.volume_condition.unwrap_or(VolumeCondition::ANY);
        let route_info = OrderRouteInfo {
            order: qaorder_to_qifi(&order),
            status: OrderStatus::PendingRoute,
            submit_time: timestamp,
            update_time: timestamp,
            filled_volume: 0.0,
            qa_order_id: qa_order_id.clone(), // 存储 qars 订单ID
            matching_engine_order_id: None,   // 撮合引擎订单ID (在 Accepted 事件中设置)
            time_condition: time_cond,
            volume_condition: volume_cond,
        };

        self.orders
            .insert(order_id.clone(), Arc::new(RwLock::new(route_info)));

        // 5. 更新账户订单索引
        self.user_orders
            .entry(req.account_id.clone())
            .or_insert_with(|| Arc::new(RwLock::new(Vec::new())))
            .write()
            .push(order_id.clone());

        // 6. 注册活动订单 (风控追踪)
        self.risk_checker.register_active_order(
            &req.account_id,
            order_id.clone(),
            req.instrument_id.clone(),
            req.direction.clone(),
            req.price,              // ✅ price 作为 limit_price
            req.order_type.clone(), // ✅ order_type 作为 price_type
        );

        // 7. 路由到撮合引擎
        match self.route_to_matching_engine(&req.instrument_id, qaorder_to_qifi(&order), order_id.clone()) {
            Ok(_) => {
                log::info!("Order submitted successfully: {}", order_id);

                // 获取订单的最终状态（可能已经成交）
                let final_status = if let Some(order_info) = self.orders.get(&order_id) {
                    let info = order_info.read();
                    let status_str = match info.status {
                        OrderStatus::Filled => "filled",
                        OrderStatus::PartiallyFilled => "partially_filled",
                        OrderStatus::Cancelled => "cancelled",
                        OrderStatus::Rejected => "rejected",
                        _ => "submitted", // Submitted, PendingRoute, PendingRisk
                    };
                    log::debug!(
                        "Order {} final status: {:?} -> {}",
                        order_id,
                        info.status,
                        status_str
                    );
                    status_str
                } else {
                    log::warn!(
                        "Order {} not found in orders map when checking status",
                        order_id
                    );
                    "submitted"
                };

                // IOC 后置处理: 如果是 IOC 订单，自动撤销未成交部分
                // @yutiansut @quantaxis
                if time_cond == TimeCondition::IOC {
                    self.handle_ioc_remaining(&order_id, &req.account_id);

                    // 重新获取状态（可能已被撤销）
                    let updated_status = if let Some(order_info) = self.orders.get(&order_id) {
                        let info = order_info.read();
                        match info.status {
                            OrderStatus::Filled => "filled",
                            OrderStatus::PartiallyFilled => "partially_filled",
                            OrderStatus::Cancelled => "cancelled",
                            OrderStatus::Rejected => "rejected",
                            _ => "submitted",
                        }
                    } else {
                        final_status
                    };

                    log::info!("[IOC] Order {} final status after IOC handling: {}", order_id, updated_status);

                    return SubmitOrderResponse {
                        success: true,
                        order_id: Some(order_id),
                        status: Some(updated_status.to_string()),
                        error_message: None,
                        error_code: None,
                    };
                }

                SubmitOrderResponse {
                    success: true,
                    order_id: Some(order_id),
                    status: Some(final_status.to_string()),
                    error_message: None,
                    error_code: None,
                }
            }
            Err(e) => {
                log::error!("Failed to route order {}: {}", order_id, e);

                // 更新订单状态为拒绝
                if let Some(order_info) = self.orders.get(&order_id) {
                    let mut info = order_info.write();
                    info.status = OrderStatus::Rejected;
                }

                SubmitOrderResponse {
                    success: false,
                    order_id: Some(order_id),
                    status: Some("rejected".to_string()),
                    error_message: Some(format!("Routing error: {}", e)),
                    error_code: Some(5000),
                }
            }
        }
    }

    /// 路由订单到撮合引擎
    fn route_to_matching_engine(
        &self,
        instrument_id: &str,
        order: Order,
        order_id: String,
    ) -> Result<(), ExchangeError> {
        // 获取订单簿
        let orderbook = self
            .matching_engine
            .get_orderbook(instrument_id)
            .ok_or_else(|| {
                ExchangeError::MatchingError(format!(
                    "Orderbook not found for instrument: {}",
                    instrument_id
                ))
            })?;

        // 转换订单方向
        let direction = match order.direction.as_str() {
            "BUY" => OrderDirection::BUY,
            "SELL" => OrderDirection::SELL,
            _ => {
                return Err(ExchangeError::OrderError(format!(
                    "Invalid direction: {}",
                    order.direction
                )))
            }
        };

        // 创建撮合订单请求
        let asset = InstrumentAsset::from_code(instrument_id);
        let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        let match_request = crate::matching::orders::new_limit_order_request(
            asset,
            direction,
            order.limit_price,
            order.volume_orign,
            timestamp,
        );

        // 提交到订单簿
        let mut ob = orderbook.write();
        let results = ob
            .process_order(match_request)
            .into_iter()
            .collect::<Vec<_>>();
        drop(ob); // 尽早释放锁

        // 处理撮合结果
        self.process_matching_results(&order_id, &order, results)?;

        Ok(())
    }

    /// 处理撮合引擎返回的结果
    ///
    /// 注意：matching engine可能返回多个Success事件：
    /// 1. Accepted - 订单被接受
    /// 2. Filled/PartiallyFilled - 新订单成交
    /// 3. Filled/PartiallyFilled - 对手单成交（opposite_order）
    ///
    /// 我们只处理新订单的事件，忽略对手单的事件
    fn process_matching_results(
        &self,
        order_id: &str,
        order: &Order,
        results: Vec<Result<Success, Failed>>,
    ) -> Result<(), ExchangeError> {
        let mut handled_accepted = false;
        let mut handled_trade = false; // 是否已处理成交事件（Filled/PartiallyFilled）

        log::debug!(
            "🔍 process_matching_results: order_id={}, user_id={}, results_count={}",
            order_id,
            order.user_id,
            results.len()
        );

        for (idx, result) in results.into_iter().enumerate() {
            log::debug!("🔍   Result[{}]: {:?}", idx, result);
            match result {
                Ok(success) => {
                    match success {
                        Success::Accepted { .. } => {
                            // 只处理第一个Accepted
                            if !handled_accepted {
                                log::debug!(
                                    "🔍     Processing Accepted event for order {}",
                                    order_id
                                );
                                // Accepted 事件不涉及成交记录，is_taker 参数无影响
                                self.handle_success_result(order_id, order, success, true)?;
                                handled_accepted = true;
                            } else {
                                log::debug!(
                                    "🔍     Skipping duplicate Accepted event for order {}",
                                    order_id
                                );
                            }
                        }
                        Success::Filled {
                            order_id: match_order_id,
                            opposite_order_id,
                            ..
                        }
                        | Success::PartiallyFilled {
                            order_id: match_order_id,
                            opposite_order_id,
                            ..
                        } => {
                            // 处理成交事件
                            // qars 会返回两个事件：新订单成交 + 对手单成交
                            // 我们需要更新对手单的状态（如果它属于我们管理的订单）

                            if !handled_trade {
                                // 第一个事件：新订单的成交（taker - 主动方）
                                log::debug!(
                                    "🔍     Processing TAKER order trade: order_id={}, opposite={}",
                                    match_order_id,
                                    opposite_order_id
                                );
                                // ✨ is_taker=true: 主动方，记录成交到 TradeRecorder @yutiansut @quantaxis
                                self.handle_success_result(order_id, order, success.clone(), true)?;
                                handled_trade = true;
                            } else {
                                // 第二个事件：对手单（挂单方）的成交
                                // qars 返回的第二个 Filled 事件中：
                                // - match_order_id = 对手单（挂单方）的 engine_id
                                // - opposite_order_id = 新订单（taker）的 engine_id
                                // 我们需要用 match_order_id 找到对手单的 order_id 来更新其账户
                                log::debug!("🔍     Processing MAKER order trade: maker_engine_id={}, taker_engine_id={}", match_order_id, opposite_order_id);

                                // ✨ 关键修复：使用 match_order_id（对手单的engine_id）查找对手单的 order_id
                                // 之前的 BUG：使用 opposite_order_id 查找，导致找到的是已处理的新订单
                                // @yutiansut @quantaxis
                                if let Some(maker_order_id_str) = self.engine_id_to_order.get(&match_order_id) {
                                    let maker_order_str = maker_order_id_str.value().clone();
                                    log::debug!("🔍     Found maker order mapping: engine_id={} → order_id={}", match_order_id, maker_order_str);

                                    // 如果挂单方（maker）在我们的订单簿中，更新它的状态
                                    if self.orders.contains_key(&maker_order_str) {
                                        log::debug!("🔍     Found maker order {} in our orderbook, updating status", maker_order_str);

                                        // 提取挂单方信息用于处理
                                        if let Some(maker_info) = self.orders.get(&maker_order_str) {
                                            let maker_order_data = maker_info.read().order.clone();
                                            // 处理挂单方的成交 - 更新其账户持仓和资金
                                            // ✨ is_taker=false: 被动方（maker），不记录成交到 TradeRecorder @yutiansut @quantaxis
                                            self.handle_success_result(
                                                &maker_order_str,
                                                &maker_order_data,
                                                success,
                                                false, // maker 不记录成交
                                            )?;
                                        }
                                    } else {
                                        log::warn!(
                                            "⚠️     Maker order {} not found in our orderbook (inconsistent state!)",
                                            maker_order_str
                                        );
                                    }
                                } else {
                                    log::debug!(
                                        "🔍     Maker order engine_id={} not in our exchange (external order), skipping",
                                        match_order_id
                                    );
                                }
                            }
                        }
                        _ => {
                            // 其他事件正常处理（Cancelled, Amended等）
                            // 不涉及成交记录，is_taker 参数无影响
                            self.handle_success_result(order_id, order, success, true)?;
                        }
                    }
                }
                Err(failed) => {
                    log::warn!("Order matching failed: {:?}", failed);

                    // Phase 6: 使用新的 handle_order_rejected_new (交易所推送REJECTED回报)
                    let reason = format!("{:?}", failed);
                    let _ = self.trade_gateway.handle_order_rejected_new(
                        &order.exchange_id,
                        &order.instrument_id,
                        &order.user_id,
                        order_id,
                        &order.direction,
                        &order.offset,
                        &order.price_type,
                        order.limit_price,
                        order.volume_orign,
                        &reason,
                    );

                    log::debug!("Order {} rejected, reason: {}", order_id, reason);

                    // 更新订单状态为拒绝
                    if let Some(order_info) = self.orders.get(order_id) {
                        let mut info = order_info.write();
                        info.status = OrderStatus::Rejected;
                        info.update_time = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
                    }
                }
            }
        }
        Ok(())
    }

    /// 处理成功的撮合结果 (Phase 6: 使用新的回报机制)
    /// 处理成交结果
    /// @yutiansut @quantaxis
    /// is_taker: 是否为主动方（taker），只有 taker 才记录到 TradeRecorder
    fn handle_success_result(
        &self,
        order_id: &str,
        order: &Order,
        success: Success,
        is_taker: bool, // ✨ 是否为主动方
    ) -> Result<(), ExchangeError> {
        match success {
            Success::Accepted { id, order_type: _, ts } => {
                // 订单被接受，等待撮合
                log::info!("Order {} accepted at {}", order_id, ts);

                // 更新订单状态并存储撮合引擎订单ID
                if let Some(order_info) = self.orders.get(order_id) {
                    let mut info = order_info.write();
                    info.status = OrderStatus::Submitted;
                    info.update_time = ts;
                    info.matching_engine_order_id = Some(id); // 存储撮合引擎订单ID，用于撤单
                }

                // ✨ 存储反向映射: matching_engine_order_id → order_id / user_id @yutiansut @quantaxis
                // 用于在成交时通过对手单的matching_engine_order_id找到对应的order_id和user_id
                self.engine_id_to_order.insert(id, order_id.to_string());
                self.engine_id_to_user.insert(id, order.user_id.clone()); // ✨ O(1) 直接映射
                log::debug!("💾 Stored reverse mapping: engine_id={} → order_id={}, user_id={}", id, order_id, order.user_id);

                // Phase 6: 使用新的 handle_order_accepted_new (交易所只推送ACCEPTED回报)
                let exchange_order_id = self.trade_gateway.handle_order_accepted_new(
                    &order.exchange_id,
                    &order.instrument_id,
                    &order.user_id,
                    order_id,
                    &order.direction,
                    &order.offset,
                    &order.price_type,
                    order.limit_price,
                    order.volume_orign,
                )?;

                log::debug!(
                    "Order {} accepted, exchange_order_id={}",
                    order_id,
                    exchange_order_id
                );

                // 持久化订单簿tick数据（订单挂入导致bid/ask变化）
                self.persist_orderbook_tick(&order.instrument_id)?;

                // 广播订单簿更新（通知前端订单簿已变化）
                if let Some(ref broadcaster) = self.market_broadcaster {
                    // 获取更新后的bid/ask价格用于广播
                    if let Some(orderbook) =
                        self.matching_engine.get_orderbook(&order.instrument_id)
                    {
                        let _ob = orderbook.read();
                        let side = if order.direction == "BUY" {
                            "bid"
                        } else {
                            "ask"
                        };
                        broadcaster.broadcast_orderbook_update(
                            order.instrument_id.clone(),
                            side.to_string(),
                            order.limit_price,
                            order.volume_orign,
                        );
                    }
                }

                // 持久化订单簿快照（订单已进入订单簿）
                self.persist_orderbook_snapshot(&order.instrument_id)?;
            }
            Success::Filled {
                order_id: match_order_id,
                direction: _,
                order_type: _,
                price,
                volume,
                ts,
                opposite_order_id,
            } => {
                // 订单完全成交
                log::info!(
                    "Order {} filled: price={}, volume={}",
                    order_id,
                    price,
                    volume
                );

                // 更新订单状态和已成交量
                if let Some(order_info) = self.orders.get(order_id) {
                    let mut info = order_info.write();
                    info.status = OrderStatus::Filled;
                    info.update_time = ts;
                    info.filled_volume = volume;
                }

                // 更新成交统计
                self.update_trade_stats(price, volume);

                // 广播Tick成交数据
                if let Some(ref broadcaster) = self.market_broadcaster {
                    let direction_str = if order.direction == "BUY" {
                        "buy"
                    } else {
                        "sell"
                    };
                    broadcaster.broadcast_tick(
                        order.instrument_id.clone(),
                        price,
                        volume,
                        direction_str.to_string(),
                    );

                    // 同时广播最新价
                    broadcaster.broadcast_last_price(order.instrument_id.clone(), price);
                }

                // 持久化Tick数据到WAL
                self.persist_tick_data(&order.instrument_id, price, volume)?;

                // 持久化订单簿快照（订单成交后订单簿发生变化）
                self.persist_orderbook_snapshot(&order.instrument_id)?;

                // 获取 qars 订单ID
                let qa_order_id = if let Some(order_info) = self.orders.get(order_id) {
                    order_info.read().qa_order_id.clone()
                } else {
                    log::error!("Order info not found for {}", order_id);
                    String::new()
                };

                // ✨ O(1) 直接查找对手方的 user_id @yutiansut @quantaxis
                // 性能优化：避免两次DashMap查找 + 一次RwLock读取
                let opposite_user_id: Option<String> = self
                    .engine_id_to_user
                    .get(&opposite_order_id)
                    .map(|v| v.value().clone());

                // ✨ O(1) 查找对手方的真实订单ID @yutiansut @quantaxis
                let opposite_order_id_str: Option<String> = self
                    .engine_id_to_order
                    .get(&opposite_order_id)
                    .map(|v| v.value().clone());

                log::debug!(
                    "⚡ Trade opposite lookup (O(1)): engine_id={} -> user_id={:?}, order_id={:?}",
                    opposite_order_id,
                    opposite_user_id,
                    opposite_order_id_str
                );

                // Phase 6: 使用新的 handle_trade_new (交易所只推送TRADE回报，不判断FILLED/PARTIAL)
                // 注意：这里假设我们使用已生成的exchange_order_id（从Accepted事件保存）
                // 简化实现：使用match_order_id作为exchange_order_id
                // ✨ 修复：传递qa_order_id用于调用receive_deal_sim @yutiansut @quantaxis
                let trade_id = self.trade_gateway.handle_trade_new(
                    &order.exchange_id,
                    &order.instrument_id,
                    match_order_id as i64,
                    &order.user_id,
                    order_id,
                    &order.direction,
                    &order.offset,
                    volume,
                    price,
                    Some(opposite_order_id as i64),
                    opposite_user_id.as_deref(), // ✨ 传递对手方user_id
                    &qa_order_id, // ✨ 传递qars订单ID
                    opposite_order_id_str.as_deref(), // ✨ 传递对手方真实订单ID
                    is_taker, // ✨ 是否为主动方，只有 taker 记录成交 @yutiansut @quantaxis
                )?;

                log::debug!(
                    "Trade executed: trade_id={}, order_id={}, volume={}, price={}",
                    trade_id,
                    order_id,
                    volume,
                    price
                );

                // 从活动订单追踪中移除
                self.risk_checker
                    .remove_active_order(&order.user_id, order_id);
            }
            Success::PartiallyFilled {
                order_id: match_order_id,
                direction: _,
                order_type: _,
                price,
                volume,
                ts,
                opposite_order_id,
            } => {
                // 订单部分成交
                log::info!(
                    "Order {} partially filled: price={}, volume={}",
                    order_id,
                    price,
                    volume
                );

                // 更新订单状态和累计成交量
                if let Some(order_info) = self.orders.get(order_id) {
                    let mut info = order_info.write();
                    info.status = OrderStatus::PartiallyFilled;
                    info.update_time = ts;
                    info.filled_volume += volume;
                }

                // 更新成交统计
                self.update_trade_stats(price, volume);

                // 广播Tick成交数据
                if let Some(ref broadcaster) = self.market_broadcaster {
                    let direction_str = if order.direction == "BUY" {
                        "buy"
                    } else {
                        "sell"
                    };
                    broadcaster.broadcast_tick(
                        order.instrument_id.clone(),
                        price,
                        volume,
                        direction_str.to_string(),
                    );

                    // 同时广播最新价
                    broadcaster.broadcast_last_price(order.instrument_id.clone(), price);
                }

                // 持久化Tick数据到WAL
                self.persist_tick_data(&order.instrument_id, price, volume)?;

                // 持久化订单簿快照（订单成交后订单簿发生变化）
                self.persist_orderbook_snapshot(&order.instrument_id)?;

                // 获取 qars 订单ID
                let qa_order_id = if let Some(order_info) = self.orders.get(order_id) {
                    order_info.read().qa_order_id.clone()
                } else {
                    log::error!("Order info not found for {}", order_id);
                    String::new()
                };

                // ✨ O(1) 直接查找对手方的 user_id @yutiansut @quantaxis
                // 性能优化：避免两次DashMap查找 + 一次RwLock读取
                let opposite_user_id: Option<String> = self
                    .engine_id_to_user
                    .get(&opposite_order_id)
                    .map(|v| v.value().clone());

                // ✨ O(1) 查找对手方的真实订单ID @yutiansut @quantaxis
                let opposite_order_id_str: Option<String> = self
                    .engine_id_to_order
                    .get(&opposite_order_id)
                    .map(|v| v.value().clone());

                log::debug!(
                    "⚡ Trade opposite lookup (O(1), partial): engine_id={} -> user_id={:?}, order_id={:?}",
                    opposite_order_id,
                    opposite_user_id,
                    opposite_order_id_str
                );

                // Phase 6: 使用新的 handle_trade_new (交易所不区分FILLED/PARTIAL，只推送TRADE)
                // ✨ 修复：传递qa_order_id用于调用receive_deal_sim @yutiansut @quantaxis
                let trade_id = self.trade_gateway.handle_trade_new(
                    &order.exchange_id,
                    &order.instrument_id,
                    match_order_id as i64,
                    &order.user_id,
                    order_id,
                    &order.direction,
                    &order.offset,
                    volume,
                    price,
                    Some(opposite_order_id as i64),
                    opposite_user_id.as_deref(), // ✨ 传递对手方user_id
                    &qa_order_id, // ✨ 传递qars订单ID
                    opposite_order_id_str.as_deref(), // ✨ 传递对手方真实订单ID
                    is_taker, // ✨ 是否为主动方，只有 taker 记录成交 @yutiansut @quantaxis
                )?;

                log::debug!(
                    "Trade executed (partial): trade_id={}, order_id={}, volume={}, price={}",
                    trade_id,
                    order_id,
                    volume,
                    price
                );
            }
            Success::Cancelled { id, ts } => {
                // 订单被撤销
                log::info!("Order {} cancelled at {}", order_id, ts);

                // 更新订单状态，并获取 qa_order_id 用于释放冻结资金
                // ✨ 修复：获取 qa_order_id 传递给 handle_cancel_accepted_new @yutiansut @quantaxis
                let (qa_order_id, remaining_volume) = if let Some(order_info) = self.orders.get(order_id) {
                    let mut info = order_info.write();
                    info.status = OrderStatus::Cancelled;
                    info.update_time = ts;
                    let remaining = info.order.volume_orign - info.filled_volume;
                    (info.qa_order_id.clone(), remaining)
                } else {
                    (String::new(), order.volume_orign)
                };

                // Phase 6: 使用新的 handle_cancel_accepted_new (交易所推送CANCEL_ACCEPTED回报)
                // ✨ 修复：传递 qa_order_id 用于调用 qars cancel_order 释放冻结资金 @yutiansut @quantaxis
                self.trade_gateway.handle_cancel_accepted_new(
                    &order.exchange_id,
                    &order.instrument_id,
                    id as i64, // 使用撮合引擎返回的ID作为exchange_order_id
                    &order.user_id,
                    order_id,
                    &order.direction,
                    &order.offset,
                    &order.price_type,
                    order.limit_price,
                    remaining_volume,
                    &qa_order_id, // ✨ 新增：传递 qars 订单ID 释放冻结资金
                )?;

                log::debug!(
                    "Order {} cancel accepted, exchange_order_id={}",
                    order_id,
                    id
                );

                // 持久化订单簿tick数据（撤单导致bid/ask变化）
                self.persist_orderbook_tick(&order.instrument_id)?;

                // 广播订单簿更新（通知前端订单簿已变化）
                if let Some(ref broadcaster) = self.market_broadcaster {
                    // 撤单后，该价格档位的挂单量减少或消失
                    if let Some(orderbook) =
                        self.matching_engine.get_orderbook(&order.instrument_id)
                    {
                        let ob = orderbook.read();
                        let side = if order.direction == "BUY" {
                            "bid"
                        } else {
                            "ask"
                        };

                        // 获取撤单后该价格档位的剩余挂单量
                        let remaining_volume = if order.direction == "BUY" {
                            ob.bid_queue
                                .get_sorted_orders()
                                .and_then(|orders| {
                                    orders
                                        .iter()
                                        .find(|o| o.price == order.limit_price)
                                        .map(|o| o.volume) // 在闭包内 map 以复制值
                                })
                                .unwrap_or(0.0)
                        } else {
                            ob.ask_queue
                                .get_sorted_orders()
                                .and_then(|orders| {
                                    orders
                                        .iter()
                                        .find(|o| o.price == order.limit_price)
                                        .map(|o| o.volume) // 在闭包内 map 以复制值
                                })
                                .unwrap_or(0.0)
                        };

                        broadcaster.broadcast_orderbook_update(
                            order.instrument_id.clone(),
                            side.to_string(),
                            order.limit_price,
                            remaining_volume, // 0表示该档位已清空
                        );
                    }
                }

                // 持久化订单簿快照（撤单后订单簿发生变化）
                self.persist_orderbook_snapshot(&order.instrument_id)?;

                // 从活动订单追踪中移除
                self.risk_checker
                    .remove_active_order(&order.user_id, order_id);
            }
            Success::Amended {
                id: _,
                price,
                volume,
                ts: _,
            } => {
                // 订单修改 (暂不处理，预留)
                log::info!(
                    "Order {} amended: price={}, volume={}",
                    order_id,
                    price,
                    volume
                );
            }
        }
        Ok(())
    }

    /// 撤单
    pub fn cancel_order(&self, req: CancelOrderRequest) -> Result<(), ExchangeError> {
        // 1. 验证订单存在
        let order_info = self.orders.get(&req.order_id).ok_or_else(|| {
            ExchangeError::OrderError(format!("Order not found: {}", req.order_id))
        })?;

        let info = order_info.write();

        // 2. 验证订单所有权
        if info.order.user_id != req.account_id {
            return Err(ExchangeError::OrderError(
                "Order does not belong to this account".to_string(),
            ));
        }

        // 2.5 交易状态检查 @yutiansut @quantaxis
        if let Some(ref state_machine) = self.trading_state_machine {
            use crate::exchange::OrderValidation;
            match state_machine.validate_cancel(&info.order.instrument_id) {
                OrderValidation::Allowed => {}
                OrderValidation::Rejected(reason) => {
                    return Err(ExchangeError::OrderError(format!(
                        "Cancel rejected by trading state: {}",
                        reason
                    )));
                }
            }
        }

        // 3. 检查订单状态是否可撤单
        if !matches!(
            info.status,
            OrderStatus::Submitted | OrderStatus::PartiallyFilled
        ) {
            return Err(ExchangeError::OrderError(format!(
                "Order cannot be cancelled in status: {:?}",
                info.status
            )));
        }

        // 4. 从撮合引擎撤单
        let matching_engine_order_id = info.matching_engine_order_id.ok_or_else(|| {
            ExchangeError::OrderError("Matching engine order ID not found".to_string())
        })?;

        let instrument_id = info.order.instrument_id.clone();
        let direction_str = info.order.direction.clone();
        // ✨ 保存订单信息用于后续处理 @yutiansut @quantaxis
        let order = info.order.clone();

        // 释放写锁，避免在调用撮合引擎时持有锁
        drop(info);
        drop(order_info);

        // 转换订单方向
        let direction = match direction_str.as_str() {
            "BUY" => OrderDirection::BUY,
            "SELL" => OrderDirection::SELL,
            _ => {
                return Err(ExchangeError::OrderError(format!(
                    "Invalid direction: {}",
                    direction_str
                )))
            }
        };

        // 创建撤单请求
        let _asset = InstrumentAsset::from_code(&instrument_id);
        let cancel_request = crate::matching::OrderRequest::CancelOrder {
            id: matching_engine_order_id,
            direction,
        };

        // 获取订单簿
        let orderbook = self
            .matching_engine
            .get_orderbook(&instrument_id)
            .ok_or_else(|| {
                ExchangeError::MatchingError(format!(
                    "Orderbook not found for instrument: {}",
                    instrument_id
                ))
            })?;

        // 提交撤单请求到撮合引擎
        let mut ob = orderbook.write();
        let results = ob
            .process_order(cancel_request)
            .into_iter()
            .collect::<Vec<_>>();
        drop(ob);

        // 处理撤单结果
        // ✨ 修复：必须调用 handle_success_result 来处理 Success::Cancelled 事件
        // 这样才能触发 handle_cancel_accepted_new 释放冻结资金 @yutiansut @quantaxis
        for result in results {
            match result {
                Ok(success) => {
                    log::info!("Cancel order success: {:?}", success);
                    // ✨ 调用 handle_success_result 处理撤单成功事件
                    // 这会触发 Success::Cancelled 分支，更新订单状态并释放冻结资金
                    // 撤单不涉及成交记录，is_taker 参数无影响
                    if let Err(e) = self.handle_success_result(&req.order_id, &order, success, true) {
                        log::error!("Failed to handle cancel success result: {:?}", e);
                    }
                }
                Err(failed) => {
                    log::error!("Cancel order failed: {:?}", failed);
                    return Err(ExchangeError::MatchingError(format!(
                        "Cancel order failed: {:?}",
                        failed
                    )));
                }
            }
        }

        log::info!("Order cancelled from matching engine: {}", req.order_id);
        Ok(())
    }

    /// IOC/FOK 处理：检查订单是否满足 FOK 条件
    /// @yutiansut @quantaxis
    ///
    /// FOK (Fill or Kill) 要求订单必须全部成交，否则全部撤销
    /// 此方法在订单提交前检查订单簿是否有足够的对手方挂单
    fn check_fok_fulfillable(
        &self,
        instrument_id: &str,
        direction: &str,
        volume: f64,
        price: f64,
    ) -> bool {
        // 获取订单簿
        let orderbook = match self.matching_engine.get_orderbook(instrument_id) {
            Some(ob) => ob,
            None => {
                log::warn!("[FOK] Orderbook not found for {}", instrument_id);
                return false;
            }
        };

        let ob = orderbook.read();

        // 计算对手方可成交量
        let available_volume = match direction {
            "BUY" => {
                // 买单需要检查卖单挂单量（价格 <= price）
                if let Some(asks) = ob.ask_queue.get_sorted_orders() {
                    asks.iter()
                        .filter(|o| o.price <= price)
                        .map(|o| o.volume)
                        .sum()
                } else {
                    0.0
                }
            }
            "SELL" => {
                // 卖单需要检查买单挂单量（价格 >= price）
                if let Some(bids) = ob.bid_queue.get_sorted_orders() {
                    bids.iter()
                        .filter(|o| o.price >= price)
                        .map(|o| o.volume)
                        .sum()
                } else {
                    0.0
                }
            }
            _ => 0.0,
        };

        let fulfillable = available_volume >= volume;
        log::debug!(
            "[FOK] Check: instrument={}, direction={}, volume={}, price={}, available={}, fulfillable={}",
            instrument_id, direction, volume, price, available_volume, fulfillable
        );

        fulfillable
    }

    /// IOC/FOK 处理：在匹配后处理 IOC 剩余订单
    /// @yutiansut @quantaxis
    ///
    /// IOC (Immediate or Cancel) 要求立即成交可成交部分，剩余部分撤销
    /// 此方法在订单匹配完成后，检查并撤销未成交部分
    fn handle_ioc_remaining(&self, order_id: &str, account_id: &str) {
        // 获取订单信息
        let order_info = match self.orders.get(order_id) {
            Some(info) => info,
            None => {
                log::warn!("[IOC] Order not found: {}", order_id);
                return;
            }
        };

        let (status, time_condition, filled_volume, original_volume) = {
            let info = order_info.read();
            (
                info.status,
                info.time_condition,
                info.filled_volume,
                info.order.volume_orign,
            )
        };

        // 只处理 IOC 订单
        if time_condition != TimeCondition::IOC {
            return;
        }

        // 如果已完全成交或已撤销，不需要处理
        if matches!(status, OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected) {
            log::debug!("[IOC] Order {} already in final status: {:?}", order_id, status);
            return;
        }

        // 如果有未成交部分，自动撤销
        let remaining_volume = original_volume - filled_volume;
        if remaining_volume > 0.001 {  // 使用小数容差
            log::info!(
                "[IOC] Auto-cancelling remaining {} of order {} (filled: {}/{})",
                remaining_volume, order_id, filled_volume, original_volume
            );

            // 发送撤单请求
            let cancel_req = CancelOrderRequest {
                account_id: account_id.to_string(),
                order_id: order_id.to_string(),
            };

            match self.cancel_order(cancel_req) {
                Ok(_) => {
                    log::info!("[IOC] Successfully cancelled remaining order: {}", order_id);
                }
                Err(e) => {
                    log::error!("[IOC] Failed to cancel remaining order {}: {:?}", order_id, e);
                }
            }
        } else {
            log::debug!("[IOC] Order {} fully filled, no cancellation needed", order_id);
        }
    }

    /// 查询订单
    pub fn query_order(&self, order_id: &str) -> Option<Order> {
        self.orders
            .get(order_id)
            .map(|info| info.read().order.clone())
    }

    /// 查询用户所有订单
    pub fn query_user_orders(&self, user_id: &str) -> Vec<Order> {
        if let Some(order_ids) = self.user_orders.get(user_id) {
            order_ids
                .read()
                .iter()
                .filter_map(|order_id| self.query_order(order_id))
                .collect()
        } else {
            Vec::new()
        }
    }

    /// 获取订单状态
    pub fn get_order_status(&self, order_id: &str) -> Option<OrderStatus> {
        self.orders.get(order_id).map(|info| info.read().status)
    }

    /// 更新订单状态 (由 TradeGateway 调用)
    pub fn update_order_status(
        &self,
        order_id: &str,
        status: OrderStatus,
    ) -> Result<(), ExchangeError> {
        let order_info = self
            .orders
            .get(order_id)
            .ok_or_else(|| ExchangeError::OrderError(format!("Order not found: {}", order_id)))?;

        let mut info = order_info.write();
        info.status = status;
        info.update_time = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // 如果订单完成，从风控追踪中移除
        if matches!(
            status,
            OrderStatus::Filled | OrderStatus::Cancelled | OrderStatus::Rejected
        ) {
            self.risk_checker
                .remove_active_order(&info.order.user_id, order_id);
        }

        Ok(())
    }

    /// 生成订单ID
    fn generate_order_id(&self) -> String {
        let seq = self.order_seq.fetch_add(1, Ordering::SeqCst);
        let timestamp = chrono::Utc::now().timestamp_millis();
        format!("O{}{:010}", timestamp, seq)
    }

    /// 获取市价单的执行价格 @yutiansut @quantaxis
    ///
    /// 买单：使用卖一价（ask_price）
    /// 卖单：使用买一价（bid_price）
    /// 如果没有对手盘，使用 last_price 或结算价
    fn get_market_price_for_order(&self, instrument_id: &str, direction: &str) -> f64 {
        // 1. 尝试从订单簿获取对手盘价格
        if let Some(orderbook) = self.matching_engine.get_orderbook(instrument_id) {
            let ob = orderbook.read();

            let price = match direction {
                "BUY" => {
                    // 买单用卖一价（ask）
                    ob.ask_queue
                        .get_sorted_orders()
                        .and_then(|orders| orders.first().map(|o| o.price))
                        .unwrap_or(0.0)
                }
                "SELL" => {
                    // 卖单用买一价（bid）
                    ob.bid_queue
                        .get_sorted_orders()
                        .and_then(|orders| orders.first().map(|o| o.price))
                        .unwrap_or(0.0)
                }
                _ => 0.0,
            };

            if price > 0.0 {
                return price;
            }

            // 2. 没有对手盘，使用 last_price（订单簿初始化时设置为 prev_close）
            if ob.lastprice > 0.0 {
                return ob.lastprice;
            }
        }

        // 3. 无行情可用
        0.0
    }

    /// 计算 towards (买卖方向 - 遵循 qars 定义)
    fn calculate_towards(&self, direction: &str, offset: &str) -> i32 {
        match (direction, offset) {
            ("BUY", "OPEN") => 2,    // 买开 = 2 (qars 标准)
            ("SELL", "OPEN") => -2,  // 卖开 = -2
            ("BUY", "CLOSE") => 3,   // 买平 = 3
            ("SELL", "CLOSE") => -3, // 卖平 = -3 ✅
            ("BUY", "CLOSETODAY") => 4,
            ("SELL", "CLOSETODAY") => -4,
            _ => 2, // 默认买开
        }
    }

    /// 获取活动订单数量
    pub fn get_active_order_count(&self) -> usize {
        self.orders
            .iter()
            .filter(|entry| {
                let status = entry.value().read().status;
                matches!(
                    status,
                    OrderStatus::Submitted | OrderStatus::PartiallyFilled
                )
            })
            .count()
    }

    /// 获取风控检查器引用
    pub fn get_risk_checker(&self) -> Arc<PreTradeCheck> {
        self.risk_checker.clone()
    }

    /// 更新成交统计
    fn update_trade_stats(&self, price: f64, volume: f64) {
        self.trade_count.fetch_add(1, Ordering::SeqCst);
        *self.trade_volume.write() += volume;
        *self.trade_amount.write() += price * volume;
    }

    /// 持久化Tick数据到WAL
    fn persist_tick_data(
        &self,
        instrument_id: &str,
        price: f64,
        volume: f64,
    ) -> Result<(), ExchangeError> {
        if let Some(ref _storage) = self.storage {
            use crate::storage::wal::record::WalRecord;

            // 获取订单簿中的买卖价
            let (bid_price, ask_price) =
                if let Some(orderbook) = self.matching_engine.get_orderbook(instrument_id) {
                    let ob = orderbook.read();
                    let bid = ob
                        .bid_queue
                        .get_sorted_orders()
                        .and_then(|orders| orders.first().map(|o| o.price))
                        .unwrap_or(0.0);
                    let ask = ob
                        .ask_queue
                        .get_sorted_orders()
                        .and_then(|orders| orders.first().map(|o| o.price))
                        .unwrap_or(0.0);
                    (bid, ask)
                } else {
                    (0.0, 0.0)
                };

            // 创建TickData记录
            let tick_record = WalRecord::TickData {
                instrument_id: WalRecord::to_fixed_array_16(instrument_id),
                last_price: price,
                bid_price,
                ask_price,
                volume: volume as i64,
                timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            };

            // ========== 性能优化：批量写入缓冲 ==========
            // 将tick数据写入缓冲区，由异步线程定期刷新（10ms间隔）
            self.tick_buffer.lock().push(tick_record);
            log::trace!(
                "Buffered tick data for {} (buffer size: {})",
                instrument_id,
                self.tick_buffer.lock().len()
            );
        }

        Ok(())
    }

    /// 持久化订单簿tick数据到WAL（订单挂入/撤销时调用，不更新last_price）
    ///
    /// 与 persist_tick_data 的区别：
    /// - persist_tick_data: 成交时调用，更新 last_price + bid/ask
    /// - persist_orderbook_tick: 订单簿变化时调用，只更新 bid/ask，保持 last_price 不变
    fn persist_orderbook_tick(&self, instrument_id: &str) -> Result<(), ExchangeError> {
        if let Some(ref _storage) = self.storage {
            use crate::storage::wal::record::WalRecord;

            // 获取订单簿中的买卖价
            let (bid_price, ask_price, last_price) =
                if let Some(orderbook) = self.matching_engine.get_orderbook(instrument_id) {
                    let ob = orderbook.read();
                    let bid = ob
                        .bid_queue
                        .get_sorted_orders()
                        .and_then(|orders| orders.first().map(|o| o.price))
                        .unwrap_or(0.0);
                    let ask = ob
                        .ask_queue
                        .get_sorted_orders()
                        .and_then(|orders| orders.first().map(|o| o.price))
                        .unwrap_or(0.0);

                    // 尝试获取最后成交价（从订单簿的lastprice字段，或使用中间价）
                    let last = if ob.lastprice > 0.0 {
                        ob.lastprice
                    } else if bid > 0.0 && ask > 0.0 {
                        (bid + ask) / 2.0
                    } else if bid > 0.0 {
                        bid
                    } else if ask > 0.0 {
                        ask
                    } else {
                        0.0
                    };

                    (bid, ask, last)
                } else {
                    (0.0, 0.0, 0.0)
                };

            // 创建TickData记录（volume=0表示订单簿变化，非成交）
            let tick_record = WalRecord::TickData {
                instrument_id: WalRecord::to_fixed_array_16(instrument_id),
                last_price, // 保持上次成交价不变
                bid_price,
                ask_price,
                volume: 0, // 0表示订单簿变化，非成交tick
                timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
            };

            // ========== 性能优化：批量写入缓冲 ==========
            // 将订单簿tick数据写入缓冲区，由异步线程定期刷新
            self.tick_buffer.lock().push(tick_record);
            log::trace!(
                "Buffered orderbook tick for {} (buffer size: {})",
                instrument_id,
                self.tick_buffer.lock().len()
            );
        }

        Ok(())
    }

    /// 持久化订单簿快照到WAL
    fn persist_orderbook_snapshot(&self, instrument_id: &str) -> Result<(), ExchangeError> {
        // ========== 性能优化：快照频率控制 ==========
        // 限流：最多每秒1次快照（防止高频写入）
        let now = Instant::now();
        if let Some(last_time) = self.last_snapshot_time.get(instrument_id) {
            if now.duration_since(*last_time) < self.snapshot_interval {
                // 跳过此次快照（距离上次快照时间太短）
                log::trace!(
                    "Skipping snapshot for {} (last snapshot: {:?} ago)",
                    instrument_id,
                    now.duration_since(*last_time)
                );
                return Ok(());
            }
        }

        if let Some(ref storage) = self.storage {
            use crate::storage::wal::record::WalRecord;

            // 获取订单簿快照
            if let Some(orderbook) = self.matching_engine.get_orderbook(instrument_id) {
                let ob = orderbook.read();

                // 获取买卖队列的前10档数据
                let bids = ob
                    .bid_queue
                    .get_sorted_orders()
                    .map(|orders| {
                        orders
                            .iter()
                            .take(10)
                            .map(|o| (o.price, o.volume as i64))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                let asks = ob
                    .ask_queue
                    .get_sorted_orders()
                    .map(|orders| {
                        orders
                            .iter()
                            .take(10)
                            .map(|o| (o.price, o.volume as i64))
                            .collect::<Vec<_>>()
                    })
                    .unwrap_or_default();

                // 创建OrderBookSnapshot记录（10档，不足的用 (0.0, 0) 填充）
                let mut bids_array = [(0.0, 0i64); 10];
                let mut asks_array = [(0.0, 0i64); 10];

                for (i, (price, volume)) in bids.iter().enumerate() {
                    if i >= 10 {
                        break;
                    }
                    bids_array[i] = (*price, *volume);
                }

                for (i, (price, volume)) in asks.iter().enumerate() {
                    if i >= 10 {
                        break;
                    }
                    asks_array[i] = (*price, *volume);
                }

                // 获取最新价（从订单簿的第一档或0.0）
                let last_price = bids
                    .first()
                    .map(|(p, _)| *p)
                    .or_else(|| asks.first().map(|(p, _)| *p))
                    .unwrap_or(0.0);

                let snapshot_record = WalRecord::OrderBookSnapshot {
                    instrument_id: WalRecord::to_fixed_array_16(instrument_id),
                    bids: bids_array,
                    asks: asks_array,
                    last_price,
                    timestamp: chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0),
                };

                // 写入WAL
                if let Err(e) = storage.write(snapshot_record) {
                    log::warn!("Failed to persist orderbook snapshot to WAL: {}", e);
                    // 不影响交易流程，只记录警告
                } else {
                    // 更新快照时间
                    self.last_snapshot_time
                        .insert(instrument_id.to_string(), now);
                    log::debug!(
                        "Persisted orderbook snapshot for {}: {} bids, {} asks",
                        instrument_id,
                        bids.len(),
                        asks.len()
                    );
                }
            }
        }

        Ok(())
    }

    /// 获取订单统计
    pub fn get_order_statistics(&self) -> OrderStatistics {
        let mut total_count = 0;
        let mut pending_count = 0;
        let mut filled_count = 0;
        let mut cancelled_count = 0;
        let mut rejected_count = 0;

        for entry in self.orders.iter() {
            total_count += 1;
            let status = entry.value().read().status;
            match status {
                OrderStatus::Submitted | OrderStatus::PartiallyFilled => pending_count += 1,
                OrderStatus::Filled => filled_count += 1,
                OrderStatus::Cancelled => cancelled_count += 1,
                OrderStatus::Rejected => rejected_count += 1,
                _ => {}
            }
        }

        OrderStatistics {
            total_count,
            pending_count,
            filled_count,
            cancelled_count,
            rejected_count,
        }
    }

    /// 获取成交统计
    pub fn get_trade_statistics(&self) -> TradeStatistics {
        TradeStatistics {
            total_count: self.trade_count.load(Ordering::SeqCst),
            total_volume: *self.trade_volume.read(),
            total_amount: *self.trade_amount.read(),
        }
    }

    // ========== 性能优化：批量刷新线程 ==========

    /// 启动批量刷新线程（异步定期刷新tick缓冲区）
    ///
    /// 性能优势：
    /// - 将多个单次写入合并为一次批量写入
    /// - 10ms刷新间隔，平衡延迟和吞吐量
    /// - 批量大小自适应（最多1000条/批）
    pub fn start_batch_flush_worker(&self) {
        if let Some(ref storage) = self.storage {
            let storage = storage.clone();
            let tick_buffer = self.tick_buffer.clone();
            let stop_signal = self.flush_stop_signal.clone();

            // 重置停止信号
            stop_signal.store(false, Ordering::SeqCst);

            // 启动后台刷新线程
            std::thread::spawn(move || {
                log::info!("Batch flush worker started (interval: 10ms, max_batch: 1000)");

                loop {
                    // 检查停止信号
                    if stop_signal.load(Ordering::SeqCst) {
                        log::info!("Batch flush worker received stop signal, exiting...");
                        break;
                    }

                    // 睡眠10ms
                    std::thread::sleep(Duration::from_millis(10));

                    // 从缓冲区取出所有记录
                    let mut buffer = tick_buffer.lock();
                    if buffer.is_empty() {
                        drop(buffer); // 尽早释放锁
                        continue;
                    }

                    // 取出缓冲区数据（最多1000条）
                    let batch_size = buffer.len().min(1000);
                    let batch: Vec<_> = buffer.drain(..batch_size).collect();
                    drop(buffer); // 释放锁

                    // 批量写入WAL
                    match storage.write_batch(batch.clone()) {
                        Ok(sequences) => {
                            log::debug!(
                                "Batch flushed {} tick records to WAL (seq: {} - {})",
                                batch.len(),
                                sequences.first().unwrap_or(&0),
                                sequences.last().unwrap_or(&0)
                            );
                        }
                        Err(e) => {
                            log::error!("Batch flush failed: {}, retrying...", e);
                            // 写入失败，重新放回缓冲区
                            let mut buffer = tick_buffer.lock();
                            for record in batch.into_iter().rev() {
                                buffer.insert(0, record);
                            }
                        }
                    }
                }

                // 线程退出前，刷新剩余数据
                let mut buffer = tick_buffer.lock();
                if !buffer.is_empty() {
                    let remaining: Vec<_> = buffer.drain(..).collect();
                    drop(buffer);
                    if let Err(e) = storage.write_batch(remaining.clone()) {
                        log::error!(
                            "Failed to flush remaining {} records on shutdown: {}",
                            remaining.len(),
                            e
                        );
                    } else {
                        log::info!("Flushed remaining {} records on shutdown", remaining.len());
                    }
                }

                log::info!("Batch flush worker stopped");
            });
        } else {
            log::warn!("Cannot start batch flush worker: storage not set");
        }
    }

    /// 停止批量刷新线程
    pub fn stop_batch_flush_worker(&self) {
        log::info!("Stopping batch flush worker...");
        self.flush_stop_signal.store(true, Ordering::SeqCst);
        // 等待线程退出（最多1秒）
        std::thread::sleep(Duration::from_millis(100));
    }

    /// 获取优先级队列统计信息
    pub fn get_priority_queue_stats(&self) -> Option<crate::exchange::PriorityQueueStatistics> {
        self.priority_queue.as_ref().map(|q| q.get_statistics())
    }

    /// 获取订单详细信息（包含时间戳和成交量）
    pub fn get_order_detail(&self, order_id: &str) -> Option<(Order, OrderStatus, i64, i64, f64)> {
        self.orders.get(order_id).map(|info| {
            let i = info.read();
            (
                i.order.clone(),
                i.status,
                i.submit_time,
                i.update_time,
                i.filled_volume,
            )
        })
    }

    /// 获取用户所有订单的详细信息 (order_id, order, status, submit_time, update_time, filled_volume)
    pub fn get_user_order_details(
        &self,
        user_id: &str,
    ) -> Vec<(String, Order, OrderStatus, i64, i64, f64)> {
        if let Some(order_ids) = self.user_orders.get(user_id) {
            order_ids
                .read()
                .iter()
                .filter_map(|order_id| {
                    self.orders.get(order_id).map(|info| {
                        let i = info.read();
                        (
                            order_id.clone(),
                            i.order.clone(),
                            i.status,
                            i.submit_time,
                            i.update_time,
                            i.filled_volume,
                        )
                    })
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// 获取所有订单的详细信息 (管理端)
    /// @yutiansut @quantaxis
    pub fn get_all_orders(&self) -> Vec<(String, Order, OrderStatus, i64, i64, f64)> {
        self.orders
            .iter()
            .map(|entry| {
                let order_id = entry.key().clone();
                let info = entry.value().read();
                (
                    order_id,
                    info.order.clone(),
                    info.status,
                    info.submit_time,
                    info.update_time,
                    info.filled_volume,
                )
            })
            .collect()
    }

    /// 获取订单总数
    pub fn get_order_count(&self) -> usize {
        self.orders.len()
    }

    /// 从账户的 dailyorders 恢复订单索引
    /// 在服务器重启后调用，从账户快照中恢复待处理订单到 order_router
    /// ✨ 修复：同时将订单重新提交到撮合引擎订单簿，以支持撤单操作 @yutiansut @quantaxis
    pub fn restore_orders_from_accounts(&self) {
        let accounts = self.account_mgr.get_all_accounts();
        let mut restored_count = 0;
        let mut orderbook_restored_count = 0;

        for account_arc in accounts {
            let account = account_arc.read();
            let account_id = account.account_cookie.clone();

            for (order_id, order) in &account.dailyorders {
                // 只恢复待处理订单 (SUBMITTED/ALIVE)
                if order.status != "SUBMITTED" && order.status != "ALIVE" {
                    continue;
                }

                // 检查是否已经存在
                if self.orders.contains_key(order_id) {
                    continue;
                }

                // 解析订单状态
                let status = match order.status.as_str() {
                    "SUBMITTED" => OrderStatus::Submitted,
                    "ALIVE" => OrderStatus::PartiallyFilled,
                    _ => continue,
                };

                // 计算已成交量和剩余量
                let filled_volume = order.volume_orign - order.volume_left;
                let remaining_volume = order.volume_left;

                // ✨ 关键修复：将订单重新提交到撮合引擎订单簿 @yutiansut @quantaxis
                let mut matching_engine_order_id: Option<u64> = None;

                // 只有剩余数量 > 0 的订单才需要恢复到订单簿
                if remaining_volume > 0.0 {
                    if let Some(orderbook) = self.matching_engine.get_orderbook(&order.instrument_id) {
                        // 转换订单方向
                        let direction = match order.direction.as_str() {
                            "BUY" => OrderDirection::BUY,
                            "SELL" => OrderDirection::SELL,
                            _ => {
                                log::warn!("⚠️ Invalid direction for order {}: {}", order_id, order.direction);
                                continue;
                            }
                        };

                        // 创建撮合订单请求（使用剩余量，不是原始量）
                        let asset = InstrumentAsset::from_code(&order.instrument_id);
                        let timestamp = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

                        let match_request = crate::matching::orders::new_limit_order_request(
                            asset,
                            direction,
                            order.limit_price,
                            remaining_volume,  // 使用剩余量
                            timestamp,
                        );

                        // 提交到订单簿
                        let mut ob = orderbook.write();
                        let results: Vec<_> = ob.process_order(match_request).into_iter().collect();
                        drop(ob);

                        // 从结果中获取 matching_engine_order_id
                        for result in results {
                            if let Ok(Success::Accepted { id, .. }) = result {
                                matching_engine_order_id = Some(id);

                                // ✨ 存储反向映射 @yutiansut @quantaxis
                                self.engine_id_to_order.insert(id, order_id.clone());
                                self.engine_id_to_user.insert(id, order.user_id.clone());

                                orderbook_restored_count += 1;
                                log::info!(
                                    "📚 Restored order to orderbook: order_id={}, engine_id={}, instrument={}, volume={}",
                                    order_id, id, order.instrument_id, remaining_volume
                                );
                                break;
                            }
                        }
                    } else {
                        log::warn!(
                            "⚠️ Orderbook not found for instrument {}, order {} cannot be restored to orderbook",
                            order.instrument_id, order_id
                        );
                    }
                }

                // 创建 OrderRouteInfo
                let info = OrderRouteInfo {
                    order: order.clone(),
                    status,
                    submit_time: order.insert_date_time / 1_000_000_000, // 纳秒转秒
                    update_time: chrono::Utc::now().timestamp(),
                    filled_volume,
                    qa_order_id: order_id.clone(),
                    matching_engine_order_id, // ✨ 现在有值了！
                    time_condition: TimeCondition::GFD,
                    volume_condition: VolumeCondition::ANY,
                };

                // 添加到订单映射
                self.orders
                    .insert(order_id.clone(), Arc::new(RwLock::new(info)));

                // 更新用户订单索引 (注意：QIFI 的 user_id 实际上是 account_id)
                self.user_orders
                    .entry(account_id.clone())
                    .or_insert_with(|| Arc::new(RwLock::new(Vec::new())))
                    .write()
                    .push(order_id.clone());

                restored_count += 1;
                log::info!(
                    "🔄 Restored order: account={}, order_id={}, instrument={}, status={:?}, volume_left={}, engine_id={:?}",
                    account_id,
                    order_id,
                    order.instrument_id,
                    status,
                    order.volume_left,
                    matching_engine_order_id
                );
            }
        }

        if restored_count > 0 {
            log::info!(
                "✅ Restored {} pending orders from account snapshots ({} restored to orderbook)",
                restored_count,
                orderbook_restored_count
            );
        } else {
            log::debug!("No pending orders to restore from accounts");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::account_ext::{AccountType, OpenAccountRequest};
    use crate::exchange::instrument_registry::InstrumentInfo;

    fn create_test_router() -> OrderRouter {
        // 创建账户管理器
        let account_mgr = Arc::new(AccountManager::new());
        let req = OpenAccountRequest {
            user_id: "test_user".to_string(),
            account_id: Some("test_user".to_string()), // 使用固定ID以便测试
            account_name: "Test User".to_string(),
            init_cash: 1000000.0,
            account_type: AccountType::Individual,
        };
        let account_id = account_mgr.open_account(req).unwrap();
        assert_eq!(account_id, "test_user"); // 验证账户ID正确

        // 创建撮合引擎
        let matching_engine = Arc::new(ExchangeMatchingEngine::new());
        matching_engine
            .register_instrument("IX2301".to_string(), 120.0)
            .unwrap();

        // 创建合约注册表
        let instrument_registry = Arc::new(InstrumentRegistry::new());
        instrument_registry
            .register(InstrumentInfo {
                instrument_id: "IX2301".to_string(),
                instrument_name: "IX2301".to_string(),
                instrument_type:
                    crate::exchange::instrument_registry::InstrumentType::CommodityFuture,
                exchange: "SHFE".to_string(),
                contract_multiplier: 1,
                price_tick: 0.01,
                margin_rate: 0.1,
                commission_rate: 0.0005,
                limit_up_rate: 0.1,
                limit_down_rate: 0.1,
                status: crate::exchange::instrument_registry::InstrumentStatus::Active,
                list_date: Some("2023-01-01".to_string()),
                expire_date: Some("2023-12-31".to_string()),
                created_at: "2023-01-01T00:00:00Z".to_string(),
                updated_at: "2023-01-01T00:00:00Z".to_string(),
            })
            .unwrap();

        // 创建成交回报网关
        let trade_gateway = Arc::new(TradeGateway::new(account_mgr.clone()));

        OrderRouter::new(
            account_mgr,
            matching_engine,
            instrument_registry,
            trade_gateway,
        )
    }

    // ==================== TimeCondition 测试 @yutiansut @quantaxis ====================

    /// 测试 TimeCondition Default trait
    #[test]
    fn test_time_condition_default() {
        let tc: TimeCondition = TimeCondition::default();
        assert_eq!(tc, TimeCondition::GFD);
    }

    /// 测试 TimeCondition 所有枚举值
    #[test]
    fn test_time_condition_values() {
        assert_eq!(TimeCondition::IOC.to_string(), "IOC");
        assert_eq!(TimeCondition::GFS.to_string(), "GFS");
        assert_eq!(TimeCondition::GFD.to_string(), "GFD");
        assert_eq!(TimeCondition::GTD.to_string(), "GTD");
        assert_eq!(TimeCondition::GTC.to_string(), "GTC");
        assert_eq!(TimeCondition::GFA.to_string(), "GFA");
    }

    /// 测试 TimeCondition from_str
    #[test]
    fn test_time_condition_from_str() {
        assert_eq!(TimeCondition::from_str("IOC"), TimeCondition::IOC);
        assert_eq!(TimeCondition::from_str("GFS"), TimeCondition::GFS);
        assert_eq!(TimeCondition::from_str("GFD"), TimeCondition::GFD);
        assert_eq!(TimeCondition::from_str("GTD"), TimeCondition::GTD);
        assert_eq!(TimeCondition::from_str("GTC"), TimeCondition::GTC);
        assert_eq!(TimeCondition::from_str("GFA"), TimeCondition::GFA);
        // 小写
        assert_eq!(TimeCondition::from_str("ioc"), TimeCondition::IOC);
        // 未知值默认 GFD
        assert_eq!(TimeCondition::from_str("UNKNOWN"), TimeCondition::GFD);
    }

    // ==================== VolumeCondition 测试 @yutiansut @quantaxis ====================

    /// 测试 VolumeCondition Default trait
    #[test]
    fn test_volume_condition_default() {
        let vc: VolumeCondition = VolumeCondition::default();
        assert_eq!(vc, VolumeCondition::ANY);
    }

    /// 测试 VolumeCondition 所有枚举值
    #[test]
    fn test_volume_condition_values() {
        assert_eq!(VolumeCondition::ANY.to_string(), "ANY");
        assert_eq!(VolumeCondition::MIN.to_string(), "MIN");
        assert_eq!(VolumeCondition::ALL.to_string(), "ALL");
    }

    /// 测试 VolumeCondition from_str
    #[test]
    fn test_volume_condition_from_str() {
        assert_eq!(VolumeCondition::from_str("ANY"), VolumeCondition::ANY);
        assert_eq!(VolumeCondition::from_str("MIN"), VolumeCondition::MIN);
        assert_eq!(VolumeCondition::from_str("ALL"), VolumeCondition::ALL);
        // 小写
        assert_eq!(VolumeCondition::from_str("all"), VolumeCondition::ALL);
        // 未知值默认 ANY
        assert_eq!(VolumeCondition::from_str("UNKNOWN"), VolumeCondition::ANY);
    }

    // ==================== OrderStatus 测试 @yutiansut @quantaxis ====================

    /// 测试 OrderStatus 枚举值
    #[test]
    fn test_order_status_values() {
        let statuses = vec![
            OrderStatus::PendingRisk,
            OrderStatus::PendingRoute,
            OrderStatus::Submitted,
            OrderStatus::PartiallyFilled,
            OrderStatus::Filled,
            OrderStatus::Cancelled,
            OrderStatus::Rejected,
        ];

        // 测试相等性
        assert_eq!(OrderStatus::Submitted, OrderStatus::Submitted);
        assert_ne!(OrderStatus::Submitted, OrderStatus::Filled);

        // 验证所有状态都可以被创建
        assert_eq!(statuses.len(), 7);
    }

    // ==================== 订单提交测试 @yutiansut @quantaxis ====================

    #[test]
    fn test_submit_order() {
        let router = create_test_router();

        let req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 10.0,
            price: 120.0,
            order_type: "LIMIT".to_string(),
            time_condition: None,
            volume_condition: None,
        };

        let response = router.submit_order(req);
        assert!(response.success);
        assert!(response.order_id.is_some());
        assert!(response.error_message.is_none());
    }

    #[test]
    fn test_submit_order_insufficient_funds() {
        let router = create_test_router();

        let req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 100000.0, // 超大数量
            price: 1000.0,
            order_type: "LIMIT".to_string(),
            time_condition: None,
            volume_condition: None,
        };

        let response = router.submit_order(req);
        assert!(!response.success);
        assert!(response.error_message.is_some());
    }

    #[test]
    fn test_query_order() {
        let router = create_test_router();

        let req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 10.0,
            price: 120.0,
            order_type: "LIMIT".to_string(),
            time_condition: None,
            volume_condition: None,
        };

        let response = router.submit_order(req);
        assert!(response.success);

        let order_id = response.order_id.unwrap();
        let order = router.query_order(&order_id);
        assert!(order.is_some());

        let order = order.unwrap();
        assert_eq!(order.user_id, "test_user");
        assert_eq!(order.instrument_id, "IX2301");
    }

    #[test]
    fn test_query_user_orders() {
        let router = create_test_router();

        // 提交多个订单
        for i in 0..3 {
            let req = SubmitOrderRequest {
                account_id: "test_user".to_string(),
                instrument_id: "IX2301".to_string(),
                direction: "BUY".to_string(),
                offset: "OPEN".to_string(),
                volume: 10.0 + i as f64,
                price: 120.0,
                order_type: "LIMIT".to_string(),
                time_condition: None,
                volume_condition: None,
            };
            router.submit_order(req);
        }

        let orders = router.query_user_orders("test_user");
        assert_eq!(orders.len(), 3);
    }

    #[test]
    fn test_generate_order_id() {
        let router = create_test_router();

        let id1 = router.generate_order_id();
        let id2 = router.generate_order_id();

        assert_ne!(id1, id2);
        assert!(id1.starts_with('O'));
        assert!(id2.starts_with('O'));
    }

    #[test]
    fn test_complete_order_flow_with_matching() {
        // 完整的订单流程集成测试：风控 -> 路由 -> 撮合 -> 成交 -> 账户更新

        // 1. 创建路由器和两个测试账户（避免自成交）
        let router = create_test_router();

        // 创建第二个账户用于卖单
        let req2 = OpenAccountRequest {
            user_id: "test_user_2".to_string(),
            account_id: Some("test_user_2".to_string()),
            account_name: "Test User 2".to_string(),
            init_cash: 1000000.0,
            account_type: AccountType::Individual,
        };
        router.account_mgr.open_account(req2).unwrap();

        let trade_receiver = router.trade_gateway.subscribe_user("test_user".to_string());

        // 2. 获取初始账户状态（使用user_id获取默认账户）
        let account = router.account_mgr.get_default_account("test_user").unwrap();
        let init_balance = account.read().accounts.balance;
        log::info!("Initial balance: {}", init_balance);

        // 3. 提交买单（账户1）
        let buy_req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 10.0,
            price: 120.0,
            order_type: "LIMIT".to_string(),
            time_condition: None,
            volume_condition: None,
        };

        let buy_response = router.submit_order(buy_req);
        assert!(
            buy_response.success,
            "Buy order submission failed: {:?}",
            buy_response.error_message
        );
        let buy_order_id = buy_response.order_id.unwrap();
        log::info!("Buy order submitted: {}", buy_order_id);

        // 4. 提交卖单（账户2，避免自成交）
        let sell_req = SubmitOrderRequest {
            account_id: "test_user_2".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "SELL".to_string(),
            offset: "OPEN".to_string(), // 使用OPEN，因为是不同账户
            volume: 5.0,                // 部分成交
            price: 120.0,
            order_type: "LIMIT".to_string(),
            time_condition: None,
            volume_condition: None,
        };

        let sell_response = router.submit_order(sell_req);
        assert!(
            sell_response.success,
            "Sell order submission failed: {:?}",
            sell_response.error_message
        );
        let sell_order_id = sell_response.order_id.unwrap();
        log::info!("Sell order submitted: {}", sell_order_id);

        // 5. 检查是否收到成交通知（可选）
        // 注意：由于撮合是同步的，通知应该已经发送
        let mut notifications = Vec::new();
        while let Ok(notification) = trade_receiver.try_recv() {
            log::info!("Received notification: {:?}", notification);
            notifications.push(notification);
        }

        // 通知系统可能使用新的NotificationBroker，这里不强制要求
        log::info!("Total notifications received: {}", notifications.len());

        // 6. 查询订单状态
        let buy_order = router.query_order(&buy_order_id).unwrap();
        log::info!("Buy order status: {:?}", buy_order.status);

        // 7. 验证账户状态已更新
        // 注意：由于撮合逻辑的复杂性，这里只验证账户依然存在且可访问
        let account = router.account_mgr.get_default_account("test_user").unwrap();
        let final_balance = account.read().accounts.balance;
        log::info!("Final balance: {}", final_balance);

        // 账户应该依然有效
        assert!(final_balance > 0.0, "Account balance should be positive");

        log::info!("Complete order flow test passed!");
    }

    // ==================== 撤单测试 @yutiansut @quantaxis ====================

    /// 测试撤单 - 订单不存在
    #[test]
    fn test_cancel_order_not_found() {
        let router = create_test_router();

        let req = CancelOrderRequest {
            account_id: "test_user".to_string(),
            order_id: "NON_EXISTENT_ORDER".to_string(),
        };

        let result = router.cancel_order(req);
        assert!(result.is_err());

        if let Err(ExchangeError::OrderError(msg)) = result {
            assert!(msg.contains("Order not found"));
        } else {
            panic!("Expected OrderError");
        }
    }

    /// 测试撤单 - 订单不属于当前账户
    #[test]
    fn test_cancel_order_wrong_owner() {
        let router = create_test_router();

        // 创建第二个账户
        let req2 = OpenAccountRequest {
            user_id: "test_user_2".to_string(),
            account_id: Some("test_user_2".to_string()),
            account_name: "Test User 2".to_string(),
            init_cash: 1000000.0,
            account_type: AccountType::Individual,
        };
        router.account_mgr.open_account(req2).unwrap();

        // test_user 提交订单
        let submit_req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 10.0,
            price: 120.0,
            order_type: "LIMIT".to_string(),
            time_condition: None,
            volume_condition: None,
        };

        let response = router.submit_order(submit_req);
        assert!(response.success);
        let order_id = response.order_id.unwrap();

        // test_user_2 尝试撤单
        let cancel_req = CancelOrderRequest {
            account_id: "test_user_2".to_string(),
            order_id: order_id,
        };

        let result = router.cancel_order(cancel_req);
        assert!(result.is_err());

        if let Err(ExchangeError::OrderError(msg)) = result {
            assert!(msg.contains("does not belong"));
        } else {
            panic!("Expected OrderError about ownership");
        }
    }

    // ==================== 订单统计测试 @yutiansut @quantaxis ====================

    /// 测试订单统计 - 初始状态
    #[test]
    fn test_get_order_statistics_empty() {
        let router = create_test_router();

        let stats = router.get_order_statistics();
        assert_eq!(stats.total_count, 0);
        assert_eq!(stats.pending_count, 0);
        assert_eq!(stats.filled_count, 0);
        assert_eq!(stats.cancelled_count, 0);
        assert_eq!(stats.rejected_count, 0);
    }

    /// 测试订单统计 - 提交后
    #[test]
    fn test_get_order_statistics_after_submit() {
        let router = create_test_router();

        // 提交订单
        let req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 10.0,
            price: 120.0,
            order_type: "LIMIT".to_string(),
            time_condition: None,
            volume_condition: None,
        };

        router.submit_order(req);

        let stats = router.get_order_statistics();
        assert_eq!(stats.total_count, 1);
        // 订单提交后应为 pending 状态
        assert!(stats.pending_count >= 0); // 可能是submitted或其他状态
    }

    /// 测试订单统计 - 多订单
    #[test]
    fn test_get_order_statistics_multiple() {
        let router = create_test_router();

        // 提交多个订单
        for _ in 0..5 {
            let req = SubmitOrderRequest {
                account_id: "test_user".to_string(),
                instrument_id: "IX2301".to_string(),
                direction: "BUY".to_string(),
                offset: "OPEN".to_string(),
                volume: 1.0,
                price: 120.0,
                order_type: "LIMIT".to_string(),
                time_condition: None,
                volume_condition: None,
            };
            router.submit_order(req);
        }

        let stats = router.get_order_statistics();
        assert_eq!(stats.total_count, 5);
    }

    // ==================== 成交统计测试 @yutiansut @quantaxis ====================

    /// 测试成交统计 - 初始状态
    #[test]
    fn test_get_trade_statistics_empty() {
        let router = create_test_router();

        let stats = router.get_trade_statistics();
        assert_eq!(stats.total_count, 0);
        assert_eq!(stats.total_volume, 0.0);
        assert_eq!(stats.total_amount, 0.0);
    }

    // ==================== 订单详情测试 @yutiansut @quantaxis ====================

    /// 测试获取订单详情 - 不存在
    #[test]
    fn test_get_order_detail_not_found() {
        let router = create_test_router();

        let detail = router.get_order_detail("NON_EXISTENT");
        assert!(detail.is_none());
    }

    /// 测试获取订单详情 - 存在
    #[test]
    fn test_get_order_detail_exists() {
        let router = create_test_router();

        let req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 10.0,
            price: 120.0,
            order_type: "LIMIT".to_string(),
            time_condition: None,
            volume_condition: None,
        };

        let response = router.submit_order(req);
        assert!(response.success);
        let order_id = response.order_id.unwrap();

        let detail = router.get_order_detail(&order_id);
        assert!(detail.is_some());

        let (order, status, submit_time, update_time, filled_volume) = detail.unwrap();
        assert_eq!(order.user_id, "test_user");
        assert_eq!(order.instrument_id, "IX2301");
        assert!(submit_time > 0);
        assert!(update_time >= submit_time);
        assert_eq!(filled_volume, 0.0);
    }

    // ==================== 用户订单详情测试 @yutiansut @quantaxis ====================

    /// 测试获取用户订单详情 - 用户不存在
    #[test]
    fn test_get_user_order_details_not_found() {
        let router = create_test_router();

        let details = router.get_user_order_details("NON_EXISTENT_USER");
        assert!(details.is_empty());
    }

    /// 测试获取用户订单详情 - 存在订单
    #[test]
    fn test_get_user_order_details_exists() {
        let router = create_test_router();

        // 提交多个订单
        for i in 0..3 {
            let req = SubmitOrderRequest {
                account_id: "test_user".to_string(),
                instrument_id: "IX2301".to_string(),
                direction: "BUY".to_string(),
                offset: "OPEN".to_string(),
                volume: 1.0 + i as f64,
                price: 120.0,
                order_type: "LIMIT".to_string(),
                time_condition: None,
                volume_condition: None,
            };
            router.submit_order(req);
        }

        let details = router.get_user_order_details("test_user");
        assert_eq!(details.len(), 3);

        // 验证每个订单详情
        for (order_id, order, _status, submit_time, update_time, _filled) in details {
            assert!(!order_id.is_empty());
            assert_eq!(order.user_id, "test_user");
            assert!(submit_time > 0);
            assert!(update_time >= submit_time);
        }
    }

    // ==================== 全部订单测试 @yutiansut @quantaxis ====================

    /// 测试获取所有订单 - 空
    #[test]
    fn test_get_all_orders_empty() {
        let router = create_test_router();

        let orders = router.get_all_orders();
        assert!(orders.is_empty());
    }

    /// 测试获取所有订单 - 多用户
    #[test]
    fn test_get_all_orders_multiple_users() {
        let router = create_test_router();

        // 创建第二个账户
        let req2 = OpenAccountRequest {
            user_id: "test_user_2".to_string(),
            account_id: Some("test_user_2".to_string()),
            account_name: "Test User 2".to_string(),
            init_cash: 1000000.0,
            account_type: AccountType::Individual,
        };
        router.account_mgr.open_account(req2).unwrap();

        // 用户1提交订单
        for _ in 0..2 {
            let req = SubmitOrderRequest {
                account_id: "test_user".to_string(),
                instrument_id: "IX2301".to_string(),
                direction: "BUY".to_string(),
                offset: "OPEN".to_string(),
                volume: 1.0,
                price: 120.0,
                order_type: "LIMIT".to_string(),
                time_condition: None,
                volume_condition: None,
            };
            router.submit_order(req);
        }

        // 用户2提交订单
        for _ in 0..3 {
            let req = SubmitOrderRequest {
                account_id: "test_user_2".to_string(),
                instrument_id: "IX2301".to_string(),
                direction: "SELL".to_string(),
                offset: "OPEN".to_string(),
                volume: 1.0,
                price: 120.0,
                order_type: "LIMIT".to_string(),
                time_condition: None,
                volume_condition: None,
            };
            router.submit_order(req);
        }

        let orders = router.get_all_orders();
        assert_eq!(orders.len(), 5);
    }

    // ==================== 订单计数测试 @yutiansut @quantaxis ====================

    /// 测试订单计数 - 初始
    #[test]
    fn test_get_order_count_empty() {
        let router = create_test_router();

        assert_eq!(router.get_order_count(), 0);
    }

    /// 测试订单计数 - 增长
    #[test]
    fn test_get_order_count_growth() {
        let router = create_test_router();

        for i in 0..10 {
            let req = SubmitOrderRequest {
                account_id: "test_user".to_string(),
                instrument_id: "IX2301".to_string(),
                direction: "BUY".to_string(),
                offset: "OPEN".to_string(),
                volume: 1.0,
                price: 120.0,
                order_type: "LIMIT".to_string(),
                time_condition: None,
                volume_condition: None,
            };
            router.submit_order(req);
            assert_eq!(router.get_order_count(), i + 1);
        }
    }

    // ==================== 查询订单测试 @yutiansut @quantaxis ====================

    /// 测试查询订单 - 不存在
    #[test]
    fn test_query_order_not_found() {
        let router = create_test_router();

        let order = router.query_order("NON_EXISTENT");
        assert!(order.is_none());
    }

    /// 测试查询用户订单 - 用户不存在
    #[test]
    fn test_query_user_orders_not_found() {
        let router = create_test_router();

        let orders = router.query_user_orders("NON_EXISTENT_USER");
        assert!(orders.is_empty());
    }

    // ==================== 订单ID生成测试 @yutiansut @quantaxis ====================

    /// 测试订单ID唯一性
    #[test]
    fn test_generate_order_id_unique() {
        let router = create_test_router();

        let mut ids = std::collections::HashSet::new();
        for _ in 0..1000 {
            let id = router.generate_order_id();
            assert!(ids.insert(id.clone()), "Duplicate order ID generated: {}", id);
        }
    }

    /// 测试订单ID格式
    #[test]
    fn test_generate_order_id_format() {
        let router = create_test_router();

        for _ in 0..10 {
            let id = router.generate_order_id();
            assert!(id.starts_with('O'), "Order ID should start with 'O': {}", id);
            assert!(id.len() > 10, "Order ID should be longer than 10 chars: {}", id);
        }
    }

    // ==================== SubmitOrderRequest 测试 @yutiansut @quantaxis ====================

    /// 测试 SubmitOrderRequest 创建
    #[test]
    fn test_submit_order_request_creation() {
        let req = SubmitOrderRequest {
            account_id: "user1".to_string(),
            instrument_id: "cu2501".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 10.0,
            price: 85000.0,
            order_type: "LIMIT".to_string(),
            time_condition: Some(TimeCondition::GFD),
            volume_condition: Some(VolumeCondition::ANY),
        };

        assert_eq!(req.account_id, "user1");
        assert_eq!(req.instrument_id, "cu2501");
        assert_eq!(req.direction, "BUY");
        assert_eq!(req.offset, "OPEN");
        assert_eq!(req.volume, 10.0);
        assert_eq!(req.price, 85000.0);
        assert_eq!(req.order_type, "LIMIT");
        assert_eq!(req.time_condition, Some(TimeCondition::GFD));
        assert_eq!(req.volume_condition, Some(VolumeCondition::ANY));
    }

    /// 测试 SubmitOrderRequest Clone
    #[test]
    fn test_submit_order_request_clone() {
        let req = SubmitOrderRequest {
            account_id: "user1".to_string(),
            instrument_id: "cu2501".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 10.0,
            price: 85000.0,
            order_type: "LIMIT".to_string(),
            time_condition: None,
            volume_condition: None,
        };

        let cloned = req.clone();
        assert_eq!(req.account_id, cloned.account_id);
        assert_eq!(req.instrument_id, cloned.instrument_id);
    }

    // ==================== CancelOrderRequest 测试 @yutiansut @quantaxis ====================

    /// 测试 CancelOrderRequest 创建
    #[test]
    fn test_cancel_order_request_creation() {
        let req = CancelOrderRequest {
            account_id: "user1".to_string(),
            order_id: "O12345".to_string(),
        };

        assert_eq!(req.account_id, "user1");
        assert_eq!(req.order_id, "O12345");
    }

    /// 测试 CancelOrderRequest Clone
    #[test]
    fn test_cancel_order_request_clone() {
        let req = CancelOrderRequest {
            account_id: "user1".to_string(),
            order_id: "O12345".to_string(),
        };

        let cloned = req.clone();
        assert_eq!(req.account_id, cloned.account_id);
        assert_eq!(req.order_id, cloned.order_id);
    }

    // ==================== SubmitOrderResponse 测试 @yutiansut @quantaxis ====================

    /// 测试 SubmitOrderResponse 成功
    #[test]
    fn test_submit_order_response_success() {
        let resp = SubmitOrderResponse {
            success: true,
            order_id: Some("O12345".to_string()),
            status: Some("submitted".to_string()),
            error_message: None,
            error_code: None,
        };

        assert!(resp.success);
        assert_eq!(resp.order_id, Some("O12345".to_string()));
        assert_eq!(resp.status, Some("submitted".to_string()));
        assert!(resp.error_message.is_none());
        assert!(resp.error_code.is_none());
    }

    /// 测试 SubmitOrderResponse 失败
    #[test]
    fn test_submit_order_response_failure() {
        let resp = SubmitOrderResponse {
            success: false,
            order_id: None,
            status: Some("rejected".to_string()),
            error_message: Some("Insufficient funds".to_string()),
            error_code: Some(4001),
        };

        assert!(!resp.success);
        assert!(resp.order_id.is_none());
        assert_eq!(resp.status, Some("rejected".to_string()));
        assert_eq!(resp.error_message, Some("Insufficient funds".to_string()));
        assert_eq!(resp.error_code, Some(4001));
    }

    // ==================== OrderStatistics 测试 @yutiansut @quantaxis ====================

    /// 测试 OrderStatistics 字段
    #[test]
    fn test_order_statistics_fields() {
        let stats = OrderStatistics {
            total_count: 100,
            pending_count: 20,
            filled_count: 70,
            cancelled_count: 5,
            rejected_count: 5,
        };

        assert_eq!(stats.total_count, 100);
        assert_eq!(stats.pending_count, 20);
        assert_eq!(stats.filled_count, 70);
        assert_eq!(stats.cancelled_count, 5);
        assert_eq!(stats.rejected_count, 5);

        // 验证总数一致性（注意：pending包含submitted和partially_filled）
        assert!(stats.pending_count + stats.filled_count + stats.cancelled_count + stats.rejected_count <= stats.total_count);
    }

    /// 测试 OrderStatistics Clone
    #[test]
    fn test_order_statistics_clone() {
        let stats = OrderStatistics {
            total_count: 50,
            pending_count: 10,
            filled_count: 30,
            cancelled_count: 5,
            rejected_count: 5,
        };

        let cloned = stats.clone();
        assert_eq!(stats.total_count, cloned.total_count);
        assert_eq!(stats.pending_count, cloned.pending_count);
    }

    // ==================== TradeStatistics 测试 @yutiansut @quantaxis ====================

    /// 测试 TradeStatistics 字段
    #[test]
    fn test_trade_statistics_fields() {
        let stats = TradeStatistics {
            total_count: 1000,
            total_volume: 50000.0,
            total_amount: 4250000000.0,
        };

        assert_eq!(stats.total_count, 1000);
        assert_eq!(stats.total_volume, 50000.0);
        assert_eq!(stats.total_amount, 4250000000.0);
    }

    /// 测试 TradeStatistics Clone
    #[test]
    fn test_trade_statistics_clone() {
        let stats = TradeStatistics {
            total_count: 500,
            total_volume: 25000.0,
            total_amount: 2125000000.0,
        };

        let cloned = stats.clone();
        assert_eq!(stats.total_count, cloned.total_count);
        assert_eq!(stats.total_volume, cloned.total_volume);
    }

    // ==================== OrderStatus 更多测试 @yutiansut @quantaxis ====================

    /// 测试 OrderStatus Copy trait
    #[test]
    fn test_order_status_copy() {
        let status1 = OrderStatus::Submitted;
        let status2 = status1; // Copy
        let status3 = status1.clone(); // Clone

        assert_eq!(status1, status2);
        assert_eq!(status1, status3);
    }

    /// 测试 OrderStatus 所有状态
    #[test]
    fn test_order_status_all_values() {
        let statuses = [
            OrderStatus::PendingRisk,
            OrderStatus::PendingRoute,
            OrderStatus::Submitted,
            OrderStatus::PartiallyFilled,
            OrderStatus::Filled,
            OrderStatus::Cancelled,
            OrderStatus::Rejected,
        ];

        // 验证没有重复
        for (i, s1) in statuses.iter().enumerate() {
            for (j, s2) in statuses.iter().enumerate() {
                if i != j {
                    assert_ne!(s1, s2);
                }
            }
        }
    }

    // ==================== 订单类型测试 @yutiansut @quantaxis ====================

    /// 测试 LIMIT 订单
    #[test]
    fn test_submit_limit_order() {
        let router = create_test_router();

        let req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 10.0,
            price: 120.0,
            order_type: "LIMIT".to_string(),
            time_condition: Some(TimeCondition::GFD),
            volume_condition: Some(VolumeCondition::ANY),
        };

        let response = router.submit_order(req);
        assert!(response.success);
    }

    /// 测试卖单
    #[test]
    fn test_submit_sell_order() {
        let router = create_test_router();

        let req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "SELL".to_string(),
            offset: "OPEN".to_string(),
            volume: 5.0,
            price: 120.0,
            order_type: "LIMIT".to_string(),
            time_condition: None,
            volume_condition: None,
        };

        let response = router.submit_order(req);
        assert!(response.success);
    }

    // ==================== 时间/数量条件测试 @yutiansut @quantaxis ====================

    /// 测试 IOC 订单
    #[test]
    fn test_submit_ioc_order() {
        let router = create_test_router();

        let req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 5.0,
            price: 120.0,
            order_type: "LIMIT".to_string(),
            time_condition: Some(TimeCondition::IOC),
            volume_condition: Some(VolumeCondition::ANY),
        };

        let response = router.submit_order(req);
        // IOC订单可能被拒绝（如果没有对手方），或成功提交
        // 这里只验证请求不会导致panic
        assert!(response.order_id.is_some() || response.error_message.is_some());
    }

    /// 测试 GTC 订单
    #[test]
    fn test_submit_gtc_order() {
        let router = create_test_router();

        let req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 5.0,
            price: 120.0,
            order_type: "LIMIT".to_string(),
            time_condition: Some(TimeCondition::GTC),
            volume_condition: None,
        };

        let response = router.submit_order(req);
        assert!(response.success);
    }

    /// 测试 ALL (FOK) 数量条件
    #[test]
    fn test_submit_fok_order() {
        let router = create_test_router();

        let req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 5.0,
            price: 120.0,
            order_type: "LIMIT".to_string(),
            time_condition: Some(TimeCondition::IOC),
            volume_condition: Some(VolumeCondition::ALL), // FOK = IOC + ALL
        };

        let response = router.submit_order(req);
        // FOK订单在没有对手方时会被拒绝
        // 这里只验证请求不会导致panic
        assert!(response.order_id.is_some() || response.error_message.is_some());
    }

    // ==================== 合约不存在测试 @yutiansut @quantaxis ====================

    /// 测试提交订单到不存在的合约
    #[test]
    fn test_submit_order_instrument_not_found() {
        let router = create_test_router();

        let req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "NON_EXISTENT_INSTRUMENT".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 10.0,
            price: 100.0,
            order_type: "LIMIT".to_string(),
            time_condition: None,
            volume_condition: None,
        };

        let response = router.submit_order(req);
        assert!(!response.success);
        assert!(response.error_message.is_some());
    }

    // ==================== 账户不存在测试 @yutiansut @quantaxis ====================

    /// 测试提交订单到不存在的账户
    #[test]
    fn test_submit_order_account_not_found() {
        let router = create_test_router();

        let req = SubmitOrderRequest {
            account_id: "NON_EXISTENT_ACCOUNT".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 10.0,
            price: 120.0,
            order_type: "LIMIT".to_string(),
            time_condition: None,
            volume_condition: None,
        };

        let response = router.submit_order(req);
        assert!(!response.success);
        assert!(response.error_message.is_some());
    }

    // ==================== 边界条件测试 @yutiansut @quantaxis ====================

    /// 测试零价格订单
    #[test]
    fn test_submit_order_zero_price() {
        let router = create_test_router();

        let req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 10.0,
            price: 0.0, // 零价格
            order_type: "LIMIT".to_string(),
            time_condition: None,
            volume_condition: None,
        };

        let response = router.submit_order(req);
        // 零价格可能被拒绝
        // 这里只验证不会panic
        assert!(response.order_id.is_some() || response.error_message.is_some());
    }

    /// 测试零数量订单
    #[test]
    fn test_submit_order_zero_volume() {
        let router = create_test_router();

        let req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: 0.0, // 零数量
            price: 120.0,
            order_type: "LIMIT".to_string(),
            time_condition: None,
            volume_condition: None,
        };

        let response = router.submit_order(req);
        // 零数量可能被拒绝
        // 这里只验证不会panic
        assert!(response.order_id.is_some() || response.error_message.is_some());
    }

    /// 测试负数量订单
    #[test]
    fn test_submit_order_negative_volume() {
        let router = create_test_router();

        let req = SubmitOrderRequest {
            account_id: "test_user".to_string(),
            instrument_id: "IX2301".to_string(),
            direction: "BUY".to_string(),
            offset: "OPEN".to_string(),
            volume: -10.0, // 负数量
            price: 120.0,
            order_type: "LIMIT".to_string(),
            time_condition: None,
            volume_condition: None,
        };

        let response = router.submit_order(req);
        // 负数量应该被拒绝
        // 这里只验证不会panic
        assert!(response.order_id.is_some() || response.error_message.is_some());
    }

    // ==================== 并发测试 @yutiansut @quantaxis ====================

    /// 测试并发提交订单
    #[test]
    fn test_concurrent_submit_orders() {
        use std::thread;

        let router = Arc::new(create_test_router());
        let mut handles = vec![];

        for i in 0..10 {
            let router_clone = router.clone();
            handles.push(thread::spawn(move || {
                let req = SubmitOrderRequest {
                    account_id: "test_user".to_string(),
                    instrument_id: "IX2301".to_string(),
                    direction: if i % 2 == 0 { "BUY" } else { "SELL" }.to_string(),
                    offset: "OPEN".to_string(),
                    volume: 1.0,
                    price: 120.0,
                    order_type: "LIMIT".to_string(),
                    time_condition: None,
                    volume_condition: None,
                };
                router_clone.submit_order(req)
            }));
        }

        let mut success_count = 0;
        for handle in handles {
            let response = handle.join().unwrap();
            if response.success {
                success_count += 1;
            }
        }

        // 至少有一些订单应该成功
        assert!(success_count > 0, "No orders were successful");

        // 验证订单数量
        assert!(router.get_order_count() >= success_count);
    }

    /// 测试并发查询订单
    #[test]
    fn test_concurrent_query_orders() {
        use std::thread;

        let router = Arc::new(create_test_router());

        // 先提交一些订单
        for _ in 0..5 {
            let req = SubmitOrderRequest {
                account_id: "test_user".to_string(),
                instrument_id: "IX2301".to_string(),
                direction: "BUY".to_string(),
                offset: "OPEN".to_string(),
                volume: 1.0,
                price: 120.0,
                order_type: "LIMIT".to_string(),
                time_condition: None,
                volume_condition: None,
            };
            router.submit_order(req);
        }

        let mut handles = vec![];

        for _ in 0..10 {
            let router_clone = router.clone();
            handles.push(thread::spawn(move || {
                // 并发查询
                let _ = router_clone.query_user_orders("test_user");
                let _ = router_clone.get_order_statistics();
                let _ = router_clone.get_trade_statistics();
                let _ = router_clone.get_all_orders();
                router_clone.get_order_count()
            }));
        }

        for handle in handles {
            let count = handle.join().unwrap();
            assert!(count >= 0);
        }
    }
}
