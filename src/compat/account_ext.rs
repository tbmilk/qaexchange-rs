//! QA_Account 的 qars2 兼容扩展方法（最小实现，基于 qapro-rs 公开字段重建）
//!
//! 语义依据：qapro-rs `send_order`/`order_check` 下单时 `money -= frozen` 并写入
//! `frozen` map；成交路径 `receive_deal*` 做 `money += frozen.money; frozen.remove`。
//! 撤单释放逻辑与成交释放对称。

use qars::qaaccount::account::QA_Account;
use qars::qaprotocol::qifi::account::Order;

pub trait AccountQars2Ext {
    /// 入金：增加可用资金，累计入金记录
    fn deposit(&mut self, amount: f64);
    /// 出金：减少可用资金，累计出金记录
    fn withdraw(&mut self, amount: f64);
    /// 挂单冻结保证金合计（动态保证金 = get_margin() + get_frozen_margin()）
    fn get_frozen_margin(&self) -> f64;
    /// 订单确认回报：置 ALIVE 并记录交易所订单号
    fn on_order_confirm(&mut self, order_id: &str, exchange_order_id: &str)
        -> Result<(), String>;
    /// 撤单：释放冻结资金，订单置终态，返回被撤订单副本
    fn cancel_order(&mut self, order_id: &str) -> Result<Order, String>;
}

impl AccountQars2Ext for QA_Account {
    fn deposit(&mut self, amount: f64) {
        self.money += amount;
        self.accounts.deposit += amount;
    }

    fn withdraw(&mut self, amount: f64) {
        self.money -= amount;
        self.accounts.withdraw += amount;
    }

    fn get_frozen_margin(&self) -> f64 {
        self.frozen.values().map(|f| f.money).sum()
    }

    fn on_order_confirm(
        &mut self,
        order_id: &str,
        exchange_order_id: &str,
    ) -> Result<(), String> {
        match self.dailyorders.get_mut(order_id) {
            Some(order) => {
                order.status = "ALIVE".to_string();
                order.exchange_order_id = exchange_order_id.to_string();
                Ok(())
            }
            None => Err(format!("order {} not found", order_id)),
        }
    }

    fn cancel_order(&mut self, order_id: &str) -> Result<Order, String> {
        // 释放冻结资金（与成交释放对称：money += frozen.money）
        if let Some(frozen) = self.frozen.remove(order_id) {
            self.money += frozen.money;
        }
        match self.dailyorders.get_mut(order_id) {
            Some(order) => {
                order.cancel();
                order.volume_left = 0.0;
                Ok(order.clone())
            }
            None => Err(format!("order {} not found", order_id)),
        }
    }
}

/// QAOrder（qaaccount 层）→ QIFI Order（协议层）转换
///
/// qars2 中两者可直接互换；qars3 谱系分离为两个类型，此处按字段一一映射。
pub fn qaorder_to_qifi(o: &qars::qaaccount::order::QAOrder) -> Order {
    Order {
        seqno: 0,
        user_id: o.user_id.clone(),
        order_id: o.order_id.clone(),
        exchange_id: o.exchange_id.clone(),
        instrument_id: o.instrument_id.clone(),
        direction: o.direction.clone(),
        offset: o.offset.clone(),
        volume_orign: o.volume,
        price_type: o.price_type.clone(),
        limit_price: o.limit_price,
        time_condition: o.time_condition.clone(),
        volume_condition: o.volume_condition.clone(),
        insert_date_time: 0,
        exchange_order_id: o.exchange_order_id.clone(),
        status: "ALIVE".to_string(),
        volume_left: o.volume_left,
        last_msg: o.last_msg.clone(),
    }
}
