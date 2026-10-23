//! Optional exchange-side fill diagnostics. Reading these is research-only: an
//! exchange fill can precede its local acknowledgement.

/// One actual execution, including partial fills. Layout matches Python's
/// `fill_record_dtype` (64 bytes). No Order ABI changes are required.
#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct FillRecord {
    pub order_id: u64,
    pub exch_ts: i64,
    pub price_tick: i64,
    pub exec_price_tick: i64,
    pub qty: f64,
    pub leaves_qty: f64,
    pub side: i64,
    pub reason: u64,
}

pub const TRADE_AT_PRICE: u64 = 1;
pub const TRADE_THROUGH: u64 = 2;
pub const BOOK_TOUCH: u64 = 3;
pub const BOOK_CROSS: u64 = 4;
pub const TAKER: u64 = 5;
