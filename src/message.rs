use serde::{Deserialize, Serialize};
// use chrono::NaiveDateTime;
use rust_decimal::Decimal;
use compact_str::CompactString;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct QuoteData {
    pub code: CompactString,
    pub datetime: CompactString, // NaiveDateTime,
    pub open: Decimal,
    pub target_kind_price: Decimal,
    pub trade_bid_vol_sum: i64,
    pub trade_ask_vol_sum: i64,
    pub avg_price: Decimal,
    pub close: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub amount: Decimal,
    pub amount_sum: Decimal,
    pub volume: i64,
    pub vol_sum: i64,
    pub tick_type: i32,
    pub diff_type: i32,
    pub diff_price: Decimal,
    pub diff_rate: Decimal,
    pub simtrade: i32,
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_quote_data_serde() {
        let quote = QuoteData {
            code: CompactString::from("AAPL"),
            datetime: CompactString::from("2024-03-20 10:30:00"),
            open: dec!(150.50),
            target_kind_price: dec!(151.00),
            trade_bid_vol_sum: 1000,
            trade_ask_vol_sum: 1200,
            avg_price: dec!(150.75),
            close: dec!(151.25),
            high: dec!(151.50),
            low: dec!(150.25),
            amount: dec!(1000000.00),
            amount_sum: dec!(5000000.00),
            volume: 6600,
            vol_sum: 33000,
            tick_type: 1,
            diff_type: 1,
            diff_price: dec!(0.75),
            diff_rate: dec!(0.5),
            simtrade: 0,
        };

        let encoded = rmp_serde::to_vec(&quote).unwrap();
        let decoded: QuoteData = rmp_serde::from_slice(&encoded).unwrap();
        
        assert_eq!(quote, decoded);
        assert!(encoded.len() > 0);
    }
} 