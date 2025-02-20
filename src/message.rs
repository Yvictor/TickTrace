use serde::{Deserialize, Serialize};
// use chrono::NaiveDateTime;
use rust_decimal::Decimal;
use compact_str::CompactString;

#[derive(Debug, Serialize, Deserialize, PartialEq, Default)]
pub struct QuoFOPv2 {
    pub code: CompactString,
    pub date: CompactString, // NaiveDateTime,
    pub time: CompactString, // NaiveDateTime,
    pub target_kind_price: Decimal,
    pub open: Decimal,
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
    pub trade_bid_vol_sum: i64,
    pub trade_ask_vol_sum: i64,
    pub trade_bid_cnt: i64,
    pub trade_ask_cnt: i64,
    pub bid_price: [Decimal; 5],
    pub bid_volume: [i64; 5],
    pub diff_bid_vol: [i64; 5],
    pub ask_price: [Decimal; 5],
    pub ask_volume: [i64; 5],
    pub diff_ask_vol: [i64; 5],
    pub first_derived_bid_price: Decimal,
    pub first_derived_ask_price: Decimal,
    pub first_derived_bid_volume: i64,
    pub first_derived_ask_volume: i64,
    pub simtrade: i32,
}
// QuoFOPv2 { code: "TXFC5", date: "2025/02/20", time: "10:47:12.401000", target_kind_price: 23506.66, open: 23505, avg_price: 23450.543381, close: 23464, high: 23564, low: 23371, amount: 70392, amount_sum: 819502689, volume: 0, vol_sum: 34946, tick_type: 2, diff_type: 4, diff_price: -146, diff_rate: -0.618382, trade_bid_vol_sum: 15983, trade_ask_vol_sum: 18074, trade_bid_cnt: 23662, trade_ask_cnt: 22061, bid_price: [23463, 23462, 23461, 23460, 23459], bid_volume: [2, 7, 17, 16, 12], diff_bid_vol: [0, 0, 0, 0, 0], ask_price: [23464, 23465, 23466, 23467, 23468], ask_volume: [1, 7, 9, 20, 14], diff_ask_vol: [0, 0, 0, 0, 0], first_derived_bid_price: 23459, first_derived_ask_price: 23467, first_derived_bid_volume: 1, first_derived_ask_volume: 1, simtrade: 0 }


#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TicFopV1 {
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
    fn test_quo_fop_v2_serde() {
        let quote = QuoFOPv2 {
            code: CompactString::from("AAPL"),
            date: CompactString::from("2024-03-20"),
            time: CompactString::from("10:30:00"),
            target_kind_price: dec!(151.00),
            open: dec!(150.50),
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
            trade_bid_vol_sum: 1000,
            trade_ask_vol_sum: 1200,
            trade_bid_cnt: 100,
            trade_ask_cnt: 120,
            bid_price: [dec!(151.00), dec!(150.90), dec!(150.80), dec!(150.70), dec!(150.60)],  
            bid_volume: [1000, 1000, 1000, 1000, 1000],
            diff_bid_vol: [100, 100, 100, 100, 100],
            ask_price: [dec!(151.00), dec!(150.90), dec!(150.80), dec!(150.70), dec!(150.60)],
            ask_volume: [1000, 1000, 1000, 1000, 1000],
            diff_ask_vol: [100, 100, 100, 100, 100],
            first_derived_bid_price: dec!(151.00),
            first_derived_ask_price: dec!(151.00),
            first_derived_bid_volume: 1000,
            first_derived_ask_volume: 1000,
            simtrade: 0,
        };  

        let encoded = rmp_serde::to_vec(&quote).unwrap();
        let decoded: QuoFOPv2 = rmp_serde::from_slice(&encoded).unwrap();
        
        assert_eq!(quote, decoded);
        assert!(encoded.len() > 0);
    }
        
    #[test]
    fn test_tic_fop_v1_serde() {
        let quote = TicFopV1 {
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
        let decoded: TicFopV1 = rmp_serde::from_slice(&encoded).unwrap();
        
        assert_eq!(quote, decoded);
        assert!(encoded.len() > 0);
    }
} 