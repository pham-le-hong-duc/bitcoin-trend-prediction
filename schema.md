# CSV Schema Documentation

## futures-aggTrades.csv
- `agg_trade_id` - integer
- `price` - decimal
- `quantity` - decimal
- `first_trade_id` - integer
- `last_trade_id` - integer
- `transact_time` - integer (timestamp)
- `is_buyer_maker` - boolean

## futures-fundingRate.csv
- `calc_time` - integer (timestamp)
- `funding_interval_hours` - integer
- `last_funding_rate` - decimal

## futures-indexPriceKlines.csv
- `open_time` - integer (timestamp)
- `open` - decimal
- `high` - decimal
- `low` - decimal
- `close` - decimal
- `volume` - decimal
- `close_time` - integer (timestamp)
- `quote_volume` - decimal
- `count` - integer
- `taker_buy_volume` - decimal
- `taker_buy_quote_volume` - decimal
- `ignore` - integer

## futures-markPriceKlines.csv
- `open_time` - integer (timestamp)
- `open` - decimal
- `high` - decimal
- `low` - decimal
- `close` - decimal
- `volume` - decimal
- `close_time` - integer (timestamp)
- `quote_volume` - decimal
- `count` - integer
- `taker_buy_volume` - decimal
- `taker_buy_quote_volume` - decimal
- `ignore` - integer

## futures-metrics.csv
- `create_time` - string (datetime)
- `symbol` - string
- `sum_open_interest` - decimal
- `sum_open_interest_value` - decimal
- `count_toptrader_long_short_ratio` - decimal
- `sum_toptrader_long_short_ratio` - decimal
- `count_long_short_ratio` - decimal
- `sum_taker_long_short_vol_ratio` - decimal

## futures-premiumIndexKlines.csv
- `open_time` - integer (timestamp)
- `open` - decimal
- `high` - decimal
- `low` - decimal
- `close` - decimal
- `volume` - decimal
- `close_time` - integer (timestamp)
- `quote_volume` - decimal
- `count` - integer
- `taker_buy_volume` - decimal
- `taker_buy_quote_volume` - decimal
- `ignore` - integer

## spot-aggTrades.csv
- `aggregate_trade_id` - integer
- `price` - decimal
- `quantity` - decimal
- `first_trade_id` - integer
- `last_trade_id` - integer
- `timestamp` - integer (timestamp)
- `was_buyer_maker` - boolean
- `was_best_price_match` - boolean
