# Binance

## futures-aggTrades
- `agg_trade_id` - integer
- `price` - decimal
- `quantity` - decimal
- `first_trade_id` - integer
- `last_trade_id` - integer
- `transact_time` - integer (timestamp)
- `is_buyer_maker` - boolean

## futures-fundingRate
- `calc_time` - integer (timestamp)
- `funding_interval_hours` - integer
- `last_funding_rate` - decimal

## futures-klines
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

## futures-indexPriceKlines
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

## futures-markPriceKlines
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

## futures-metrics
- `create_time` - string (datetime)
- `symbol` - string
- `sum_open_interest` - decimal
- `sum_open_interest_value` - decimal
- `count_toptrader_long_short_ratio` - decimal
- `sum_toptrader_long_short_ratio` - decimal
- `count_long_short_ratio` - decimal
- `sum_taker_long_short_vol_ratio` - decimal

## futures-premiumIndexKlines
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

## spot-aggTrades
- `aggregate_trade_id` - integer
- `price` - decimal
- `quantity` - decimal
- `first_trade_id` - integer
- `last_trade_id` - integer
- `timestamp` - integer (timestamp)
- `was_buyer_maker` - boolean
- `was_best_price_match` - boolean

## spot-klines
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

# Reddit

## submissions
- `id` - string
- `author` - string
- `created_utc` - integer (timestamp)
- `subreddit` - string
- `link_flair_text` - string
- `title` - string
- `selftext` - string

## comments
- `id` - string
- `author` - string
- `created_utc` - integer (timestamp)
- `link_id` - string
- `body` - string
