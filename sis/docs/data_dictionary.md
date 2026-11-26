# Data dictionary

Source: Financial Modeling Prep, `GET /stable/historical-price-eod/full` (one row = one stock on one trading day).

## Raw / collected fields (`data/raw_data.csv`)
| Field | Type | Kind | Description |
|---|---|---|---|
| symbol | text | categorical | Ticker: AAPL, MSFT, NVDA, AMZN, GOOGL |
| date | text `YYYY-MM-DD` | date/time | Trading day |
| open, high, low, close | float | numerical | Daily prices, USD |
| volume | integer | numerical | Shares traded that day |
| change | float | numerical | close - open (USD) |
| changePercent | float | numerical | change in % of open |
| vwap | float | numerical | Volume-weighted average price |

## Derived fields (`data/clean_data.csv` -> table `daily_prices`)
| Field | Formula | Meaning |
|---|---|---|
| log_return | ln(close_t / close_t-1) | Daily log return (NULL for the first day of each ticker and for split-suspect jumps > 35%) |
| abs_return | abs(log_return) * 100 | Volatility proxy 1, % |
| intraday_range_pct | (high - low) / close * 100 | Volatility proxy 2, % |
| log_volume | ln(volume) | |
| rel_volume | volume / mean(volume of previous 20 days) | Volume relative to the stock's own recent normal (1 = normal) |
| log_rel_volume | ln(rel_volume) | Main volume measure in the analysis |
| volume_zscore | z-score of log_volume within each ticker | |
| volatility_20d | std(log_return, 20 days) * sqrt(252) * 100 | Annualised realised volatility, % |
| direction | sign of `change` | up / down / flat |
| company_name, sector | lookup from ticker | Normalised text attributes (table `symbols`) |

## Database (`data/output.db`)
* `symbols(symbol PK, company_name, sector)`
* `daily_prices(PRIMARY KEY (symbol, trade_date), FK symbol -> symbols, CHECK constraints on prices, volume, direction)`
