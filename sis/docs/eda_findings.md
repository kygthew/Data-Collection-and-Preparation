# EDA findings (generated automatically from output.db)

- Observations: **3760** trading days, **5** stocks, 2023-11-27 to 2026-11-24.
- Volume vs intraday range (Spearman): strongest for **NVDA** (rho = 0.57), weakest for **MSFT** (rho = 0.47). All p-values < 0.05: **True**.
- Mean intraday range rises from **1.75%** (lowest-volume quintile) to **3.64%** (highest-volume quintile).
- Same-day (k=0) Spearman correlation between volume and range: AAPL 0.48, AMZN 0.50, GOOGL 0.54, MSFT 0.47, NVDA 0.57.
- Lead/lag: same-day correlation is the largest in absolute terms for 5 of 5 stocks.
- Correlation is not causation: volume and volatility are both driven by news (earnings, macro releases).

## Figures
- figures/fig1_volume_vs_range_scatter.png
- figures/fig2_correlation_by_stock.png
- figures/fig3_volatility_by_volume_quintile.png
- figures/fig4_timeseries_volume_vs_volatility.png
- figures/fig5_lead_lag_heatmap.png

## Descriptive statistics
```
          close        volume  abs_return  intraday_range_pct  rel_volume  volatility_20d
count  3760.000  3.760000e+03    3755.000            3760.000    3660.000        3660.000
mean    255.141  8.322852e+07       1.483               2.415       1.016          30.869
std     112.116  1.135024e+08       1.610               1.440       0.447          14.135
min      45.500  5.855900e+06       0.000               0.443       0.230           9.409
25%     177.682  2.738039e+07       0.478               1.519       0.768          21.529
50%     222.690  3.986664e+07       1.057               2.054       0.913          27.345
75%     333.020  7.016271e+07       1.950               2.868       1.123          36.633
max     555.800  1.142269e+09      27.370              15.496       6.780         127.002
```