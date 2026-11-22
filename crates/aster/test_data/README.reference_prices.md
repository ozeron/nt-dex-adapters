# Reference price fixture

`premium_index_all.json` backs `tests/reference_prices.rs`.

- Captured: 2026-11-22, about 19:32 UTC (`time` 1795375923000 ms in every `premiumIndex` row)
- Sources: `GET https://fapi.asterdex.com/fapi/v1/premiumIndex` and `GET https://fapi.asterdex.com/fapi/v1/exchangeInfo`, both public
- Content: one compact JSON array, 595 objects, one per `TRADING` `PERPETUAL` symbol. Fields: `symbol`, `markPrice`, `indexPrice` (strings exactly as `premiumIndex` returned them) and `pricePrecision` (integer from `exchangeInfo`). All other fields are dropped. Symbols that `premiumIndex` lists but `exchangeInfo` does not mark `TRADING` are left out.
- Every `markPrice` and `indexPrice` in this capture has exactly eight decimals; none is a whole number. Whole numbers (the venue sent `"1666"` for YFIUSDT in an earlier capture) are covered by unit cases in the test instead.
- Worst loss from rounding to `pricePrecision` in this capture: about 59 bps (ZILUSDT mark price). Ten values lose more than 10 bps.
