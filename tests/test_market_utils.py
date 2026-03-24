from __future__ import annotations

import unittest

from tradingagents.market_utils import build_security_profile


class MarketUtilsTests(unittest.TestCase):
    def test_cn_a_normalization_for_szse(self) -> None:
        profile = build_security_profile("000001", "cn_a")
        self.assertEqual(profile.market_region, "cn_a")
        self.assertEqual(profile.normalized_ticker, "000001.SZ")
        self.assertEqual(profile.exchange, "SZSE")
        self.assertEqual(profile.akshare_symbol, "000001")

    def test_cn_a_normalization_for_sse(self) -> None:
        profile = build_security_profile("600519", "cn_a")
        self.assertEqual(profile.normalized_ticker, "600519.SS")
        self.assertEqual(profile.exchange, "SSE")
        self.assertEqual(profile.eastmoney_symbol, "600519.SH")

    def test_growth_board_normalization(self) -> None:
        profile = build_security_profile("300750", "cn_a")
        self.assertEqual(profile.normalized_ticker, "300750.SZ")
        self.assertEqual(profile.exchange, "SZSE")


if __name__ == "__main__":
    unittest.main()
