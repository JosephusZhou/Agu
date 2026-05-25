#!/usr/bin/env python3
"""
补充个股历史行情到数据库。

用法：
    python fetch_stock_history.py 600519 000858 002304
    python fetch_stock_history.py 600519 --days 120 --force
"""

from __future__ import annotations

import argparse
import sys
import time

from scan_low_rising_stocks import (
    init_database,
    get_stock_history,
    get_trade_calendar,
    get_latest_trade_date,
    REQUEST_SLEEP_SECONDS,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="补充个股历史行情到数据库")
    parser.add_argument("codes", nargs="+", help="股票代码，支持多个，如 600519 000858")
    parser.add_argument("--days", type=int, default=120, help="拉取历史天数，默认 120")
    parser.add_argument("--force", action="store_true", help="忽略缓存，强制重新抓取")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    codes = [c.zfill(6) for c in args.codes]

    init_database()
    trade_calendar = get_trade_calendar()
    target_date = get_latest_trade_date(trade_calendar)

    print(f"目标日期: {target_date}，拉取天数: {args.days}，强制刷新: {args.force}")
    print(f"待处理股票: {', '.join(codes)}\n")

    for code in codes:
        print(f"[{code}] 拉取历史行情...")
        df = get_stock_history(
            code,
            days=args.days,
            is_complete=1,
            force_refresh=args.force,
            end_date=target_date,
        )
        if df.empty:
            print(f"[{code}] 未获取到数据")
        else:
            latest = df.iloc[-1]
            print(f"[{code}] 已入库 {len(df)} 条，最新日期 {latest['日期']}，收盘价 {latest['收盘价']}")
        time.sleep(REQUEST_SLEEP_SECONDS)

    print("\n完成")


if __name__ == "__main__":
    main()
