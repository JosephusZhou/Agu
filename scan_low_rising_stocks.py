#!/usr/bin/env python3
"""
A股低位连涨个股扫描脚本

逻辑参考 scan_low_sectors.py：
- 低位：当前收盘价低于最近 N 日均价，默认 60 日
- 连涨：最近 M 个交易日涨跌幅均为正，默认 3 日

个股数据获取方式参考 UZI-Skill：
- A股列表：akshare.stock_info_a_code_name
- K线主源：akshare.stock_zh_a_hist（东方财富）
- K线后备：akshare.stock_zh_a_daily（新浪）
- K线后备：东方财富 push2his 直连 HTTP

缓存：SQLite，避免重复抓取全市场历史行情。
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import akshare as ak
import pandas as pd

try:
    import requests
except ImportError:  # pragma: no cover - requests 是可选 fallback
    requests = None


DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "stock_data.db")
DATA_RETENTION_DAYS = 420
REQUEST_SLEEP_SECONDS = 0.15
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/122.0 Safari/537.36"
BATCH_SIZE = 100


@contextmanager
def get_db_connection():
    """获取 SQLite 连接的上下文管理器，自动提交/回滚并关闭。"""
    conn = sqlite3.connect(DB_PATH)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def get_trade_calendar() -> set:
    """
    获取 A 股历史交易日历（新浪财经），返回 YYYYMMDD 字符串集合。
    带本地数据库缓存，每天最多刷新一次。
    """
    today = datetime.now().strftime("%Y%m%d")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS trade_calendar (
                trade_date TEXT PRIMARY KEY,
                update_date TEXT
            )
        """)
        cursor.execute("SELECT COUNT(*) FROM trade_calendar WHERE update_date = ?", (today,))
        if cursor.fetchone()[0] > 0:
            cursor.execute("SELECT trade_date FROM trade_calendar")
            return {row[0] for row in cursor.fetchall()}

    try:
        df = ak.tool_trade_date_hist_sina()
        dates = set()
        for _, row in df.iterrows():
            d = str(row.iloc[0]).replace("-", "")[:8]
            if len(d) == 8:
                dates.add(d)
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("DELETE FROM trade_calendar")
            for d in dates:
                cursor.execute("INSERT OR REPLACE INTO trade_calendar (trade_date, update_date) VALUES (?, ?)", (d, today))
        print(f"交易日历已更新，共 {len(dates)} 个交易日")
        return dates
    except Exception as e:
        print(f"获取交易日历失败: {e}，将使用工作日作为近似判断")
        return set()


def is_trade_date(date_str: str, calendar: set) -> bool:
    """判断某天是否是交易日。如果日历为空则按工作日近似。"""
    if calendar:
        return date_str in calendar
    dt = datetime.strptime(date_str, "%Y%m%d")
    return dt.weekday() < 5


def get_latest_trade_date(calendar: set) -> str:
    """获取最近一个已收盘的交易日。"""
    now = datetime.now()
    today = now.strftime("%Y%m%d")
    afternoon_close = datetime.strptime("15:00", "%H:%M").time()

    if is_trade_date(today, calendar) and now.time() >= afternoon_close:
        return today

    dt = now - timedelta(days=1)
    for _ in range(10):
        d = dt.strftime("%Y%m%d")
        if is_trade_date(d, calendar):
            return d
        dt -= timedelta(days=1)
    return today


def is_trading_time(calendar: set) -> bool:
    """检查当前是否在A股交易时间内。"""
    now = datetime.now()
    today = now.strftime("%Y%m%d")

    if not is_trade_date(today, calendar):
        return False

    current_time = now.time()
    morning_start = datetime.strptime("09:30", "%H:%M").time()
    morning_end = datetime.strptime("11:30", "%H:%M").time()
    afternoon_start = datetime.strptime("13:00", "%H:%M").time()
    afternoon_end = datetime.strptime("15:00", "%H:%M").time()
    return morning_start <= current_time <= morning_end or afternoon_start <= current_time <= afternoon_end


def init_database() -> None:
    """初始化SQLite缓存库。"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_list (
                code TEXT PRIMARY KEY,
                name TEXT,
                update_date TEXT
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS stock_history (
                code TEXT,
                trade_date TEXT,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                close_price REAL,
                volume REAL,
                amount REAL,
                change_pct REAL,
                data_source TEXT,
                is_complete INTEGER DEFAULT 1,
                update_date TEXT,
                PRIMARY KEY (code, trade_date)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS potential_stocks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                code TEXT,
                name TEXT,
                consecutive_days INTEGER,
                current_price REAL,
                avg_price REAL,
                deviation REAL,
                today_change REAL,
                volume_ratio REAL,
                amount_ratio REAL,
                is_volume_amplified INTEGER,
                is_amount_amplified INTEGER,
                data_source TEXT,
                is_complete INTEGER DEFAULT 1,
                screen_date TEXT,
                screen_time TEXT
            )
        """)

        cursor.execute("CREATE INDEX IF NOT EXISTS idx_stock_history_code_date ON stock_history (code, trade_date)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_potential_stocks_screen ON potential_stocks (screen_date, is_complete, screen_time DESC, id DESC)")

    print(f"数据库初始化完成: {DB_PATH}")


def cleanup_old_data(retention_days: int = DATA_RETENTION_DAYS) -> None:
    """清理过期缓存数据。"""
    cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime("%Y%m%d")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM stock_history WHERE trade_date < ?", (cutoff_date,))
        deleted_history = cursor.rowcount
        cursor.execute("DELETE FROM potential_stocks WHERE screen_date < ?", (cutoff_date,))
        deleted_results = cursor.rowcount
        cursor.execute("PRAGMA optimize")

    if deleted_history or deleted_results:
        print(f"已清理 {cutoff_date} 前缓存: 历史行情 {deleted_history} 条, 筛选结果 {deleted_results} 条")


def normalize_date(value) -> str:
    """把不同数据源返回的日期统一成 YYYYMMDD。"""
    if pd.isna(value):
        return ""
    return str(value).replace("-", "")[:8]


def normalize_history_df(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """统一K线字段名与数据类型。"""
    if df is None or df.empty:
        return pd.DataFrame()

    rename_map = {
        "日期": "日期",
        "date": "日期",
        "开盘": "开盘价",
        "开盘价": "开盘价",
        "open": "开盘价",
        "最高": "最高价",
        "最高价": "最高价",
        "high": "最高价",
        "最低": "最低价",
        "最低价": "最低价",
        "low": "最低价",
        "收盘": "收盘价",
        "收盘价": "收盘价",
        "close": "收盘价",
        "成交量": "成交量",
        "volume": "成交量",
        "成交额": "成交额",
        "amount": "成交额",
        "涨跌幅": "涨跌幅",
        "pctChg": "涨跌幅",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns}).copy()

    required_cols = ["日期", "开盘价", "最高价", "最低价", "收盘价"]
    if any(col not in df.columns for col in required_cols):
        return pd.DataFrame()

    for col in ["开盘价", "最高价", "最低价", "收盘价", "成交量", "成交额", "涨跌幅"]:
        if col not in df.columns:
            df[col] = 0
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["日期"] = df["日期"].apply(normalize_date)
    df = df[df["日期"].str.len() == 8]
    df = df.dropna(subset=["收盘价"]).sort_values("日期").drop_duplicates("日期", keep="last")
    df["涨跌幅"] = df["涨跌幅"].where(df["涨跌幅"].notna(), df["收盘价"].pct_change() * 100)
    df["数据来源"] = source
    return df.reset_index(drop=True)


def stock_suffix(code: str) -> str:
    """按A股代码推断交易所后缀。"""
    return "SH" if code.startswith(("5", "6", "9")) else "SZ"


def should_skip_stock(code: str, name: str) -> bool:
    """过滤退市、B股、ST、北交所、科创板等不适合本扫描逻辑的标的。"""
    name_upper = str(name).upper()
    if "退" in str(name) or name_upper.startswith("PT"):
        return True
    # B股
    if code.startswith(("2", "9")):
        return True
    # ST / *ST
    if "ST" in name_upper:
        return True
    # 北交所（8 开头）
    if code.startswith("8"):
        return True
    # 科创板（688/689 开头）
    if code.startswith(("688", "689")):
        return True
    return False


def get_cached_stock_list(date: str) -> Optional[pd.DataFrame]:
    with get_db_connection() as conn:
        df = pd.read_sql_query("SELECT code, name FROM stock_list WHERE update_date = ? ORDER BY code", conn, params=(date,))
    return df if not df.empty else None


def cache_stock_list(df: pd.DataFrame, date: str) -> None:
    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("DELETE FROM stock_list WHERE update_date != ?", (date,))
        for _, row in df.iterrows():
            cursor.execute(
                "INSERT OR REPLACE INTO stock_list (code, name, update_date) VALUES (?, ?, ?)",
                (row["code"], row["name"], date),
            )


def get_stock_list(force_refresh: bool = False) -> pd.DataFrame:
    """获取A股代码列表。"""
    today = datetime.now().strftime("%Y%m%d")
    if not force_refresh:
        cached_df = get_cached_stock_list(today)
        if cached_df is not None:
            print(f"从缓存获取到 {len(cached_df)} 只A股个股")
            return cached_df

    try:
        df = ak.stock_info_a_code_name()
    except Exception as exc:
        print(f"获取A股列表失败: {exc}")
        return pd.DataFrame(columns=["code", "name"])

    df = df.rename(columns={"代码": "code", "名称": "name"})
    df["code"] = df["code"].astype(str).str.zfill(6)
    df["name"] = df["name"].astype(str)
    df = df[["code", "name"]]
    df = df[~df.apply(lambda row: should_skip_stock(row["code"], row["name"]), axis=1)]
    df = df.drop_duplicates("code").sort_values("code").reset_index(drop=True)
    cache_stock_list(df, today)
    print(f"从网络获取到 {len(df)} 只A股个股")
    return df


def get_cached_stock_history(code: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    with get_db_connection() as conn:
        query = """
            SELECT trade_date, open_price, high_price, low_price, close_price, volume, amount,
                   change_pct, data_source, is_complete
            FROM stock_history
            WHERE code = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date
        """
        df = pd.read_sql_query(query, conn, params=(code, start_date, end_date))
    if df.empty:
        return None

    return df.rename(columns={
        "trade_date": "日期",
        "open_price": "开盘价",
        "high_price": "最高价",
        "low_price": "最低价",
        "close_price": "收盘价",
        "volume": "成交量",
        "amount": "成交额",
        "change_pct": "涨跌幅",
        "data_source": "数据来源",
        "is_complete": "数据完整性",
    })


def should_refresh_history_cache(cached_df: Optional[pd.DataFrame], target_date: str, min_rows: int, is_complete: int) -> bool:
    if cached_df is None or cached_df.empty:
        return True
    if len(cached_df) < min_rows:
        return True
    today_rows = cached_df[cached_df["日期"] == target_date]
    if today_rows.empty:
        return True
    if is_complete == 1 and not bool((today_rows["数据完整性"] == 1).all()):
        return True
    return False


def cache_stock_history(df: pd.DataFrame, code: str, is_complete: int, end_date: str = "") -> None:
    if df.empty:
        return

    today = datetime.now().strftime("%Y%m%d")
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for _, row in df.iterrows():
            row_date = row.get("日期", "")
            # 只有 end_date 当天才可能未收盘，历史数据一定是完整的
            row_complete = is_complete if row_date == end_date else 1
            cursor.execute("""
                INSERT OR REPLACE INTO stock_history
                (code, trade_date, open_price, high_price, low_price, close_price, volume, amount,
                 change_pct, data_source, is_complete, update_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                code,
                row_date,
                row.get("开盘价", 0),
                row.get("最高价", 0),
                row.get("最低价", 0),
                row.get("收盘价", 0),
                row.get("成交量", 0),
                row.get("成交额", 0),
                row.get("涨跌幅", 0),
                row.get("数据来源", "unknown"),
                row_complete,
                today,
            ))


def fetch_stock_history_from_ak_em(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    df = ak.stock_zh_a_hist(symbol=code, period="daily", start_date=start_date, end_date=end_date, adjust="qfq")
    return normalize_history_df(df, "akshare_em")


def fetch_stock_history_from_ak_sina(code: str, start_date: str, end_date: str) -> pd.DataFrame:
    symbol = ("sh" if stock_suffix(code) == "SH" else "sz") + code
    df = ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, adjust="qfq")
    if df is not None and not df.empty:
        normalized = normalize_history_df(df, "akshare_sina")
        if not normalized.empty:
            normalized = normalized[normalized["日期"] <= end_date]
        return normalized
    return pd.DataFrame()


def fetch_stock_history_from_em_http(code: str, start_date: str, end_date: str, limit: int = 500) -> pd.DataFrame:
    if requests is None:
        return pd.DataFrame()

    secid = f"1.{code}" if stock_suffix(code) == "SH" else f"0.{code}"
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
    params = {
        "secid": secid,
        "ut": "fa5fd1943c7b386f172d6893dbfba10b",
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "beg": start_date,
        "end": end_date,
        "lmt": str(limit),
    }
    response = requests.get(url, params=params, timeout=12, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    klines = (response.json().get("data") or {}).get("klines") or []
    rows = []
    for line in klines:
        parts = line.split(",")
        if len(parts) < 9:
            continue
        rows.append({
            "日期": parts[0],
            "开盘价": parts[1],
            "收盘价": parts[2],
            "最高价": parts[3],
            "最低价": parts[4],
            "成交量": parts[5],
            "成交额": parts[6],
            "涨跌幅": parts[8],
        })
    return normalize_history_df(pd.DataFrame(rows), "eastmoney_http")


def get_stock_history(
    code: str,
    days: int,
    is_complete: int = 1,
    force_refresh: bool = False,
    end_date: Optional[str] = None,
    _stats: Optional[Dict[str, int]] = None,
) -> pd.DataFrame:
    """获取单只个股历史行情，带缓存和多源fallback。"""
    today = datetime.now().strftime("%Y%m%d")
    end_date = end_date or today
    start_date = (datetime.now() - timedelta(days=max(days * 2, 180))).strftime("%Y%m%d")
    cached_df = None if force_refresh else get_cached_stock_history(code, start_date, end_date)
    if not force_refresh and not should_refresh_history_cache(cached_df, end_date, min_rows=max(days // 2, 30), is_complete=is_complete):
        if _stats is not None:
            _stats["cache"] = _stats.get("cache", 0) + 1
        return cached_df.drop(columns=["数据完整性"], errors="ignore")

    fetchers = [
        ("akshare_em", lambda: fetch_stock_history_from_ak_em(code, start_date, end_date)),
        ("akshare_sina", lambda: fetch_stock_history_from_ak_sina(code, start_date, end_date)),
        ("eastmoney_http", lambda: fetch_stock_history_from_em_http(code, start_date, end_date)),
    ]
    errors = []
    for source, fetcher in fetchers:
        try:
            df = fetcher()
            if not df.empty:
                cache_stock_history(df, code, is_complete, end_date)
                if _stats is not None:
                    _stats["network"] = _stats.get("network", 0) + 1
                return df
        except Exception as exc:
            errors.append(f"{source}: {exc}")

    if _stats is not None:
        _stats["failed"] = _stats.get("failed", 0) + 1
    if errors:
        print(f"  [失败] {code}: {'; '.join(errors)}")
    return pd.DataFrame()


def check_consecutive_rise(df: pd.DataFrame, days: int = 3) -> Tuple[bool, int]:
    """检查是否最近指定交易日连续上涨。"""
    if df is None or len(df) < days + 1:
        return False, 0

    recent_data = df.tail(days)
    consecutive_days = 0
    for _, row in recent_data.iterrows():
        change_pct = row.get("涨跌幅")
        if pd.notna(change_pct) and change_pct > 0:
            consecutive_days += 1
        else:
            consecutive_days = 0

    return consecutive_days >= days, consecutive_days


def check_low_position(df: pd.DataFrame, lookback_days: int = 60) -> Tuple[bool, float, float]:
    """检查当前价是否低于最近N日均价。"""
    if df is None or len(df) < lookback_days // 2:
        return False, 0, 0

    recent_data = df.tail(lookback_days)
    avg_price = recent_data["收盘价"].mean()
    current_price = df.iloc[-1]["收盘价"]
    if not avg_price or pd.isna(avg_price) or pd.isna(current_price):
        return False, 0, 0
    return current_price < avg_price, float(current_price), float(avg_price)


def check_metric_amplified(df: pd.DataFrame, column: str, days: int = 5, threshold: float = 1.5) -> Tuple[bool, float]:
    """检查成交量/成交额是否较近N日均值放大。"""
    if df is None or len(df) < days + 1 or column not in df.columns:
        return False, 0

    recent_avg = df.iloc[-(days + 1):-1][column].mean()
    current_value = df.iloc[-1][column]
    if not recent_avg or pd.isna(recent_avg) or pd.isna(current_value):
        return False, 0

    ratio = float(current_value) / float(recent_avg)
    return ratio >= threshold, round(ratio, 2)


def get_cached_screening_results(date: str, is_complete: int = 1) -> Optional[List[Dict]]:
    """获取当天最近一次筛选结果。"""
    with get_db_connection() as conn:
        latest_df = pd.read_sql_query(
            """
            SELECT screen_time
            FROM potential_stocks
            WHERE screen_date = ? AND is_complete = ?
            ORDER BY screen_time DESC, id DESC
            LIMIT 1
            """,
            conn,
            params=(date, is_complete),
        )
        if latest_df.empty:
            return None

        latest_time = latest_df.iloc[0]["screen_time"]
        df = pd.read_sql_query(
            """
            SELECT code, name, consecutive_days, current_price, avg_price, deviation, today_change,
                   volume_ratio, amount_ratio, is_volume_amplified, is_amount_amplified, data_source
            FROM potential_stocks
            WHERE screen_date = ? AND is_complete = ? AND screen_time = ?
            ORDER BY id DESC
            """,
            conn,
            params=(date, is_complete, latest_time),
        )

    if df.empty:
        return None

    return [format_stock_record(row) for _, row in df.iterrows()]


def format_stock_record(row) -> Dict:
    return {
        "股票代码": row["code"],
        "股票名称": row["name"],
        "连涨天数": int(row["consecutive_days"]),
        "当前价格": round(float(row["current_price"]), 2),
        "均价": round(float(row["avg_price"]), 2),
        "偏离度": round(float(row["deviation"]), 2),
        "今日涨跌幅": round(float(row["today_change"]), 2),
        "成交量比值": round(float(row["volume_ratio"]), 2),
        "成交额比值": round(float(row["amount_ratio"]), 2),
        "成交量放大": "是" if int(row["is_volume_amplified"]) else "否",
        "成交额放大": "是" if int(row["is_amount_amplified"]) else "否",
        "数据来源": row["data_source"],
    }


def _analyze_single_stock(
    code: str,
    name: str,
    history_days: int,
    min_consecutive_days: int,
    lookback_days: int,
    is_complete: int,
    force_refresh: bool,
    end_date: Optional[str],
    _stats: Optional[Dict[str, int]] = None,
) -> Optional[Dict]:
    """对单只股票执行抓取+分析，返回符合条件的结果或 None。"""
    history_df = get_stock_history(
        code,
        history_days,
        is_complete=is_complete,
        force_refresh=force_refresh,
        end_date=end_date,
        _stats=_stats,
    )
    if history_df.empty:
        return None

    is_consecutive, consecutive_days = check_consecutive_rise(history_df, min_consecutive_days)
    if not is_consecutive:
        return None

    is_low, current_price, avg_price = check_low_position(history_df, lookback_days)
    if not is_low:
        return None

    deviation = (current_price - avg_price) / avg_price * 100
    is_volume_amp, volume_ratio = check_metric_amplified(history_df, "成交量")
    is_amount_amp, amount_ratio = check_metric_amplified(history_df, "成交额")
    today_change = history_df.iloc[-1].get("涨跌幅", 0)
    data_source = history_df.iloc[-1].get("数据来源", "unknown")

    return {
        "股票代码": code,
        "股票名称": name,
        "连涨天数": consecutive_days,
        "当前价格": round(current_price, 2),
        "均价": round(avg_price, 2),
        "偏离度": round(deviation, 2),
        "今日涨跌幅": round(float(today_change), 2) if pd.notna(today_change) else 0,
        "成交量比值": volume_ratio,
        "成交额比值": amount_ratio,
        "成交量放大": "是" if is_volume_amp else "否",
        "成交额放大": "是" if is_amount_amp else "否",
        "数据来源": data_source,
    }


def _process_batch(
    batch: pd.DataFrame,
    batch_idx: int,
    total_stocks: int,
    num_batches: int,
    history_days: int,
    min_consecutive_days: int,
    lookback_days: int,
    is_complete: int,
    force_refresh: bool,
    end_date: Optional[str],
) -> List[Dict]:
    """处理一批股票，返回该批次命中的结果列表。"""
    batch_results = []
    batch_start = batch_idx * BATCH_SIZE
    batch_end = batch_start + len(batch)
    stats: Dict[str, int] = {"cache": 0, "network": 0, "failed": 0}

    t_start = datetime.now()
    print(f"[批次 {batch_idx+1}/{num_batches}] 处理 #{batch_start+1}~#{batch_end} ... 开始于 {t_start.strftime('%H:%M:%S')}")

    for _, row in batch.iterrows():
        result = _analyze_single_stock(
            row["code"], row["name"], history_days, min_consecutive_days,
            lookback_days, is_complete, force_refresh, end_date, _stats=stats,
        )
        if result is not None:
            batch_results.append(result)

        time.sleep(REQUEST_SLEEP_SECONDS)

    t_end = datetime.now()
    elapsed = (t_end - t_start).total_seconds()
    print(
        f"[批次 {batch_idx+1}/{num_batches}] 完成于 {t_end.strftime('%H:%M:%S')} (耗时 {elapsed:.1f}s) | "
        f"缓存: {stats['cache']} | 网络: {stats['network']} | "
        f"失败: {stats['failed']} | 命中: {len(batch_results)}"
    )
    return batch_results


def filter_low_rising_stocks(
    min_consecutive_days: int = 3,
    lookback_days: int = 60,
    is_complete: int = 1,
    limit: Optional[int] = None,
    force_refresh: bool = False,
    end_date: Optional[str] = None,
    max_workers: int = 2,
) -> List[Dict]:
    """
    扫描A股个股，筛选低位连涨标的。
    使用批次并发：每批 BATCH_SIZE 只股票，最多 max_workers 批并行。
    """
    stock_list = get_stock_list(force_refresh=force_refresh)
    if stock_list.empty:
        return []

    if limit is not None and limit > 0:
        stock_list = stock_list.head(limit)

    total_stocks = len(stock_list)
    history_days = lookback_days + min_consecutive_days + 10
    num_batches = (total_stocks + BATCH_SIZE - 1) // BATCH_SIZE

    print(f"\n共 {total_stocks} 只股票，分 {num_batches} 批（每批 {BATCH_SIZE} 只），并发数 {max_workers}")
    print(f"说明: 缓存=读取本地数据 | 网络=从API获取 | 命中=符合筛选条件\n")

    batches = []
    for i in range(num_batches):
        start = i * BATCH_SIZE
        end = min(start + BATCH_SIZE, total_stocks)
        batches.append((i, stock_list.iloc[start:end]))

    all_results: List[Dict] = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
        for batch_idx, batch_df in batches:
            future = executor.submit(
                _process_batch,
                batch_df, batch_idx, total_stocks, num_batches, history_days,
                min_consecutive_days, lookback_days, is_complete,
                force_refresh, end_date,
            )
            futures[future] = batch_idx

        for future in as_completed(futures):
            batch_idx = futures[future]
            try:
                batch_results = future.result()
                all_results.extend(batch_results)
            except Exception as exc:
                print(f"[批次 {batch_idx + 1}/{num_batches}] 异常: {exc}")

    return all_results


def save_results_to_db(stocks: List[Dict], is_complete: int = 1) -> None:
    if not stocks:
        return

    now = datetime.now()
    screen_date = now.strftime("%Y%m%d")
    screen_time = now.strftime("%H%M%S")

    with get_db_connection() as conn:
        cursor = conn.cursor()
        for stock in stocks:
            cursor.execute("""
                INSERT INTO potential_stocks
                (code, name, consecutive_days, current_price, avg_price, deviation, today_change,
                 volume_ratio, amount_ratio, is_volume_amplified, is_amount_amplified, data_source,
                 is_complete, screen_date, screen_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                stock["股票代码"],
                stock["股票名称"],
                stock["连涨天数"],
                stock["当前价格"],
                stock["均价"],
                stock["偏离度"],
                stock["今日涨跌幅"],
                stock["成交量比值"],
                stock["成交额比值"],
                1 if stock["成交量放大"] == "是" else 0,
                1 if stock["成交额放大"] == "是" else 0,
                stock["数据来源"],
                is_complete,
                screen_date,
                screen_time,
            ))

    print("筛选结果已保存到数据库")


def output_results(stocks: List[Dict], is_complete: int = 1, save_to_db: bool = True) -> None:
    if not stocks:
        print("\n未找到符合条件的低位连涨个股")
        return

    stocks.sort(key=lambda x: (-x["连涨天数"], x["偏离度"], -x["成交额比值"]))

    print("\n" + "=" * 120)
    print("A股低位连涨个股扫描结果")
    print("=" * 120)
    print(f"筛选时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"数据完整性: {'完整' if is_complete == 1 else '含未收盘数据'}")
    print(f"共找到 {len(stocks)} 只个股")
    print("-" * 120)
    print(f"{'代码':<8} {'名称':<12} {'连涨':<6} {'当前价':<10} {'均价':<10} {'偏离度':<9} {'涨幅':<8} {'量比':<8} {'额比':<8} {'量放大':<7} {'额放大':<7} {'来源':<14}")
    print("-" * 120)

    for stock in stocks:
        print(
            f"{stock['股票代码']:<8} "
            f"{stock['股票名称']:<12} "
            f"{stock['连涨天数']:<6} "
            f"{stock['当前价格']:<10} "
            f"{stock['均价']:<10} "
            f"{stock['偏离度']:<9} "
            f"{stock['今日涨跌幅']:<8} "
            f"{stock['成交量比值']:<8} "
            f"{stock['成交额比值']:<8} "
            f"{stock['成交量放大']:<7} "
            f"{stock['成交额放大']:<7} "
            f"{stock['数据来源']:<14}"
        )

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"potential_stocks_{timestamp}.csv"
    pd.DataFrame(stocks).to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"\n结果已保存到: {filename}")

    if save_to_db:
        save_results_to_db(stocks, is_complete=is_complete)

    volume_amplified = [s for s in stocks if s["成交量放大"] == "是"]
    amount_amplified = [s for s in stocks if s["成交额放大"] == "是"]
    print("\n" + "=" * 120)
    print("统计信息:")
    print(f"  平均连涨天数: {sum(s['连涨天数'] for s in stocks) / len(stocks):.1f} 天")
    print(f"  平均偏离度: {sum(s['偏离度'] for s in stocks) / len(stocks):.1f}%")
    print(f"  成交量放大个股: {len(volume_amplified)} 只 ({len(volume_amplified) / len(stocks) * 100:.1f}%)")
    print(f"  成交额放大个股: {len(amount_amplified)} 只 ({len(amount_amplified) / len(stocks) * 100:.1f}%)")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="扫描A股低位并连涨三天的个股")
    parser.add_argument("--days", type=int, default=3, help="连涨天数，默认 3")
    parser.add_argument("--lookback", type=int, default=60, help="低位均价回看交易日，默认 60")
    parser.add_argument("--limit", type=int, default=None, help="仅扫描前 N 只股票，用于调试")
    parser.add_argument("--include-intraday", action="store_true", help="交易时间内也纳入当天未收盘数据")
    parser.add_argument("--force-refresh", action="store_true", help="忽略缓存，强制重新抓取")
    parser.add_argument("--no-cache-result", action="store_true", help="不复用当天完整筛选结果")
    parser.add_argument("--workers", type=int, default=2, help="并发批次数，默认 2（受 API 频率限制不宜过高）")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    print("A股低位连涨个股扫描脚本")
    print(f"筛选条件：低位（当前价格低于{args.lookback}日均价）、连涨{args.days}天")
    print(f"已过滤：ST/*ST、北交所(8开头)、科创板(688/689开头)、B股、退市股")
    print("=" * 120)

    init_database()
    cleanup_old_data()

    # 获取交易日历
    trade_calendar = get_trade_calendar()
    target_date = get_latest_trade_date(trade_calendar)
    today = datetime.now().strftime("%Y%m%d")

    is_complete = 1
    if not is_trade_date(today, trade_calendar):
        print(f"\n今天 ({today}) 非交易日，使用最近交易日 {target_date} 的数据")
    elif is_trading_time(trade_calendar):
        print("\n当前处于交易时间内，当天数据可能不完整。")
        if args.include_intraday:
            is_complete = 0
            target_date = today
            print("已选择包含未收盘数据")
        else:
            print(f"未指定 --include-intraday，使用最近完整交易日 {target_date} 的数据")
    else:
        print(f"\n当前非交易时间，使用最近完整交易日 {target_date} 的数据")

    if is_complete == 1 and not args.force_refresh and not args.no_cache_result:
        cached_results = get_cached_screening_results(target_date, is_complete=1)
        if cached_results is not None:
            print(f"\n检测到 {target_date} 已有完整筛选结果，直接复用缓存结果")
            output_results(cached_results, is_complete=1, save_to_db=False)
            return

    stocks = filter_low_rising_stocks(
        min_consecutive_days=args.days,
        lookback_days=args.lookback,
        is_complete=is_complete,
        limit=args.limit,
        force_refresh=args.force_refresh,
        end_date=target_date,
        max_workers=args.workers,
    )
    output_results(stocks, is_complete=is_complete)


if __name__ == "__main__":
    main()
