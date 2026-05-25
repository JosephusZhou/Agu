#!/usr/bin/env python3
"""
A股潜力板块筛选脚本
功能：获取每天收盘后A股市场的潜力板块（低位、连涨3天）
数据源：akshare（同花顺）
缓存：SQLite数据库
"""

import akshare as ak
import pandas as pd
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import List, Dict, Tuple, Optional
import os


# 数据库路径
DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sector_data.db")
DATA_RETENTION_DAYS = 360


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
    """
    检查当前是否在A股交易时间内。
    使用真实交易日历判断，而非仅看周几。
    """
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


def is_data_complete_for_today() -> bool:
    """检查今天的数据是否已经完整（已收盘）"""
    today = datetime.now().strftime("%Y%m%d")

    with get_db_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 1 FROM sector_list
            WHERE update_date = ? AND is_complete = 1
            LIMIT 1
        """, (today,))
        return cursor.fetchone() is not None


def get_cached_screening_results(date: str, is_complete: int = 1) -> Optional[Tuple[List[Dict], List[Dict]]]:
    """获取当天最近一次筛选结果，避免重复计算"""
    with get_db_connection() as conn:
        latest_time_query = """
            SELECT screen_time
            FROM potential_sectors
            WHERE screen_date = ? AND is_complete = ?
            ORDER BY screen_time DESC, id DESC
            LIMIT 1
        """
        latest_time_df = pd.read_sql_query(latest_time_query, conn, params=(date, is_complete))

        if latest_time_df.empty:
            return None

        latest_screen_time = latest_time_df.iloc[0]["screen_time"]
        query = """
            SELECT sector_name, sector_type, sector_code, data_source, consecutive_days,
                   current_price, avg_price_60d, deviation, today_change, leading_stock,
                   leading_stock_change, volume_ratio, turnover_ratio,
                   is_volume_amplified, is_turnover_amplified
            FROM potential_sectors
            WHERE screen_date = ? AND is_complete = ? AND screen_time = ?
            ORDER BY id DESC
        """
        df = pd.read_sql_query(query, conn, params=(date, is_complete, latest_screen_time))

    if df.empty:
        return None

    def format_records(records: pd.DataFrame) -> List[Dict]:
        results = []
        for _, row in records.iterrows():
            results.append({
                "板块名称": row["sector_name"],
                "板块代码": row["sector_code"],
                "数据来源": row["data_source"],
                "连涨天数": int(row["consecutive_days"]),
                "当前价格": round(row["current_price"], 2),
                "60日均价": round(row["avg_price_60d"], 2),
                "偏离度": round(row["deviation"], 2),
                "今日涨跌幅": round(row["today_change"], 2),
                "领涨股票": row["leading_stock"],
                "领涨股票涨跌幅": round(row["leading_stock_change"], 2),
                "板块类型": row["sector_type"],
                "成交量比值": round(row["volume_ratio"], 2),
                "换手率比值": round(row["turnover_ratio"], 2),
                "成交量放大": "是" if row["is_volume_amplified"] else "否",
                "换手率放大": "是" if row["is_turnover_amplified"] else "否"
            })
        return results

    industry_df = df[df["sector_type"] == "行业"]
    concept_df = df[df["sector_type"] == "概念"]
    return format_records(industry_df), format_records(concept_df)


def get_user_choice_for_incomplete_data() -> bool:
    """让用户选择是否包含当天未收盘数据"""
    print("\n" + "="*60)
    print("⚠️  当前处于交易时间内")
    print("="*60)
    print("当天数据尚未收盘，可能不完整。")
    print("请选择：")
    print("  1. 包含当天未收盘数据（后续可覆盖更新）")
    print("  2. 跳过今天，只使用已收盘的历史数据")
    print("="*60)
    
    while True:
        choice = input("请输入选项 (1 或 2): ").strip()
        if choice == "1":
            print("✓ 已选择包含当天未收盘数据")
            return True
        elif choice == "2":
            print("✓ 已选择跳过今天的数据")
            return False
        else:
            print("无效输入，请输入 1 或 2")


def init_database():
    """初始化SQLite数据库"""
    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sector_list (
                sector_name TEXT,
                sector_type TEXT,
                sector_code TEXT,
                today_change REAL,
                leading_stock TEXT,
                leading_stock_change REAL,
                data_source TEXT,
                is_complete INTEGER DEFAULT 1,
                update_date TEXT,
                PRIMARY KEY (sector_name, sector_type, update_date)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sector_history (
                sector_name TEXT,
                sector_type TEXT,
                trade_date TEXT,
                open_price REAL,
                high_price REAL,
                low_price REAL,
                close_price REAL,
                volume REAL,
                amount REAL,
                change_pct REAL,
                is_complete INTEGER DEFAULT 1,
                update_date TEXT,
                PRIMARY KEY (sector_name, sector_type, trade_date)
            )
        """)

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS potential_sectors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sector_name TEXT,
                sector_type TEXT,
                sector_code TEXT,
                data_source TEXT,
                consecutive_days INTEGER,
                current_price REAL,
                avg_price_60d REAL,
                deviation REAL,
                today_change REAL,
                leading_stock TEXT,
                leading_stock_change REAL,
                volume_ratio REAL,
                turnover_ratio REAL,
                is_volume_amplified INTEGER,
                is_turnover_amplified INTEGER,
                is_complete INTEGER DEFAULT 1,
                screen_date TEXT,
                screen_time TEXT
            )
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_sector_list_lookup
            ON sector_list (sector_type, update_date, is_complete)
        """)

        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_potential_sectors_screen_lookup
            ON potential_sectors (screen_date, is_complete, screen_time DESC, id DESC)
        """)

    print(f"数据库初始化完成: {DB_PATH}")


def cleanup_old_data(retention_days: int = DATA_RETENTION_DAYS):
    """清理超出保留期限的历史数据，控制数据库体积。"""
    cutoff_date = (datetime.now() - timedelta(days=retention_days)).strftime("%Y%m%d")
    with get_db_connection() as conn:
        cursor = conn.cursor()

        cursor.execute("DELETE FROM sector_history WHERE trade_date < ?", (cutoff_date,))
        deleted_history = cursor.rowcount

        cursor.execute("DELETE FROM sector_list WHERE update_date < ?", (cutoff_date,))
        deleted_sector_list = cursor.rowcount

        cursor.execute("DELETE FROM potential_sectors WHERE screen_date < ?", (cutoff_date,))
        deleted_potential = cursor.rowcount

        cursor.execute("PRAGMA optimize")

    total_deleted = deleted_history + deleted_sector_list + deleted_potential
    if total_deleted > 0:
        print(
            f"已清理 {cutoff_date} 之前的数据: "
            f"sector_history {deleted_history} 条, "
            f"sector_list {deleted_sector_list} 条, "
            f"potential_sectors {deleted_potential} 条"
        )


def get_cached_sector_list(sector_type: str, date: str, is_complete: int = 1) -> Optional[pd.DataFrame]:
    """从缓存获取板块列表"""
    with get_db_connection() as conn:
        query = """
            SELECT sector_name, sector_code, today_change, leading_stock, leading_stock_change, data_source, is_complete
            FROM sector_list
            WHERE sector_type = ? AND update_date = ? AND is_complete = ?
        """
        df = pd.read_sql_query(query, conn, params=(sector_type, date, is_complete))

    if not df.empty:
        df = df.rename(columns={
            "sector_name": "板块名称",
            "sector_code": "板块代码",
            "today_change": "今日涨跌幅",
            "leading_stock": "领涨股票",
            "leading_stock_change": "领涨股票涨跌幅",
            "data_source": "数据来源",
            "is_complete": "数据完整性"
        })
        return df
    return None


def cache_sector_list(df: pd.DataFrame, sector_type: str, date: str, is_complete: int = 1):
    """缓存板块列表"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO sector_list
                (sector_name, sector_type, sector_code, today_change, leading_stock, leading_stock_change, data_source, is_complete, update_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                row.get("板块名称", ""),
                sector_type,
                row.get("板块代码", ""),
                row.get("今日涨跌幅", 0),
                row.get("领涨股票", ""),
                row.get("领涨股票涨跌幅", 0),
                row.get("数据来源", "同花顺"),
                is_complete,
                date
            ))


def get_cached_sector_history(sector_name: str, sector_type: str, start_date: str, end_date: str) -> Optional[pd.DataFrame]:
    """从缓存获取板块历史行情"""
    with get_db_connection() as conn:
        query = """
            SELECT trade_date, open_price, high_price, low_price, close_price, volume, amount, change_pct, is_complete
            FROM sector_history
            WHERE sector_name = ? AND sector_type = ? AND trade_date >= ? AND trade_date <= ?
            ORDER BY trade_date
        """
        df = pd.read_sql_query(query, conn, params=(sector_name, sector_type, start_date, end_date))

    if not df.empty:
        df = df.rename(columns={
            "trade_date": "日期",
            "open_price": "开盘价",
            "high_price": "最高价",
            "low_price": "最低价",
            "close_price": "收盘价",
            "volume": "成交量",
            "amount": "成交额",
            "change_pct": "涨跌幅",
            "is_complete": "数据完整性"
        })
        return df
    return None


def should_refresh_history_cache(cached_df: Optional[pd.DataFrame], today: str, is_complete: int) -> bool:
    """完整模式下，如果今天这条历史数据仍是不完整缓存，则强制刷新。"""
    if cached_df is None or cached_df.empty:
        return True

    if len(cached_df) < 5:
        return True

    if is_complete != 1:
        return False

    today_rows = cached_df[cached_df["日期"] == today]
    if today_rows.empty:
        return True

    return not bool((today_rows["数据完整性"] == 1).all())


def cache_sector_history(df: pd.DataFrame, sector_name: str, sector_type: str, date: str, is_complete: int = 1):
    """缓存板块历史行情"""
    with get_db_connection() as conn:
        cursor = conn.cursor()
        for _, row in df.iterrows():
            cursor.execute("""
                INSERT OR REPLACE INTO sector_history
                (sector_name, sector_type, trade_date, open_price, high_price, low_price, close_price, volume, amount, change_pct, is_complete, update_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sector_name,
                sector_type,
                str(row.get("日期", "")),
                row.get("开盘价", 0),
                row.get("最高价", 0),
                row.get("最低价", 0),
                row.get("收盘价", 0),
                row.get("成交量", 0),
                row.get("成交额", 0),
                row.get("涨跌幅", 0),
                is_complete,
                date
            ))


def get_sector_list(sector_type: str = "industry", is_complete: int = 1) -> pd.DataFrame:
    """
    获取板块列表（带缓存）
    """
    today = datetime.now().strftime("%Y%m%d")
    
    # 尝试从缓存获取（完整数据）
    cached_df = get_cached_sector_list(sector_type, today, is_complete=1)
    if cached_df is not None:
        print(f"从缓存获取到 {len(cached_df)} 个{('行业' if sector_type == 'industry' else '概念')}板块（完整数据）")
        return cached_df
    
    # 如果用户选择包含不完整数据，尝试获取不完整数据
    if is_complete == 0:
        cached_df = get_cached_sector_list(sector_type, today, is_complete=0)
        if cached_df is not None:
            print(f"从缓存获取到 {len(cached_df)} 个{('行业' if sector_type == 'industry' else '概念')}板块（含未收盘数据）")
            return cached_df
    
    # 从网络获取
    try:
        if sector_type == "industry":
            df = ak.stock_board_industry_summary_ths()
            df = df.rename(columns={
                "板块": "板块名称",
                "涨跌幅": "今日涨跌幅",
                "领涨股": "领涨股票",
                "领涨股-涨跌幅": "领涨股票涨跌幅"
            })
            df["数据来源"] = "同花顺"
        else:
            df = ak.stock_board_concept_name_ths()
            df = df.rename(columns={"name": "板块名称", "code": "板块代码"})
            df["今日涨跌幅"] = 0
            df["领涨股票"] = ""
            df["领涨股票涨跌幅"] = 0
            df["数据来源"] = "同花顺"
        
        print(f"从网络获取到 {len(df)} 个{('行业' if sector_type == 'industry' else '概念')}板块")
        
        # 缓存数据
        cache_sector_list(df, sector_type, today, is_complete)
        
        return df
    except Exception as e:
        print(f"获取{('行业' if sector_type == 'industry' else '概念')}板块列表失败: {e}")
        return pd.DataFrame()


def get_sector_history(sector_name: str, sector_type: str = "industry",
                      days: int = 30, is_complete: int = 1,
                      target_date: Optional[str] = None) -> pd.DataFrame:
    """
    获取板块历史行情数据（带缓存）
    target_date: 数据截止的目标交易日（YYYYMMDD），由调用方传入以避免重复计算
    """
    today = target_date or datetime.now().strftime("%Y%m%d")
    start_date = (datetime.now() - timedelta(days=days*2)).strftime("%Y%m%d")
    
    # 尝试从缓存获取
    cached_df = get_cached_sector_history(sector_name, sector_type, start_date, today)
    if not should_refresh_history_cache(cached_df, today, is_complete):
        cached_df = cached_df.drop(columns=["数据完整性"], errors="ignore")
        return cached_df
    
    # 从网络获取
    try:
        end_date = today
        
        if sector_type == "industry":
            df = ak.stock_board_industry_index_ths(
                symbol=sector_name,
                start_date=start_date,
                end_date=end_date
            )
        else:
            df = ak.stock_board_concept_index_ths(
                symbol=sector_name,
                start_date=start_date,
                end_date=end_date
            )
        
        if df is not None and not df.empty:
            df = df.sort_values("日期", ascending=True).reset_index(drop=True)
            df["涨跌幅"] = df["收盘价"].pct_change() * 100
            
            # 缓存数据
            cache_sector_history(df, sector_name, sector_type, today, is_complete)
            
            return df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        print(f"获取 {sector_name} 历史数据失败: {e}")
        return pd.DataFrame()


def check_consecutive_rise(df: pd.DataFrame, days: int = 3) -> Tuple[bool, int]:
    """检查是否连涨指定天数"""
    if df is None or len(df) < days + 1:
        return False, 0
    
    recent_data = df.tail(days)
    consecutive_days = 0
    
    for _, row in recent_data.iterrows():
        if pd.notna(row["涨跌幅"]) and row["涨跌幅"] > 0:
            consecutive_days += 1
        else:
            consecutive_days = 0
    
    return consecutive_days >= days, consecutive_days


def check_low_position(df: pd.DataFrame, lookback_days: int = 60) -> Tuple[bool, float, float]:
    """
    检查是否处于低位（当前价格低于历史均值）
    lookback_days: 回看天数，即用来计算均价的时间窗口
    """
    if df is None or len(df) < lookback_days // 2:
        return False, 0, 0
    
    recent_data = df.tail(lookback_days)
    avg_price = recent_data["收盘价"].mean()
    current_price = df.iloc[-1]["收盘价"]
    is_low = current_price < avg_price
    
    return is_low, current_price, avg_price


def check_volume_amplified(df: pd.DataFrame, days: int = 5, threshold: float = 1.5) -> Tuple[bool, float]:
    """
    检查成交量是否放大
    
    Args:
        df: 历史行情数据
        days: 近期天数
        threshold: 放大阈值（当前成交量 / 近期平均成交量）
    
    Returns:
        (是否放大, 成交量比值)
    """
    if df is None or len(df) < days + 1:
        return False, 0
    
    # 计算近期平均成交量（排除今天）
    recent_avg_volume = df.iloc[-(days+1):-1]["成交量"].mean()
    
    if recent_avg_volume == 0:
        return False, 0
    
    # 今天的成交量
    current_volume = df.iloc[-1]["成交量"]
    
    # 计算比值
    volume_ratio = current_volume / recent_avg_volume
    
    return volume_ratio >= threshold, round(volume_ratio, 2)


def check_turnover_amplified(df: pd.DataFrame, days: int = 5, threshold: float = 1.5) -> Tuple[bool, float]:
    """
    检查换手率是否放大（使用成交额作为代理指标）
    
    Args:
        df: 历史行情数据
        days: 近期天数
        threshold: 放大阈值
    
    Returns:
        (是否放大, 换手率比值)
    """
    if df is None or len(df) < days + 1:
        return False, 0
    
    # 计算近期平均成交额（排除今天）
    recent_avg_amount = df.iloc[-(days+1):-1]["成交额"].mean()
    
    if recent_avg_amount == 0:
        return False, 0
    
    # 今天的成交额
    current_amount = df.iloc[-1]["成交额"]
    
    # 计算比值
    turnover_ratio = current_amount / recent_avg_amount
    
    return turnover_ratio >= threshold, round(turnover_ratio, 2)


def filter_potential_sectors(sector_type: str = "industry",
                           min_consecutive_days: int = 3,
                           lookback_days: int = 60,
                           is_complete: int = 1,
                           target_date: Optional[str] = None) -> List[Dict]:
    """
    筛选潜力板块
    target_date: 数据截止的目标交易日
    """
    print(f"\n开始筛选{('行业' if sector_type == 'industry' else '概念')}板块...")

    sector_list = get_sector_list(sector_type, is_complete)
    if sector_list.empty:
        return []

    potential_sectors = []
    total = len(sector_list)

    for idx, row in sector_list.iterrows():
        sector_name = row["板块名称"]
        sector_code = row.get("板块代码", "")
        today_change = row.get("今日涨跌幅", 0)

        if today_change is not None and today_change < 0:
            continue

        if (idx + 1) % 10 == 0 or idx == 0:
            print(f"处理进度: {idx + 1}/{total} - {sector_name}")

        history_df = get_sector_history(sector_name, sector_type, lookback_days + 10, is_complete, target_date=target_date)
        
        if history_df.empty:
            continue
        
        # 检查连涨
        is_consecutive, consecutive_days = check_consecutive_rise(history_df, min_consecutive_days)
        
        if not is_consecutive:
            continue
        
        # 检查低位
        is_low, current_price, avg_price = check_low_position(history_df, lookback_days)
        
        if not is_low:
            continue
        
        # 计算偏离度
        deviation = (current_price - avg_price) / avg_price * 100
        
        # 检查成交量放大
        is_volume_amp, volume_ratio = check_volume_amplified(history_df)
        
        # 检查换手率放大
        is_turnover_amp, turnover_ratio = check_turnover_amplified(history_df)
        
        leading_stock = row.get("领涨股票", "")
        leading_stock_change = row.get("领涨股票涨跌幅", 0)
        data_source = row.get("数据来源", "同花顺")
        
        potential_sectors.append({
            "板块名称": sector_name,
            "板块代码": sector_code,
            "数据来源": data_source,
            "连涨天数": consecutive_days,
            "当前价格": round(current_price, 2),
            "60日均价": round(avg_price, 2),
            "偏离度": round(deviation, 2),
            "今日涨跌幅": round(today_change, 2) if today_change else 0,
            "领涨股票": leading_stock,
            "领涨股票涨跌幅": round(leading_stock_change, 2) if leading_stock_change else 0,
            "板块类型": "行业" if sector_type == "industry" else "概念",
            "成交量比值": volume_ratio,
            "换手率比值": turnover_ratio,
            "成交量放大": "是" if is_volume_amp else "否",
            "换手率放大": "是" if is_turnover_amp else "否"
        })
        
        time.sleep(0.5)
    
    return potential_sectors


def save_results_to_db(sectors: List[Dict], is_complete: int = 1):
    """保存筛选结果到数据库"""
    if not sectors:
        return

    now = datetime.now()
    screen_date = now.strftime("%Y%m%d")
    screen_time = now.strftime("%H%M%S")

    with get_db_connection() as conn:
        cursor = conn.cursor()
        for sector in sectors:
            cursor.execute("""
                INSERT INTO potential_sectors
                (sector_name, sector_type, sector_code, data_source, consecutive_days, current_price,
                 avg_price_60d, deviation, today_change, leading_stock, leading_stock_change,
                 volume_ratio, turnover_ratio, is_volume_amplified, is_turnover_amplified,
                 is_complete, screen_date, screen_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                sector["板块名称"],
                sector["板块类型"],
                sector["板块代码"],
                sector["数据来源"],
                sector["连涨天数"],
                sector["当前价格"],
                sector["60日均价"],
                sector["偏离度"],
                sector["今日涨跌幅"],
                sector["领涨股票"],
                sector["领涨股票涨跌幅"],
                sector["成交量比值"],
                sector["换手率比值"],
                1 if sector["成交量放大"] == "是" else 0,
                1 if sector["换手率放大"] == "是" else 0,
                is_complete,
                screen_date,
                screen_time
            ))

    print(f"筛选结果已保存到数据库")


def output_results(industry_sectors: List[Dict], concept_sectors: List[Dict], is_complete: int = 1,
                   save_to_db: bool = True):
    """输出筛选结果"""
    all_sectors = industry_sectors + concept_sectors
    
    if not all_sectors:
        print("\n未找到符合条件的潜力板块")
        return
    
    # 按连涨天数和偏离度排序
    all_sectors.sort(key=lambda x: (-x["连涨天数"], x["偏离度"]))
    
    print("\n" + "="*100)
    print("A股潜力板块筛选结果（低位、连涨3天）")
    print("="*100)
    print(f"筛选时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"数据完整性: {'完整' if is_complete == 1 else '⚠️ 含未收盘数据'}")
    print(f"共找到 {len(all_sectors)} 个潜力板块")
    print(f"  - 行业板块: {len(industry_sectors)} 个")
    print(f"  - 概念板块: {len(concept_sectors)} 个")
    print("="*100)
    
    # 按板块类型分组显示
    for sector_type in ["行业", "概念"]:
        type_sectors = [s for s in all_sectors if s["板块类型"] == sector_type]
        if not type_sectors:
            continue
        
        print(f"\n【{sector_type}板块】潜力板块:")
        print("-"*130)
        print(f"{'板块名称':<12} {'板块代码(来源)':<16} {'连涨':<6} {'当前价格':<10} {'均价':<10} {'偏离度':<8} {'涨幅':<8} {'量比':<8} {'换手比':<8} {'量放大':<6} {'换放大':<6} {'领涨股':<10}")
        print("-"*130)
        
        for sector in type_sectors:
            code_with_source = f"{sector['板块代码']}({sector['数据来源']})"
            print(f"{sector['板块名称']:<12} "
                  f"{code_with_source:<16} "
                  f"{sector['连涨天数']:<6} "
                  f"{sector['当前价格']:<10} "
                  f"{sector['60日均价']:<10} "
                  f"{sector['偏离度']:<8} "
                  f"{sector['今日涨跌幅']:<8} "
                  f"{sector['成交量比值']:<8} "
                  f"{sector['换手率比值']:<8} "
                  f"{sector['成交量放大']:<6} "
                  f"{sector['换手率放大']:<6} "
                  f"{sector['领涨股票']:<10}")
    
    # 保存到CSV文件
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"potential_sectors_{timestamp}.csv"
    
    df = pd.DataFrame(all_sectors)
    df.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"\n结果已保存到: {filename}")
    
    # 保存到数据库
    if save_to_db:
        save_results_to_db(all_sectors, is_complete)
    
    # 显示统计信息
    print("\n" + "="*100)
    print("统计信息:")
    print(f"  平均连涨天数: {sum(s['连涨天数'] for s in all_sectors) / len(all_sectors):.1f} 天")
    print(f"  平均偏离度: {sum(s['偏离度'] for s in all_sectors) / len(all_sectors):.1f}%")
    
    # 成交量放大统计
    volume_amplified = [s for s in all_sectors if s["成交量放大"] == "是"]
    print(f"  成交量放大板块: {len(volume_amplified)} 个 ({len(volume_amplified)/len(all_sectors)*100:.1f}%)")
    
    # 换手率放大统计
    turnover_amplified = [s for s in all_sectors if s["换手率放大"] == "是"]
    print(f"  换手率放大板块: {len(turnover_amplified)} 个 ({len(turnover_amplified)/len(all_sectors)*100:.1f}%)")
    
    # 显示连涨天数最多的前3个板块
    top3 = sorted(all_sectors, key=lambda x: -x["连涨天数"])[:3]
    print("\n连涨天数最多的板块:")
    for i, sector in enumerate(top3, 1):
        print(f"  {i}. {sector['板块名称']} ({sector['板块类型']}): 连涨{sector['连涨天数']}天, 偏离度{sector['偏离度']}%, 量比{sector['成交量比值']}")


def main():
    """主函数"""
    print("A股潜力板块筛选脚本")
    print("筛选条件：低位（当前价格低于60日均价）、连涨3天")
    print("="*100)

    # 初始化数据库
    init_database()
    cleanup_old_data()

    # 获取交易日历
    trade_calendar = get_trade_calendar()
    target_date = get_latest_trade_date(trade_calendar)
    today = datetime.now().strftime("%Y%m%d")

    # 如果今天不是交易日，直接跳过抓取实时数据
    if not is_trade_date(today, trade_calendar):
        print(f"\n✓ 今天 ({today}) 非交易日，使用最近交易日 {target_date} 的数据")
        is_complete = 1
    elif is_trading_time(trade_calendar):
        print(f"\n⚠️  当前处于交易时间内（9:30-11:30 或 13:00-15:00）")
        print("当天数据尚未收盘，可能不完整。")
        include_today = get_user_choice_for_incomplete_data()
        if include_today:
            is_complete = 0
            target_date = today
            print("✓ 将包含当天未收盘数据（数据库会标记为不完整）")
        else:
            is_complete = 1
            print(f"✓ 将跳过今天的数据，使用最近完整交易日 {target_date} 的数据")
    else:
        print(f"\n✓ 当前非交易时间，使用最近完整交易日 {target_date} 的数据")
        is_complete = 1

    if is_complete == 1:
        cached_results = get_cached_screening_results(target_date, is_complete=1)
        if cached_results is not None:
            industry_sectors, concept_sectors = cached_results
            print(f"\n✓ 检测到 {target_date} 已有完整筛选结果，直接复用缓存结果")
            output_results(industry_sectors, concept_sectors, is_complete=1, save_to_db=False)
            return

    # 筛选行业板块（回看60天）
    industry_sectors = filter_potential_sectors("industry", min_consecutive_days=3, lookback_days=60,
                                                is_complete=is_complete, target_date=target_date)

    # 筛选概念板块（回看60天）
    concept_sectors = filter_potential_sectors("concept", min_consecutive_days=3, lookback_days=60,
                                               is_complete=is_complete, target_date=target_date)

    # 输出结果
    output_results(industry_sectors, concept_sectors, is_complete)


if __name__ == "__main__":
    main()
