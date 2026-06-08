from flask import Flask, request
from linebot.v3.webhook import WebhookHandler
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    PushMessageRequest,
    TextMessage
)
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import MessageEvent, TextMessageContent

import os
import re
import math
import time
import traceback
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone, timedelta
from io import StringIO
from pathlib import Path

import pandas as pd
import yfinance as yf
import requests


# ============================================================
# HCX AI 股票分析師 LINE Bot
# V5 HCX-AI量子雷達強化版
# 重點：
# 1. 參考HCX-AI量子雷達：先抓 TWSE / TPEx 官方全市場資料，代號與名稱一起建立快取
# 2. 股票名稱不再只靠 yfinance，避免 2330 2330、1717 1717
# 3. 加入主力成本估算、支撐壓力、台股 Tick 合法價位
# 4. 加入做多 / 做空價位說明
# ============================================================

APP_VERSION = "V6.5.1 正式版｜修正提醒排程Path錯誤"

app = Flask(__name__)

configuration = Configuration(access_token=os.environ["LINE_CHANNEL_ACCESS_TOKEN"])
handler = WebhookHandler(os.environ["LINE_CHANNEL_SECRET"])

# ============================================================
# 會員限定設定
# ------------------------------------------------------------
# Render Environment Variables 可新增：
#
# MEMBER_ONLY_MODE=true
# AUTHORIZED_USER_IDS=Uxxxxxxxx,Uyyyyyyyy
# ADMIN_USER_IDS=U你的LINE_USER_ID
#
# 說明：
# 1. MEMBER_ONLY_MODE=true 才會啟用會員限制。
# 2. AUTHORIZED_USER_IDS 放允許查詢的會員 LINE userId，多人用逗號分隔。
# 3. ADMIN_USER_IDS 放管理員 userId，管理員永遠可以查詢。
# 4. 不知道 userId 時，請會員傳「我的ID」，再把回覆的 ID 加到 Render。
# ============================================================

def env_list(name):
    raw = os.environ.get(name, "")
    return set(x.strip() for x in raw.split(",") if x.strip())


MEMBER_ONLY_MODE = os.environ.get("MEMBER_ONLY_MODE", "false").lower() in ["1", "true", "yes", "y", "on"]


def get_allowed_user_ids():
    return env_list("AUTHORIZED_USER_IDS")


def get_admin_user_ids():
    return env_list("ADMIN_USER_IDS")


def get_event_user_id(event):
    try:
        return getattr(event.source, "user_id", "") or ""
    except Exception:
        return ""


def get_event_source_type(event):
    try:
        return getattr(event.source, "type", "") or ""
    except Exception:
        return ""


def is_authorized_user(user_id):
    if not MEMBER_ONLY_MODE:
        return True

    if not user_id:
        return False

    if user_id in get_admin_user_ids():
        return True

    return user_id in get_allowed_user_ids()


def member_block_message(user_id):
    return f"""🔒 HCX-AI 會員限定提醒

很抱歉，此帳號尚未開通會員查詢權限。

📌 你的會員識別ID：
{user_id or "無法取得 userId"}

請把這組 ID 傳給管理員開通。

✅ 開通後即可使用：
📈 股票分析
🏦 主力成本雷達
🎯 做多 / 做空價位
🛡️ 停損與目標價
"""


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept-Language": "zh-TW,zh;q=0.9,en;q=0.8",
}

TWSE_OPENAPI_DAY_ALL = "https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"
TPEX_OPENAPI_DAILY = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"

# 官方名稱快取，避免每次查詢都重新抓全市場
MARKET_META_CACHE = {
    "ts": 0,
    "data": {}
}

# 常用股票名稱備援表：官方資料源或外部網路臨時失敗時，仍可顯示名稱
FALLBACK_STOCK_NAMES = {
    "1101": "台泥",
    "1102": "亞泥",
    "1216": "統一",
    "1301": "台塑",
    "1303": "南亞",
    "1326": "台化",
    "1402": "遠東新",
    "1476": "儒鴻",
    "1504": "東元",
    "1605": "華新",
    "1717": "長興",
    "2002": "中鋼",
    "2207": "和泰車",
    "2301": "光寶科",
    "2303": "聯電",
    "2308": "台達電",
    "2317": "鴻海",
    "2324": "仁寶",
    "2327": "國巨",
    "2330": "台積電",
    "2344": "華邦電",
    "2345": "智邦",
    "2353": "宏碁",
    "2356": "英業達",
    "2357": "華碩",
    "2371": "大同",
    "2379": "瑞昱",
    "2382": "廣達",
    "2408": "南亞科",
    "2409": "友達",
    "2412": "中華電",
    "2449": "京元電子",
    "2454": "聯發科",
    "2498": "宏達電",
    "2603": "長榮",
    "2609": "陽明",
    "2615": "萬海",
    "2880": "華南金",
    "2881": "富邦金",
    "2882": "國泰金",
    "2884": "玉山金",
    "2885": "元大金",
    "2886": "兆豐金",
    "2891": "中信金",
    "2892": "第一金",
    "3008": "大立光",
    "3034": "聯詠",
    "3231": "緯創",
    "3443": "創意",
    "3481": "群創",
    "3661": "世芯-KY",
    "3711": "日月光投控",
    "4938": "和碩",
    "5880": "合庫金",
    "6505": "台塑化",
    "6669": "緯穎",
    "8046": "南電",
    "8069": "元太",
    "8299": "群聯",
    "9105": "泰金寶-DR",
}


def clean_text(x):
    return re.sub(r"\s+", " ", str(x or "").strip())


def clean_num(x):
    if pd.isna(x):
        return None
    s = str(x).strip()
    if s in ["", "--", "---", "-", "N/A", "NaN", "nan"]:
        return None
    s = s.replace(",", "").replace("％", "").replace("%", "")
    s = s.replace("+", "").replace("X", "").replace("x", "")
    s = re.sub(r"[^0-9.\-]", "", s)
    try:
        return float(s)
    except Exception:
        return None


def request_json(url, params=None, timeout=12, tries=2):
    last_err = None
    for _ in range(tries):
        try:
            r = requests.get(url, params=params, headers=HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last_err = e
            time.sleep(0.25)
    raise last_err


def normalize_official_rows(data, market):
    """
    參考HCX-AI量子雷達邏輯：
    將 TWSE / TPEx 不同欄位名稱統一成 代號、名稱、市場、收盤。
    """
    rows = []
    if not isinstance(data, list):
        return rows

    for item in data:
        if not isinstance(item, dict):
            continue

        code = clean_text(
            item.get("Code")
            or item.get("證券代號")
            or item.get("股票代號")
            or item.get("代號")
            or item.get("SecuritiesCompanyCode")
            or item.get("有價證券代號")
            or item.get("公司代號")
            or ""
        ).replace("=", "").replace('"', "")

        name = clean_text(
            item.get("Name")
            or item.get("證券名稱")
            or item.get("股票名稱")
            or item.get("名稱")
            or item.get("CompanyName")
            or item.get("有價證券名稱")
            or item.get("公司簡稱")
            or item.get("公司名稱")
            or ""
        )

        close = clean_num(
            item.get("ClosingPrice")
            or item.get("收盤價")
            or item.get("收盤")
            or item.get("Close")
            or item.get("LatestPrice")
            or item.get("最新成交價")
        )

        volume = clean_num(
            item.get("TradeVolume")
            or item.get("成交股數")
            or item.get("成交數量")
            or item.get("成交量")
            or item.get("Volume")
        )

        if re.fullmatch(r"\d{4}", code) and name and name.lower() != "nan":
            rows.append({
                "代號": code,
                "名稱": name,
                "市場": market,
                "官方收盤": close,
                "官方成交股數": volume,
            })

    return rows


def fetch_market_meta(force=False):
    """
    抓官方全市場代號名稱，建立快取。
    這是修正「股票名稱跑不出來」的核心。
    """
    now = time.time()

    # 快取 60 分鐘
    if not force and MARKET_META_CACHE["data"] and now - MARKET_META_CACHE["ts"] < 3600:
        return MARKET_META_CACHE["data"]

    meta = {}

    # TWSE 上市
    try:
        twse_data = request_json(TWSE_OPENAPI_DAY_ALL)
        for row in normalize_official_rows(twse_data, "上市"):
            meta[row["代號"]] = row
    except Exception as e:
        print("TWSE 官方名稱抓取失敗：", e, flush=True)

    # TPEx 上櫃
    try:
        tpex_data = request_json(TPEX_OPENAPI_DAILY)
        for row in normalize_official_rows(tpex_data, "上櫃"):
            meta[row["代號"]] = row
    except Exception as e:
        print("TPEx 官方名稱抓取失敗：", e, flush=True)

    # 內建備援也塞進 meta，避免官方暫時失效
    for code, name in FALLBACK_STOCK_NAMES.items():
        if code not in meta:
            meta[code] = {
                "代號": code,
                "名稱": name,
                "市場": "上市",
                "官方收盤": None,
                "官方成交股數": None,
            }
        else:
            # 官方名稱空掉時用備援補
            if not meta[code].get("名稱") or meta[code].get("名稱") == code:
                meta[code]["名稱"] = name

    MARKET_META_CACHE["ts"] = now
    MARKET_META_CACHE["data"] = meta
    return meta


def get_stock_meta(code):
    code = str(code).strip()
    meta = fetch_market_meta().get(code)

    if meta:
        name = clean_text(meta.get("名稱") or FALLBACK_STOCK_NAMES.get(code) or code)
        market = clean_text(meta.get("市場") or "上市")
        if name == code and code in FALLBACK_STOCK_NAMES:
            name = FALLBACK_STOCK_NAMES[code]
        return {
            "code": code,
            "name": name,
            "market": market,
            "official_close": meta.get("官方收盤"),
            "official_volume": meta.get("官方成交股數"),
        }

    # 最後備援
    return {
        "code": code,
        "name": FALLBACK_STOCK_NAMES.get(code, code),
        "market": "上市",
        "official_close": None,
        "official_volume": None,
    }


def yahoo_symbols_by_meta(code, market):
    if market == "上櫃":
        return [f"{code}.TWO", f"{code}.TW", code]
    return [f"{code}.TW", f"{code}.TWO", code]


def normalize_yfinance_df(df):
    if df is None or df.empty:
        return None

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    needed = ["Open", "High", "Low", "Close", "Volume"]
    for col in needed:
        if col not in df.columns:
            return None

    df = df[needed].copy()
    for col in needed:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.dropna()
    return df


def download_from_yfinance(code, market):
    for symbol in yahoo_symbols_by_meta(code, market):
        try:
            df = yf.download(
                symbol,
                period="8mo",
                interval="1d",
                progress=False,
                auto_adjust=False,
                threads=False
            )
            df = normalize_yfinance_df(df)
            if df is not None and not df.empty and len(df) >= 60:
                return df, f"Yahoo {symbol}"
        except Exception:
            continue

    return None, None


def download_from_twse(code):
    try:
        today = pd.Timestamp.today()
        dfs = []

        for i in range(8):
            d = today - pd.DateOffset(months=i)
            date_str = d.strftime("%Y%m%d")
            url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
            params = {"response": "json", "date": date_str, "stockNo": code}

            data = request_json(url, params=params, timeout=10, tries=2)
            if data.get("stat") != "OK":
                continue

            rows = data.get("data", [])
            fields = data.get("fields", [])
            if rows:
                dfs.append(pd.DataFrame(rows, columns=fields))

        if not dfs:
            return None, None

        raw = pd.concat(dfs, ignore_index=True)

        def roc_to_date(x):
            y, m, d = str(x).split("/")
            return pd.Timestamp(int(y) + 1911, int(m), int(d))

        df = pd.DataFrame()
        df["Date"] = raw["日期"].apply(roc_to_date)
        df["Open"] = raw["開盤價"].map(clean_num)
        df["High"] = raw["最高價"].map(clean_num)
        df["Low"] = raw["最低價"].map(clean_num)
        df["Close"] = raw["收盤價"].map(clean_num)
        df["Volume"] = raw["成交股數"].map(clean_num)

        df = df.dropna()
        df = df.drop_duplicates("Date").sort_values("Date").set_index("Date")
        if len(df) >= 60:
            return df, "TWSE上市日線備援"
    except Exception:
        traceback.print_exc()

    return None, None


def download_from_tpex(code):
    try:
        today = pd.Timestamp.today()
        dfs = []

        for i in range(8):
            d = today - pd.DateOffset(months=i)
            date_str = f"{d.year - 1911}/{d.month:02d}"
            url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
            params = {"code": code, "date": date_str, "id": "", "response": "csv"}

            r = requests.get(url, params=params, headers=HEADERS, timeout=10)
            text = r.text
            if "日期" not in text or "收盤" not in text:
                continue

            lines = [line for line in text.splitlines() if len(line.split(",")) >= 7]
            if not lines:
                continue

            temp = pd.read_csv(StringIO("\n".join(lines)))
            dfs.append(temp)

        if not dfs:
            return None, None

        raw = pd.concat(dfs, ignore_index=True)
        raw.columns = [str(c).strip().replace('"', "") for c in raw.columns]

        def roc_to_date(x):
            x = str(x).replace('"', "").strip()
            y, m, d = x.split("/")
            return pd.Timestamp(int(y) + 1911, int(m), int(d))

        df = pd.DataFrame()
        df["Date"] = raw["日期"].apply(roc_to_date)
        df["Open"] = raw["開盤"].map(clean_num)
        df["High"] = raw["最高"].map(clean_num)
        df["Low"] = raw["最低"].map(clean_num)
        df["Close"] = raw["收盤"].map(clean_num)
        df["Volume"] = raw["成交股數"].map(clean_num)

        df = df.dropna()
        df = df.drop_duplicates("Date").sort_values("Date").set_index("Date")
        if len(df) >= 60:
            return df, "TPEx上櫃日線備援"
    except Exception:
        traceback.print_exc()

    return None, None


def get_stock_data(code, market):
    df, source = download_from_yfinance(code, market)
    if df is not None:
        return df, source

    if market == "上櫃":
        df, source = download_from_tpex(code)
        if df is not None:
            return df, source
        df, source = download_from_twse(code)
        if df is not None:
            return df, source
    else:
        df, source = download_from_twse(code)
        if df is not None:
            return df, source
        df, source = download_from_tpex(code)
        if df is not None:
            return df, source

    return None, None


def series_float(series, idx=-1):
    value = series.iloc[idx]
    if hasattr(value, "iloc"):
        value = value.iloc[0]
    return float(value)


def tick_size(price):
    p = abs(float(price))
    if p < 10:
        return 0.01
    elif p < 50:
        return 0.05
    elif p < 100:
        return 0.1
    elif p < 500:
        return 0.5
    elif p < 1000:
        return 1
    else:
        return 5


def round_price_by_tick(price, mode="nearest"):
    try:
        price = float(price)
        t = tick_size(price)

        if mode == "up":
            fixed = math.ceil((price / t) - 1e-9) * t
        elif mode == "down":
            fixed = math.floor((price / t) + 1e-9) * t
        else:
            fixed = round(price / t) * t

        return fixed
    except Exception:
        return price


def fmt_price(x):
    try:
        p = round_price_by_tick(float(x), "nearest")
        t = tick_size(p)

        if t >= 1:
            return f"{p:.0f}"
        elif t >= 0.1:
            return f"{p:.1f}"
        else:
            return f"{p:.2f}"
    except Exception:
        return "-"


def fmt_pct(x):
    try:
        return f"{float(x):.2f}%"
    except Exception:
        return "-"


def calc_daytrade_tick_profit(price):
    """
    台股當沖 Tick 成本/獲利提醒。
    採最嚴格的手續費6折估算：
    - 買進手續費：0.1425% * 0.6
    - 賣出手續費：0.1425% * 0.6
    - 當沖證交稅：0.15%
    合計成本比例約 0.321%。
    """
    try:
        price = float(price)
        t = tick_size(price)

        fee_rate = 0.001425 * 0.6
        tax_rate = 0.0015
        shares = 1000

        buy_fee = price * shares * fee_rate
        sell_fee = price * shares * fee_rate
        tax_fee = price * shares * tax_rate
        total_cost = buy_fee + sell_fee + tax_fee

        tick_profit = t * shares
        break_even_ticks = max(1, math.ceil(total_cost / tick_profit)) if tick_profit > 0 else 1
        first_profit_ticks = break_even_ticks

        return {
            "tick": t,
            "cost": total_cost,
            "tick_profit": tick_profit,
            "break_even_ticks": break_even_ticks,
            "first_profit_ticks": first_profit_ticks,
            "cost_rate_pct": (fee_rate * 2 + tax_rate) * 100,
        }
    except Exception:
        return {
            "tick": 0,
            "cost": 0,
            "tick_profit": 0,
            "break_even_ticks": 1,
            "first_profit_ticks": 1,
            "cost_rate_pct": 0.321,
        }


def calc_plan_profit_ticks(entry, take_profit, kind):
    """
    計算建議停利價距離進場價大約幾個 Tick。
    """
    try:
        entry = float(entry)
        take_profit = float(take_profit)
        t = tick_size(entry)
        if t <= 0:
            return 0
        if kind == "intraday_short":
            return max(0, int(round((entry - take_profit) / t)))
        return max(0, int(round((take_profit - entry) / t)))
    except Exception:
        return 0


def format_tick_profit_line(row):
    """
    給LINE會員看的簡潔成本提醒。
    不揭露完整算法，只顯示每張成本、1 Tick 獲利與幾 Tick 回本。
    """
    close = float(row.get("close") or row.get("entry") or 0)
    entry = float(row.get("entry") or close)
    take_profit = float(row.get("take_profit") or entry)
    kind = row.get("trade_kind") or "intraday_long"

    info = calc_daytrade_tick_profit(close)
    plan_ticks = calc_plan_profit_ticks(entry, take_profit, kind)

    msg = (
        f"   🔥 回本門檻：{info['break_even_ticks']} Tick｜建議停利約 {plan_ticks} Tick"
    )
    return msg


def data_date_text(df):
    """
    顯示最後一根日K的日期，也就是本次分析採用的資料日。
    """
    try:
        last_date = df.index[-1]
        if hasattr(last_date, "tz_localize"):
            # yfinance 多半是無時區日期；這裡只取日期即可
            pass
        return pd.Timestamp(last_date).strftime("%Y-%m-%d")
    except Exception:
        return pd.Timestamp.today().strftime("%Y-%m-%d")


def query_time_text():
    """
    顯示台灣查詢時間，精準到秒。
    注意：這是使用者查詢當下時間；資料日仍以最後一根日K日期為準。
    """
    taiwan_tz = timezone(timedelta(hours=8))
    return datetime.now(taiwan_tz).strftime("%Y-%m-%d %H:%M:%S")


def calc_vwap(typical_price, volume, days):
    """
    VWAP 成本：用典型價 (High+Low+Close)/3 搭配成交量計算。
    比單純用收盤價更接近真實成交重心。
    """
    tp = typical_price.tail(days)
    vol = volume.tail(days)

    total_vol = float(vol.sum())
    if total_vol <= 0:
        return float(tp.mean())

    return float((tp * vol).sum() / total_vol)


def calc_volume_cluster_cost(typical_price, volume, days=60):
    """
    大量成交成本：
    取近 days 日中成交量最大的前 30% K棒，計算其 VWAP。
    用來估計籌碼密集區，不等於真實主力持股成本。
    """
    tp = typical_price.tail(days)
    vol = volume.tail(days)

    if len(tp) < 10 or float(vol.sum()) <= 0:
        return float(tp.mean())

    temp = pd.DataFrame({"tp": tp, "vol": vol}).dropna()
    if temp.empty:
        return float(tp.mean())

    top_n = max(5, int(len(temp) * 0.30))
    heavy = temp.sort_values("vol", ascending=False).head(top_n)
    total_vol = float(heavy["vol"].sum())

    if total_vol <= 0:
        return float(heavy["tp"].mean())

    return float((heavy["tp"] * heavy["vol"]).sum() / total_vol)


def calc_professional_main_cost(close, high_series, low_series, close_series, volume_series):
    """
    職業級成本雷達：
    舊版只用 20日VWAP，遇到急跌股會讓成本看起來過高。
    新版改為：
    1. 5日VWAP：短線控盤成本
    2. 10日VWAP：短波段成本
    3. 20日VWAP：波段籌碼成本
    4. 60日大量成交成本：大量籌碼密集區
    5. AI採用成本：依目前價格相對位置動態加權

    注意：這仍是籌碼成本估算，不是券商實際買進成本。
    """
    typical_price = (high_series + low_series + close_series) / 3

    cost5 = calc_vwap(typical_price, volume_series, 5)
    cost10 = calc_vwap(typical_price, volume_series, 10)
    cost20 = calc_vwap(typical_price, volume_series, 20)
    cluster60 = calc_volume_cluster_cost(typical_price, volume_series, 60)

    ma20 = float(close_series.rolling(20).mean().iloc[-1])
    close = float(close)

    # 急跌且跌破20日成本時，主控成本改偏重短線VWAP，避免估得太高。
    if close < cost20 and close < ma20:
        active_cost = cost5 * 0.55 + cost10 * 0.30 + cost20 * 0.15
        cost_mode = "急跌修正版：偏重5日與10日短線成交重心"
    # 強勢股站上20日成本時，加入大量成交成本，觀察主力鎖碼區。
    elif close > cost20:
        active_cost = cost5 * 0.30 + cost10 * 0.25 + cost20 * 0.25 + cluster60 * 0.20
        cost_mode = "強勢追蹤版：加入大量成交籌碼區"
    else:
        active_cost = cost5 * 0.40 + cost10 * 0.35 + cost20 * 0.25
        cost_mode = "標準波段版：5/10/20日VWAP加權"

    cost_low = min(cost5, cost10, cost20, active_cost)
    cost_high = max(cost5, cost10, cost20, active_cost)

    diff = close - active_cost
    pct = diff / active_cost * 100 if active_cost else 0

    if close > active_cost:
        light = "🟢"
        status = "現價站上AI主控成本，短線籌碼相對有撐。"
    elif close < active_cost:
        light = "🔴"
        status = "現價低於AI主控成本，代表上方仍有套牢與賣壓。"
    else:
        light = "🟡"
        status = "現價接近AI主控成本，等待方向表態。"

    return {
        "cost5": cost5,
        "cost10": cost10,
        "cost20": cost20,
        "cluster60": cluster60,
        "active_cost": active_cost,
        "cost_low": cost_low,
        "cost_high": cost_high,
        "diff": diff,
        "pct": pct,
        "light": light,
        "status": status,
        "mode": cost_mode,
    }


def stock_ai(code):
    try:
        code = str(code).strip()
        meta = get_stock_meta(code)
        stock_name = meta["name"]
        market = meta["market"]

        df, used_source = get_stock_data(code, market)

        if df is None or df.empty or len(df) < 60:
            return f"""⚠️ HCX AI 查詢提醒

🏷️ 股票：{code} {stock_name}
🏛️ 市場：{market}

目前查不到足夠日K資料。
可能原因：
① Yahoo Finance 暫時沒有回應
② TWSE/TPEx 資料源維護中
③ 股票代號輸入錯誤
④ 該商品資料不足 60 根K棒

請稍後再試，或換其他代號測試。
"""

        query_data_date = data_date_text(df)
        query_time = query_time_text()

        close_series = pd.to_numeric(df["Close"], errors="coerce")
        high_series = pd.to_numeric(df["High"], errors="coerce")
        low_series = pd.to_numeric(df["Low"], errors="coerce")
        volume_series = pd.to_numeric(df["Volume"], errors="coerce")

        close = series_float(close_series, -1)
        prev = series_float(close_series, -2)
        change = close - prev
        pct = change / prev * 100 if prev else 0

        ma5 = float(close_series.rolling(5).mean().iloc[-1])
        ma20 = float(close_series.rolling(20).mean().iloc[-1])
        ma60 = float(close_series.rolling(60).mean().iloc[-1])

        high20 = float(high_series.tail(20).max())
        low20 = float(low_series.tail(20).min())
        recent_high = float(high_series.tail(10).max())
        recent_low = float(low_series.tail(10).min())

        prev_close = close_series.shift(1)
        tr1 = high_series - low_series
        tr2 = (high_series - prev_close).abs()
        tr3 = (low_series - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr14 = float(tr.rolling(14).mean().iloc[-1])

        cost_info = calc_professional_main_cost(
            close=close,
            high_series=high_series,
            low_series=low_series,
            close_series=close_series,
            volume_series=volume_series
        )

        main_cost = cost_info["active_cost"]
        main_cost_diff = cost_info["diff"]
        main_cost_pct = cost_info["pct"]
        chip_light = cost_info["light"]
        chip_status = cost_info["status"]

        if close > ma20 > ma60:
            trend = "偏多"
            trend_icon = "🟢🚀"
            direction_summary = "主策略偏做多；不建議主動放空，除非跌破支撐且收不回。"
            advice = "站上月線與季線，短線偏多。可觀察突破壓力續攻，或回測 MA20 不破後承接。"
        elif close < ma20 < ma60:
            trend = "偏空"
            trend_icon = "🔴⚠️"
            direction_summary = "主策略偏做空或觀望；不建議急著做多，除非重新站回 MA20。"
            advice = "均線呈現空方排列，短線偏弱。保守者先觀望，避免亂接刀。"
        else:
            trend = "震盪"
            trend_icon = "🟡🔄"
            direction_summary = "震盪盤；做多等突破或支撐守住，做空等跌破支撐，不宜中間追價。"
            advice = "目前均線糾結，屬於震盪整理。等待突破壓力或跌破支撐後再決定方向。"

        # 進出場點位：先算理論值，再依台股 Tick 修正成可掛單價
        breakout_buy_raw = max(high20, close)
        breakout_stop_raw = breakout_buy_raw - atr14 * 1.5
        breakout_target1_raw = breakout_buy_raw + atr14 * 2.0
        breakout_target2_raw = breakout_buy_raw + atr14 * 3.0

        pullback_low_raw = max(low20, ma20 - atr14 * 0.5)
        pullback_high_raw = ma20 + atr14 * 0.3
        pullback_stop_raw = pullback_low_raw - atr14 * 1.2
        pullback_target_raw = recent_high

        weak_trigger_raw = min(low20, close)
        weak_stop_raw = weak_trigger_raw + atr14 * 1.5
        weak_target_raw = weak_trigger_raw - atr14 * 2.0

        breakout_buy = round_price_by_tick(breakout_buy_raw, "up")
        breakout_stop = round_price_by_tick(breakout_stop_raw, "down")
        breakout_target1 = round_price_by_tick(breakout_target1_raw, "up")
        breakout_target2 = round_price_by_tick(breakout_target2_raw, "up")

        pullback_low = round_price_by_tick(pullback_low_raw, "up")
        pullback_high = round_price_by_tick(pullback_high_raw, "down")
        if pullback_low > pullback_high:
            pullback_low = round_price_by_tick(ma20 - atr14 * 0.3, "down")
            pullback_high = round_price_by_tick(ma20 + atr14 * 0.3, "up")

        pullback_stop = round_price_by_tick(pullback_stop_raw, "down")
        pullback_target = round_price_by_tick(pullback_target_raw, "up")

        weak_trigger = round_price_by_tick(weak_trigger_raw, "down")
        weak_stop = round_price_by_tick(weak_stop_raw, "up")
        weak_target = round_price_by_tick(weak_target_raw, "down")
        ma20_recover = round_price_by_tick(ma20, "up")
        current_tick = tick_size(close)

        if trend == "偏多":
            plan = f"""🚀【做多價位說明】
✅ 進場1｜突破追價：站上 {fmt_price(breakout_buy)} 可視為轉強攻擊點
🛡️ 停損1｜突破失敗：跌破 {fmt_price(breakout_stop)} 先保護資金
🎯 目標1｜短線停利：{fmt_price(breakout_target1)}
🏆 目標2｜強勢續抱：{fmt_price(breakout_target2)}

🛡️【做多低接區】
📌 進場2｜回測承接：{fmt_price(pullback_low)} ~ {fmt_price(pullback_high)}
🚨 停損2｜跌破防守：{fmt_price(pullback_stop)}
🎯 反彈目標：{fmt_price(pullback_target)}

🔴【做空條件】
只有跌破 {fmt_price(weak_trigger)} 且反彈站不回，才考慮偏空。"""
        elif trend == "偏空":
            plan = f"""📉【做空價位說明】
⚠️ 進場1｜跌破放空：跌破 {fmt_price(weak_trigger)} 轉弱
🛡️ 空方停損：站回 {fmt_price(weak_stop)} 先停損
🎯 空方目標：{fmt_price(weak_target)}

🔁【做多轉強條件】
✅ 重新站回 MA20：{fmt_price(ma20_recover)}
✅ 再突破壓力：{fmt_price(breakout_buy)}
若兩個條件都有，偏空看法要降低。"""
        else:
            plan = f"""🔄【震盪盤多空價位】
🟢 做多條件1｜突破壓力：{fmt_price(breakout_buy)}
🟢 做多條件2｜支撐低接：{fmt_price(pullback_low)} ~ {fmt_price(pullback_high)}
🛡️ 多方停損：{fmt_price(pullback_stop)}

🔴 做空條件｜跌破支撐：{fmt_price(weak_trigger)}
🛡️ 空方停損：{fmt_price(weak_stop)}
🎯 空方目標：{fmt_price(weak_target)}

🧠 區間內不追高、不殺低，等方向確認。"""

        price_icon = "🔴📈" if change > 0 else "🟢📉" if change < 0 else "⚪➖"

        return f"""🌈✨ HCX AI 股票分析師 ✨🌈
版本：{APP_VERSION}

🏷️ 股票：{code} {stock_name}
🏛️ 市場：{market}
🕒 查詢時間：{query_time}
━━━━━━━━━━━━━━
{price_icon}【即時價格雷達】
💰 現價：{fmt_price(close)}
📊 漲跌：{fmt_price(change)}
📈 漲跌幅：{fmt_pct(pct)}

━━━━━━━━━━━━━━
📐【均線結構】
⚡ MA5：{fmt_price(ma5)}
🌙 MA20：{fmt_price(ma20)}
🏔️ MA60：{fmt_price(ma60)}
🌊 ATR14：{fmt_price(atr14)}

━━━━━━━━━━━━━━
{chip_light}【職業級成本雷達】
🏦 AI主控成本：{fmt_price(main_cost)}
📦 成本區間：{fmt_price(cost_info["cost_low"])} ~ {fmt_price(cost_info["cost_high"])}
⚡ 5日短線VWAP：{fmt_price(cost_info["cost5"])}
🌙 20日波段VWAP：{fmt_price(cost_info["cost20"])}
🔥 60日大量成本：{fmt_price(cost_info["cluster60"])}
📏 現價距主控成本：{fmt_price(main_cost_diff)} / {fmt_pct(main_cost_pct)}

━━━━━━━━━━━━━━
🧱【支撐壓力】
🔺 20日壓力：{fmt_price(high20)}
🔻 20日支撐：{fmt_price(low20)}
📌 10日高點：{fmt_price(recent_high)}
📌 10日低點：{fmt_price(recent_low)}

━━━━━━━━━━━━━━
{trend_icon}【趨勢判斷】
目前趨勢：{trend}
🎯 操作方向：{direction_summary}

🧠 AI專業解讀：
{advice}

━━━━━━━━━━━━━━
{plan}

━━━━━━━━━━━━━━
⚠️ 風險提醒
本訊息為程式估算與技術分析，不代表保證獲利。
主力成本為AI估算值，非券商實際持股成本。
"""

    except Exception as e:
        print("stock_ai 發生錯誤：", str(e), flush=True)
        traceback.print_exc()
        return f"查詢 {code} 時發生錯誤，請稍後再試。"

# ============================================================
# LINE 量子選股指令：當沖多 / 當沖空 / 隔日沖
# ------------------------------------------------------------
# 說明：
# 1. 使用官方代號名稱快取取得全市場名單。
# 2. 先取成交量較活躍股票，避免 LINE 即時服務掃描過久。
# 3. 使用日K量價條件 + 簡化回測估算 AI勝率。
# 4. 這是HCX-AI量子雷達；完整精準版仍建議由 Colab HCX量子雷達跑完整報表。
# ============================================================

QUANTUM_COMMANDS = {
    "當沖多": "intraday_long",
    "當沖空": "intraday_short",
    "隔日沖": "swing",
    "當沖股": "intraday_best",
}

def normalize_command_text(text):
    """
    將使用者輸入標準化，避免多一個空白、斜線、標點就辨識不到。
    支援：
    當沖多、/當沖多、當沖 多、我要當沖多、當衝多
    當沖空、/當沖空、當沖 空、我要當沖空、當衝空
    隔日沖、隔日衝、/隔日沖
    """
    text = str(text or "").strip()
    text = text.replace("\n", "")
    text = text.replace(" ", "")
    text = text.replace("　", "")
    text = text.replace("/", "")
    text = text.replace("股票", "")
    text = text.replace("請查", "")
    text = text.replace("查詢", "")
    text = text.replace("我要", "")
    text = text.replace("幫我", "")
    text = text.replace("一下", "")
    return text


def detect_quantum_command(raw_text):
    """
    回傳標準指令：當沖多 / 當沖空 / 隔日沖
    找不到則回傳 None。
    """
    t = normalize_command_text(raw_text)

    best_words = ["當沖股", "今日當沖", "最佳當沖", "當沖名單", "當沖前五", "當沖5", "當沖"]
    long_words = ["當沖多", "當衝多", "沖多", "衝多", "當日多", "當沖做多", "當衝做多"]
    short_words = ["當沖空", "當衝空", "沖空", "衝空", "當日空", "當沖做空", "當衝做空"]
    swing_words = ["隔日沖", "隔日衝", "隔日", "隔日多", "隔日沖多", "隔日衝多"]

    if any(w in t for w in long_words):
        return "當沖多"
    if any(w in t for w in short_words):
        return "當沖空"
    if any(w in t for w in swing_words):
        return "隔日沖"
    if any(w in t for w in best_words):
        return "當沖股"

    return None



def push_text_message(user_id, text):
    """背景掃描完成後，主動推送結果給查詢者。"""
    if not user_id:
        return False

    with ApiClient(configuration) as api_client:
        line_bot_api = MessagingApi(api_client)
        line_bot_api.push_message(
            PushMessageRequest(
                to=user_id,
                messages=[TextMessage(text=text)]
            )
        )
    return True


# ============================================================
# 會員自訂提醒排程
# ------------------------------------------------------------
# 支援指令：
# 設定提醒 當沖多 08:50
# 設定提醒 當沖空 09:00
# 設定提醒 隔日沖 13:35
# 我的提醒
# 取消提醒 當沖多
# 取消全部提醒
#
# 注意：
# Render 免費版會休眠，若服務睡著，排程不會準時觸發。
# 建議用 cron-job.org / UptimeRobot 在 08:48 先喚醒 Render。
# ============================================================

REMINDER_SCHEDULE_FILE = os.environ.get("REMINDER_SCHEDULE_FILE", "/tmp/hcx_line_member_reminders.json")
REMINDER_LOCK = threading.Lock()
REMINDER_SCHEDULES = {}
REMINDER_SCHEDULER_STARTED = False


def taipei_now():
    return datetime.now(timezone(timedelta(hours=8)))


def _safe_load_json(path, default):
    try:
        p = Path(path)
        if not p.exists():
            return default
        with p.open("r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else default
    except Exception:
        return default


def load_reminder_schedules():
    global REMINDER_SCHEDULES
    with REMINDER_LOCK:
        REMINDER_SCHEDULES = _safe_load_json(REMINDER_SCHEDULE_FILE, {})
        return REMINDER_SCHEDULES


def save_reminder_schedules():
    try:
        p = Path(REMINDER_SCHEDULE_FILE)
        p.parent.mkdir(parents=True, exist_ok=True)
        with p.open("w", encoding="utf-8") as f:
            json.dump(REMINDER_SCHEDULES, f, ensure_ascii=False, indent=2)
        return True
    except Exception as e:
        print(f"提醒設定儲存失敗：{e}", flush=True)
        return False


def parse_reminder_command(raw_text):
    """
    解析會員提醒指令。
    回傳：
    {"action":"set","command":"當沖多","time":"08:50"}
    {"action":"list"}
    {"action":"cancel","command":"當沖多"}
    {"action":"cancel_all"}
    或 None。
    """
    raw = str(raw_text or "").strip()
    compact = normalize_command_text(raw)

    if compact in ["我的提醒", "提醒列表", "查提醒", "查看提醒", "設定列表"]:
        return {"action": "list"}

    if compact in ["取消全部提醒", "刪除全部提醒", "關閉全部提醒", "停止全部提醒"]:
        return {"action": "cancel_all"}

    if "取消提醒" in compact or "刪除提醒" in compact or "停止提醒" in compact or "關閉提醒" in compact:
        cmd = detect_quantum_command(compact)
        if cmd:
            return {"action": "cancel", "command": cmd}
        return {"action": "cancel_all"}

    # 沒有「提醒 / 定時 / 每天」等關鍵字，就不要誤判一般查詢
    has_reminder_word = any(w in raw for w in ["提醒", "定時", "每天", "每日", "排程", "鬧鐘"])
    if not has_reminder_word:
        return None

    # 支援 08:50 / 8:50 / 0850 / 850
    m = re.search(r"([01]?\d|2[0-3])\s*[:：]?\s*([0-5]\d)", raw)
    if not m:
        return {"action": "help"}

    hh = int(m.group(1))
    mm = int(m.group(2))
    time_text = f"{hh:02d}:{mm:02d}"

    cmd = detect_quantum_command(raw)
    if cmd not in QUANTUM_COMMANDS:
        return {"action": "help"}

    return {"action": "set", "command": cmd, "time": time_text}


def format_reminder_help():
    return """⏰ HCX-AI 會員自訂提醒

你可以設定每天固定時間自動推播：

✅ 設定提醒 當沖多 08:50
✅ 設定提醒 當沖空 09:00
✅ 設定提醒 隔日沖 13:35
✅ 設定提醒 當沖股 08:55

查詢：
📋 我的提醒

取消：
🗑️ 取消提醒 當沖多
🗑️ 取消全部提醒

提醒會在週一到週五觸發。
⚠️ 若 Render 免費版睡著，需先喚醒服務才會準時。"""


def format_user_reminders(user_id):
    with REMINDER_LOCK:
        user_data = REMINDER_SCHEDULES.get(user_id, {})

    if not user_data:
        return """📋 目前尚未設定提醒

可輸入：
設定提醒 當沖多 08:50
設定提醒 當沖空 09:00
設定提醒 隔日沖 13:35"""

    lines = [
        "📋 你的 HCX-AI 自動提醒",
        "━━━━━━━━━━━━━━"
    ]

    for cmd in ["當沖股", "當沖多", "當沖空", "隔日沖"]:
        item = user_data.get(cmd)
        if item and item.get("enabled", True):
            lines.append(f"✅ {cmd}｜每天 {item.get('time', '--:--')}")

    lines.extend([
        "━━━━━━━━━━━━━━",
        "取消範例：取消提醒 當沖多"
    ])

    return "\n".join(lines)


def handle_reminder_command(user_id, parsed):
    if not user_id:
        return "⚠️ 無法取得你的 LINE userId，請用一對一好友聊天室設定提醒。"

    action = parsed.get("action")

    if action == "help":
        return format_reminder_help()

    if action == "list":
        return format_user_reminders(user_id)

    with REMINDER_LOCK:
        if user_id not in REMINDER_SCHEDULES:
            REMINDER_SCHEDULES[user_id] = {}

        if action == "set":
            cmd = parsed["command"]
            time_text = parsed["time"]

            REMINDER_SCHEDULES[user_id][cmd] = {
                "time": time_text,
                "enabled": True,
                "last_sent_date": "",
                "updated_at": query_time_text()
            }
            save_reminder_schedules()

            return f"""✅ 已設定自動提醒

📌 類型：{cmd}
⏰ 時間：每天 {time_text}
📆 週一到週五自動推播

查詢設定請輸入：
我的提醒"""

        if action == "cancel":
            cmd = parsed.get("command")
            if cmd in REMINDER_SCHEDULES.get(user_id, {}):
                REMINDER_SCHEDULES[user_id].pop(cmd, None)
                save_reminder_schedules()
                return f"✅ 已取消「{cmd}」提醒。"
            return f"目前沒有設定「{cmd}」提醒。"

        if action == "cancel_all":
            REMINDER_SCHEDULES[user_id] = {}
            save_reminder_schedules()
            return "✅ 已取消全部自動提醒。"

    return format_reminder_help()


def reminder_scheduler_loop():
    """
    背景排程：
    每 20 秒檢查一次。
    同一分鐘同一指令若多人訂閱，只掃描一次，再推播給所有會員。
    """
    print("========== HCX-AI 會員提醒排程已啟動 ==========", flush=True)
    load_reminder_schedules()

    while True:
        try:
            now = taipei_now()
            today = now.strftime("%Y-%m-%d")
            hhmm = now.strftime("%H:%M")

            # 台股主要提醒只在週一到週五跑
            if now.weekday() >= 5:
                time.sleep(20)
                continue

            due_map = {}

            with REMINDER_LOCK:
                for uid, user_data in list(REMINDER_SCHEDULES.items()):
                    if not isinstance(user_data, dict):
                        continue

                    for cmd, item in list(user_data.items()):
                        if not isinstance(item, dict):
                            continue
                        if not item.get("enabled", True):
                            continue
                        if item.get("time") != hhmm:
                            continue
                        if item.get("last_sent_date") == today:
                            continue

                        item["last_sent_date"] = today
                        due_map.setdefault(cmd, []).append(uid)

                if due_map:
                    save_reminder_schedules()

            for cmd, user_ids in due_map.items():
                try:
                    print(f"提醒排程觸發：{cmd} {hhmm} 人數={len(user_ids)}", flush=True)
                    result_text = run_quantum_scan(cmd)
                    header = f"⏰ HCX-AI 自動提醒｜{cmd}\n🕒 {query_time_text()}\n\n"
                    final_text = header + result_text

                    for uid in user_ids:
                        try:
                            # 若後來被取消會員權限，就不再推播
                            if is_authorized_user(uid):
                                push_text_message(uid, final_text[:4900])
                        except Exception as e:
                            print(f"提醒推播失敗 {uid}: {e}", flush=True)

                except Exception as e:
                    print(f"提醒排程掃描失敗 {cmd}: {e}", flush=True)
                    traceback.print_exc()

        except Exception as e:
            print(f"提醒排程主迴圈錯誤：{e}", flush=True)
            traceback.print_exc()

        time.sleep(20)


def start_reminder_scheduler():
    global REMINDER_SCHEDULER_STARTED
    if REMINDER_SCHEDULER_STARTED:
        return

    REMINDER_SCHEDULER_STARTED = True
    t = threading.Thread(target=reminder_scheduler_loop, daemon=True)
    t.start()



def get_quantum_scan_limit():
    """
    B方案：先用全市場活躍股 600 檔做母體。
    Render Environment Variable 可設定 QUANTUM_SCAN_LIMIT，但上限固定 600。
    """
    try:
        n = int(os.environ.get("QUANTUM_SCAN_LIMIT", "600"))
        return max(100, min(n, 600))
    except Exception:
        return 600


def get_quantum_stage1_limit():
    """
    第一層快速篩選：600檔 → 100檔。
    先用官方成交量/成交值做快速初選，避免600檔全部下載日K造成LINE等待太久。
    """
    try:
        n = int(os.environ.get("QUANTUM_STAGE1_LIMIT", "100"))
        return max(30, min(n, 150))
    except Exception:
        return 100


def get_quantum_deep_limit():
    """
    第二層深度評分：100檔 → 30檔。
    只有前30名才進入最後分K價位計算。
    """
    try:
        n = int(os.environ.get("QUANTUM_DEEP_LIMIT", "30"))
        return max(10, min(n, 50))
    except Exception:
        return 30


def get_quantum_top_n():
    """
    LINE 推播只顯示前5名，避免訊息太長。
    """
    try:
        n = int(os.environ.get("QUANTUM_TOP_N", "5"))
        return max(1, min(n, 5))
    except Exception:
        return 5


def get_quantum_min_volume_lots():
    """
    V6.1 當沖量能門檻：
    預設至少 5000 張，避免選出成交量太小、不好進出的股票。
    Render 可用 QUANTUM_MIN_VOLUME_LOTS 微調，但最低不低於 1000 張。
    """
    try:
        n = int(float(os.environ.get("QUANTUM_MIN_VOLUME_LOTS", "5000")))
        return max(1000, min(n, 50000))
    except Exception:
        return 5000


def get_quantum_min_value_m():
    """
    V6.1 成交金額門檻：
    預設至少 100 百萬，避免只靠低價股大量但不好操作。
    Render 可用 QUANTUM_MIN_VALUE_M 微調。
    """
    try:
        n = float(os.environ.get("QUANTUM_MIN_VALUE_M", "100"))
        return max(20.0, min(n, 5000.0))
    except Exception:
        return 100.0


def get_quantum_daily_list_mode():
    """
    每日可出榜模式：
    true：先嚴格篩選，若沒有結果，自動放寬條件，讓當沖多/空/隔日沖盡量都有 TOP5。
    false：完全嚴格，沒有符合就不出榜。
    """
    return str(os.environ.get("QUANTUM_DAILY_LIST_MODE", "true")).strip().lower() not in ("0", "false", "no", "off")


def get_quantum_volume_fallback_levels():
    """
    量能 fallback：
    先 5000 張，沒結果就 3000 張，再沒結果就 1000 張。
    這是為了符合原本量子雷達「每日可出榜」精神。
    """
    first = get_quantum_min_volume_lots()
    levels = [first, 3000, 1000]
    out = []
    for x in levels:
        try:
            x = int(x)
            if x not in out:
                out.append(x)
        except Exception:
            pass
    return out


def volume_shares_to_lots(volume_shares):
    """
    TWSE / TPEx / yfinance 常見回傳為股數，這裡統一轉成張數。
    """
    try:
        v = float(volume_shares or 0)
        return max(0.0, v / 1000.0)
    except Exception:
        return 0.0


def calc_value_m(close, volume_shares):
    """
    成交金額百萬估算：股價 * 股數 / 1,000,000。
    """
    try:
        return max(0.0, float(close or 0) * float(volume_shares or 0) / 1_000_000.0)
    except Exception:
        return 0.0


def fmt_lots(x):
    try:
        return f"{float(x):,.0f}"
    except Exception:
        return "-"


def is_common_stock(code, name):
    """
    排除 ETF、ETN、權證、特殊商品。
    """
    code = str(code)
    name = str(name)

    if not re.fullmatch(r"\d{4}", code):
        return False

    # 多數 ETF 是 00 開頭，權證/特殊商品也容易混入，LINE版先排除
    if code.startswith("00"):
        return False

    bad_words = [
        "ETF", "ETN", "指數", "債", "期貨", "正2", "反1", "權證",
        "購", "售", "牛", "熊", "特", "受益證券"
    ]
    if any(w in name.upper() for w in bad_words):
        return False

    return True



def get_quantum_universe():
    """
    V6.5 候選池：
    先抓官方全市場資料，排除 ETF / 權證 / 特殊商品。
    為避免官方資料單位不同造成整批空榜，這裡採「軟性量能排序」：
    - 有成交量 / 成交金額者優先
    - 不在這裡硬濾到空
    - 真正排序與出榜在後面量子雷達分數處理
    """
    meta = fetch_market_meta()
    rows = []

    for code, item in meta.items():
        name = clean_text(item.get("名稱") or FALLBACK_STOCK_NAMES.get(code) or code)
        market = clean_text(item.get("市場") or "上市")
        close = item.get("官方收盤")
        vol = item.get("官方成交股數")

        if not is_common_stock(code, name):
            continue

        try:
            close_f = float(close or 0)
        except Exception:
            close_f = 0.0

        try:
            vol_f = float(vol or 0)
        except Exception:
            vol_f = 0.0

        if close_f <= 0 or close_f < 5:
            continue

        vol_lots = volume_shares_to_lots(vol_f)
        value_m = calc_value_m(close_f, vol_f)

        rows.append({
            "code": code,
            "name": name,
            "market": market,
            "close": close_f,
            "volume": vol_f,
            "volume_lots": vol_lots,
            "official_value_m": value_m,
            "official_value": close_f * vol_f,
        })

    rows = sorted(
        rows,
        key=lambda x: (
            float(x.get("official_value_m") or 0),
            float(x.get("volume_lots") or 0),
            float(x.get("close") or 0)
        ),
        reverse=True
    )

    return rows[:get_quantum_scan_limit()]


def select_stage1_candidates(universe, command):
    """
    第一層：600 → 100。
    比照原本量子雷達精神：先用成交量 / 成交金額建立活躍母體。
    V6.1 已先排除低於5000張或成交金額不足標的。
    """
    if not universe:
        return []

    limit = get_quantum_stage1_limit()

    ranked = sorted(
        universe,
        key=lambda x: (float(x.get("official_value_m") or 0), float(x.get("volume_lots") or 0)),
        reverse=True
    )

    return ranked[:limit]


def calc_atr14(high_series, low_series, close_series):
    prev_close = close_series.shift(1)
    tr1 = high_series - low_series
    tr2 = (high_series - prev_close).abs()
    tr3 = (low_series - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return float(tr.rolling(14).mean().iloc[-1])


def _signal_features(df, i):
    close_series = pd.to_numeric(df["Close"], errors="coerce")
    high_series = pd.to_numeric(df["High"], errors="coerce")
    low_series = pd.to_numeric(df["Low"], errors="coerce")
    vol_series = pd.to_numeric(df["Volume"], errors="coerce")

    close = float(close_series.iloc[i])
    prev = float(close_series.iloc[i - 1])
    high20 = float(high_series.iloc[i-19:i+1].max())
    low20 = float(low_series.iloc[i-19:i+1].min())

    ma5 = float(close_series.rolling(5).mean().iloc[i])
    ma20 = float(close_series.rolling(20).mean().iloc[i])
    ma60 = float(close_series.rolling(60).mean().iloc[i])
    vol_ma20 = float(vol_series.rolling(20).mean().iloc[i])
    vol_ratio = float(vol_series.iloc[i] / vol_ma20) if vol_ma20 > 0 else 0
    pct = (close - prev) / prev * 100 if prev else 0
    pos20 = (close - low20) / (high20 - low20) * 100 if high20 > low20 else 50

    atr = calc_atr14(
        high_series.iloc[:i+1],
        low_series.iloc[:i+1],
        close_series.iloc[:i+1]
    )

    return {
        "close": close,
        "prev": prev,
        "ma5": ma5,
        "ma20": ma20,
        "ma60": ma60,
        "high20": high20,
        "low20": low20,
        "vol_ratio": vol_ratio,
        "pct": pct,
        "pos20": pos20,
        "atr": atr,
    }


def _is_signal(feat, kind):
    if kind == "intraday_long":
        return (
            feat["close"] > feat["ma5"] >= feat["ma20"] and
            feat["pos20"] >= 60 and
            feat["vol_ratio"] >= 0.90 and
            feat["pct"] >= -1.0
        )

    if kind == "intraday_short":
        return (
            feat["close"] < feat["ma5"] <= feat["ma20"] and
            feat["pos20"] <= 45 and
            feat["vol_ratio"] >= 0.90 and
            feat["pct"] <= 1.0
        )

    # 隔日沖：偏多、放量、收盤位置不能太差；弱市也保留相對高分股
    return (
        feat["vol_ratio"] >= 0.85 and
        feat["pos20"] >= 45 and
        feat["pct"] >= -2.5 and
        feat["close"] >= feat["ma20"] * 0.96
    )


def _signal_success(df, i, feat, kind):
    """
    用隔日K近似回測勝率。
    注意：LINE沒有逐筆成交與1分K，這裡是策略勝率估算。
    """
    next_high = float(df["High"].iloc[i + 1])
    next_low = float(df["Low"].iloc[i + 1])
    next_close = float(df["Close"].iloc[i + 1])
    entry = feat["close"]
    atr = max(float(feat["atr"]), entry * 0.015)

    if kind == "intraday_long":
        target = entry + atr * 0.60
        stop = entry - atr * 0.45
        return (next_high >= target and next_low > stop) or (next_close > entry)

    if kind == "intraday_short":
        target = entry - atr * 0.60
        stop = entry + atr * 0.45
        return (next_low <= target and next_high < stop) or (next_close < entry)

    # 隔日沖：隔日收紅或觸及短目標
    target = entry + atr * 0.50
    return (next_high >= target) or (next_close > entry)


def estimate_strategy_win_rate(df, kind, lookback=80):
    """
    極速版回測勝率：改成向量化計算，避免每一根K棒都重新算 rolling。
    速度比原本逐列迴圈快很多。
    """
    try:
        if df is None or df.empty or len(df) < 80:
            return 50.0, 0

        close = pd.to_numeric(df["Close"], errors="coerce")
        high = pd.to_numeric(df["High"], errors="coerce")
        low = pd.to_numeric(df["Low"], errors="coerce")
        vol = pd.to_numeric(df["Volume"], errors="coerce")

        prev = close.shift(1)
        pct = (close - prev) / prev * 100
        ma5 = close.rolling(5).mean()
        ma20 = close.rolling(20).mean()
        ma60 = close.rolling(60).mean()
        high20 = high.rolling(20).max()
        low20 = low.rolling(20).min()
        vol_ma20 = vol.rolling(20).mean()
        vol_ratio = vol / vol_ma20
        pos20 = (close - low20) / (high20 - low20) * 100

        tr = pd.concat([
            high - low,
            (high - prev).abs(),
            (low - prev).abs()
        ], axis=1).max(axis=1)
        atr = tr.rolling(14).mean()

        if kind == "intraday_long":
            sig = (close > ma5) & (ma5 >= ma20) & (pos20 >= 60) & (vol_ratio >= 0.90) & (pct >= -1.0)
            target = close + atr.fillna(close * 0.015) * 0.60
            stop = close - atr.fillna(close * 0.015) * 0.45
            success = ((high.shift(-1) >= target) & (low.shift(-1) > stop)) | (close.shift(-1) > close)

        elif kind == "intraday_short":
            sig = (close < ma5) & (ma5 <= ma20) & (pos20 <= 45) & (vol_ratio >= 0.90) & (pct <= 1.0)
            target = close - atr.fillna(close * 0.015) * 0.60
            stop = close + atr.fillna(close * 0.015) * 0.45
            success = ((low.shift(-1) <= target) & (high.shift(-1) < stop)) | (close.shift(-1) < close)

        else:
            sig = (vol_ratio >= 0.85) & (pos20 >= 45) & (pct >= -2.5) & (close >= ma20 * 0.96)
            target = close + atr.fillna(close * 0.015) * 0.50
            success = (high.shift(-1) >= target) | (close.shift(-1) > close)

        # 只看最近 lookback 根，且排除最後一根，因為需要隔日結果。
        start = max(60, len(df) - lookback)
        valid = sig.iloc[start:-1].fillna(False)
        samples = int(valid.sum())

        if samples <= 0:
            return 50.0, 0

        wins = int(success.iloc[start:-1][valid].fillna(False).sum())
        return wins / samples * 100, samples

    except Exception:
        return 50.0, 0



_INTRADAY_LEVEL_CACHE = {}


def _normalize_intraday_df(df_i):
    if df_i is None or df_i.empty:
        return None

    if isinstance(df_i.columns, pd.MultiIndex):
        df_i.columns = [c[0] if isinstance(c, tuple) else c for c in df_i.columns]

    need_cols = ["High", "Low", "Close"]
    for c in need_cols:
        if c not in df_i.columns:
            return None

    df_i = df_i[need_cols].copy()
    for c in need_cols:
        df_i[c] = pd.to_numeric(df_i[c], errors="coerce")

    df_i = df_i.dropna()
    if df_i.empty:
        return None

    idx = pd.to_datetime(df_i.index)
    try:
        if idx.tz is None:
            idx = idx.tz_localize("Asia/Taipei")
        else:
            idx = idx.tz_convert("Asia/Taipei")
    except Exception:
        pass

    df_i.index = idx
    return df_i


def get_yf_intraday(code, market, interval="5m", period="5d"):
    """
    只針對 TOP5 計算分K，不對30檔全部抓分K。
    加上快取，當沖多/當沖空連續查詢時速度會比較快。
    """
    cache_key = (str(code), str(market), str(interval), str(period))
    cached = _INTRADAY_LEVEL_CACHE.get(cache_key)
    now = time.time()

    # 快取 10 分鐘，避免短時間重複查詢拖慢速度
    if cached and now - cached.get("ts", 0) < 600:
        return cached.get("df")

    symbols = yahoo_symbols_by_meta(code, market)

    for symbol in symbols:
        try:
            df_i = yf.download(
                symbol,
                period=period,
                interval=interval,
                progress=False,
                auto_adjust=False,
                threads=False,
                prepost=False
            )
            df_i = _normalize_intraday_df(df_i)

            if df_i is not None and not df_i.empty:
                _INTRADAY_LEVEL_CACHE[cache_key] = {"ts": now, "df": df_i}
                return df_i

        except Exception as e:
            print(f"分K抓取失敗 {code} {symbol} {interval}: {e}", flush=True)
            continue

    return None


def _regular_session_only(df_i):
    """
    台股一般交易時段 09:00~13:30。
    用分K支撐壓力計算，避免盤前盤後資料干擾。
    """
    if df_i is None or df_i.empty:
        return df_i

    start_t = datetime.strptime("09:00", "%H:%M").time()
    end_t = datetime.strptime("13:30", "%H:%M").time()

    try:
        return df_i[
            (df_i.index.time >= start_t) &
            (df_i.index.time <= end_t)
        ]
    except Exception:
        return df_i


def calc_intraday_5m_30m_plan(code, market, kind, close, high20, low20, atr):
    """
    V5.8 職業級盤中價位算法：
    - 進場價：用 5分K 支撐/壓力計算
      當沖多/隔日沖：靠近 5分K 支撐承接
      當沖空：跌破 5分K 支撐轉弱
    - 停利價：用 5分K 壓力/支撐與 0.6R 交叉計算
      做多：停利至少要有 0.6R 正報酬，且參考 5分K 壓力
      做空：停利至少要有 0.6R 正報酬，且參考 5分K 支撐
    - 停損價：用 30分K 支撐/壓力
      做多：跌破30分K支撐停損
      做空：站回30分K壓力停損

    注意：
    這是分K技術估算，實戰仍要看1分K轉折、量能與撮合速度。
    """
    t = tick_size(close)
    df5 = get_yf_intraday(code, market, interval="5m", period="5d")
    df30 = get_yf_intraday(code, market, interval="30m", period="10d")

    df5 = _regular_session_only(df5)
    df30 = _regular_session_only(df30)

    if df5 is not None and not df5.empty:
        # 近 36 根 5分K，大約近半天到一天的短線支撐壓力
        recent5 = df5.tail(36)
        support5 = float(recent5["Low"].min())
        resistance5 = float(recent5["High"].max())
        last5 = float(recent5["Close"].iloc[-1])
    else:
        support5 = float(low20)
        resistance5 = float(high20)
        last5 = float(close)

    if df30 is not None and not df30.empty:
        # 近 8 根 30分K，大約近 1~2 個交易日的關鍵支撐壓力
        recent30 = df30.tail(8)
        support30 = float(recent30["Low"].min())
        resistance30 = float(recent30["High"].max())
    else:
        support30 = float(low20)
        resistance30 = float(high20)

    # 避免支撐/壓力過於貼近導致停損停利不合理
    min_gap = max(float(atr) * 0.20, t * 3)

    if kind == "intraday_short":
        # 當沖空：跌破 5分K 支撐進場，站回 30分K 壓力停損
        entry_raw = min(support5, last5)
        entry = round_price_by_tick(entry_raw, "down")

        stop_raw = max(resistance30, entry + min_gap)
        stop = round_price_by_tick(stop_raw, "up")

        risk = max(abs(stop - entry), min_gap)
        target_by_r = entry - risk * 0.60
        target_by_5m = support5 - max((resistance5 - support5) * 0.30, t * 2)
        take_profit = round_price_by_tick(min(target_by_r, target_by_5m), "down")

    else:
        # 當沖多 / 隔日沖：靠 5分K 支撐承接，跌破 30分K 支撐停損
        entry_raw = max(support5, support30)
        # 若支撐價高於現價太多，改用現價附近的 5分K 支撐，避免追太高
        if entry_raw > close:
            entry_raw = min(support5, close)

        entry = round_price_by_tick(entry_raw, "nearest")

        stop_raw = min(support30, entry - min_gap)
        stop = round_price_by_tick(stop_raw, "down")

        risk = max(abs(entry - stop), min_gap)
        target_by_r = entry + risk * 0.60
        target_by_5m = resistance5
        # 停利必須讓帳面至少 0.6R 為正，同時參考5分K壓力
        take_profit = round_price_by_tick(max(target_by_r, target_by_5m), "up")

    return {
        "entry": entry,
        "take_profit": take_profit,
        "stop": stop,
        "support5": support5,
        "resistance5": resistance5,
        "support30": support30,
        "resistance30": resistance30,
    }

def calc_take_profit_by_60_percent(entry, stop, kind):
    """
    停利價：以風險距離的0.6倍當作帳面正報酬停利。
    並套入台股 Tick，避免出現不能掛單的小數價。
    """
    entry = float(entry)
    stop = float(stop)
    t = tick_size(entry)

    if kind == "intraday_short":
        risk = abs(stop - entry)
        reward = max(risk * 0.60, t * 2)
        return round_price_by_tick(entry - reward, "down")

    risk = abs(entry - stop)
    reward = max(risk * 0.60, t * 2)
    return round_price_by_tick(entry + reward, "up")



def build_quantum_trade_plan(code, market, kind, close, high20, low20, atr):
    """
    V5.8：
    改用 5分K 支撐壓力計算進場與停利。
    停損改用 30分K 支撐/壓力。
    """
    try:
        plan = calc_intraday_5m_30m_plan(
            code=code,
            market=market,
            kind=kind,
            close=close,
            high20=high20,
            low20=low20,
            atr=atr
        )

        return {
            "entry": plan["entry"],
            "take_profit": plan["take_profit"],
            "stop": plan["stop"],
            "basis": "",
            "support_1230": None,
            "resistance_1230": None,
            "support5": plan.get("support5"),
            "resistance5": plan.get("resistance5"),
            "support30": plan.get("support30"),
            "resistance30": plan.get("resistance30"),
        }

    except Exception as e:
        print(f"V5.8 分K交易計畫失敗 {code}: {e}", flush=True)

    # 備援：若分K抓不到，至少回傳日K可用價位
    t = tick_size(close)
    if kind == "intraday_short":
        entry = round_price_by_tick(min(low20, close), "down")
        stop = round_price_by_tick(entry + max(atr * 0.70, t * 3), "up")
        take_profit = calc_take_profit_by_60_percent(entry, stop, kind)
    else:
        entry = round_price_by_tick(max(low20, close - atr * 0.50), "nearest")
        stop = round_price_by_tick(entry - max(atr * 0.70, t * 3), "down")
        take_profit = calc_take_profit_by_60_percent(entry, stop, kind)

    return {
        "entry": entry,
        "take_profit": take_profit,
        "stop": stop,
        "basis": "",
        "support_1230": None,
        "resistance_1230": None,
        "support5": None,
        "resistance5": None,
        "support30": None,
        "resistance30": None,
    }


def score_quantum_candidate(df, kind, code=None, market=None):
    """
    V5.9：保留每檔日K特徵，排名改由「選股日報核心」做橫向比較。
    這裡只做單檔資料整理，不在會員訊息中揭露完整算法。
    """
    close_series = pd.to_numeric(df["Close"], errors="coerce")
    high_series = pd.to_numeric(df["High"], errors="coerce")
    low_series = pd.to_numeric(df["Low"], errors="coerce")
    vol_series = pd.to_numeric(df["Volume"], errors="coerce")

    close = float(close_series.iloc[-1])
    prev = float(close_series.iloc[-2])
    pct = (close - prev) / prev * 100 if prev else 0

    day_high = float(high_series.iloc[-1])
    day_low = float(low_series.iloc[-1])
    amplitude = (day_high - day_low) / close * 100 if close else 0

    ma5 = float(close_series.rolling(5).mean().iloc[-1])
    ma10 = float(close_series.rolling(10).mean().iloc[-1])
    ma20 = float(close_series.rolling(20).mean().iloc[-1])
    ma60 = float(close_series.rolling(60).mean().iloc[-1])
    high20 = float(high_series.tail(20).max())
    low20 = float(low_series.tail(20).min())
    pos20 = (close - low20) / (high20 - low20) * 100 if high20 > low20 else 50

    volume = float(vol_series.iloc[-1]) if not pd.isna(vol_series.iloc[-1]) else 0.0
    vol_ma20 = float(vol_series.rolling(20).mean().iloc[-1])
    vol_ratio = float(volume / vol_ma20) if vol_ma20 > 0 else 0
    volume_lots = volume_shares_to_lots(volume)
    value_m = calc_value_m(close, volume)
    atr = calc_atr14(high_series, low_series, close_series)

    win_rate, samples = estimate_strategy_win_rate(df, kind if kind != "intraday_best" else "intraday_long")

    return {
        "close": close,
        "pct": pct,
        "volume": volume,
        "volume_lots": volume_lots,
        "value_m": value_m,
        "day_high": day_high,
        "day_low": day_low,
        "amplitude": amplitude,
        "ma5": ma5,
        "ma10": ma10,
        "ma20": ma20,
        "ma60": ma60,
        "pos20": pos20,
        "vol_ratio": vol_ratio,
        "atr": atr,
        "high20": high20,
        "low20": low20,
        "win_rate": win_rate,
        "samples": samples,
        "score": 0.0,
        "rank_score": 0.0,
        "entry": None,
        "take_profit": None,
        "stop": None,
        "basis": "",
        "signal": "職業當沖觀察",
        "trade_kind": kind,
    }


def _percentile_list(values):
    s = pd.to_numeric(pd.Series(values), errors="coerce").replace([float("inf"), float("-inf")], pd.NA).fillna(0)
    if len(s) <= 1 or s.nunique() <= 1:
        return [50.0] * len(s)
    return (s.rank(pct=True) * 100).tolist()


def _clip_score(x, lo=0, hi=100):
    try:
        return max(lo, min(hi, float(x)))
    except Exception:
        return 0.0


def _healthy_momentum_score(pct, side):
    """把太極端的漲跌停附近降權，優先選好操作、不容易一開盤就失控的股票。"""
    pct = float(pct or 0)
    if side == "long":
        if 0.8 <= pct <= 5.8:
            return 100.0
        if -1.5 <= pct < 0.8:
            return _clip_score((pct + 1.5) / 2.3 * 82)
        if 5.8 < pct <= 8.8:
            return _clip_score((8.8 - pct) / 3.0 * 85)
        return 20.0 if pct > 8.8 else 5.0
    if side == "short":
        ap = -pct
        if 0.8 <= ap <= 5.8:
            return 100.0
        if -1.5 <= ap < 0.8:
            return _clip_score((ap + 1.5) / 2.3 * 82)
        if 5.8 < ap <= 8.8:
            return _clip_score((8.8 - ap) / 3.0 * 85)
        return 20.0 if ap > 8.8 else 5.0
    return 50.0


def _operability_score(row):
    """職業當沖可操作性：流動性、波動、價格級距、追價風險。"""
    close = float(row.get("close") or 0)
    amplitude = abs(float(row.get("amplitude") or 0))
    vol_ratio = float(row.get("vol_ratio") or 0)
    value_m = float(row.get("value_m") or 0)

    if 15 <= close <= 300:
        price_score = 100
    elif 8 <= close < 15 or 300 < close <= 650:
        price_score = 72
    else:
        price_score = 45

    if 1.2 <= amplitude <= 7.5:
        amp_score = 100
    elif amplitude < 1.2:
        amp_score = _clip_score(amplitude / 1.2 * 80)
    elif amplitude <= 12:
        amp_score = _clip_score((12 - amplitude) / 4.5 * 85)
    else:
        amp_score = 25

    vol_ratio_score = _clip_score(vol_ratio / 2.2 * 100)
    value_score = _clip_score(value_m / 350 * 100)
    tick_risk = tick_size(close) / close * 100 if close else 5
    tick_score = 100 if tick_risk <= 0.18 else 75 if tick_risk <= 0.35 else 50

    return price_score * 0.20 + amp_score * 0.25 + vol_ratio_score * 0.25 + value_score * 0.20 + tick_score * 0.10



def _radar_score_peak(value, good_low, good_high, weak_low, weak_high, default=50.0):
    """
    參考量子雷達隔日沖模型的 peak score：
    分數高點落在合理區間，太弱或太過熱都降權。
    """
    try:
        v = float(value)
    except Exception:
        return float(default)

    if good_low <= v <= good_high:
        return 100.0
    if weak_low <= v < good_low:
        return _clip_score((v - weak_low) / max(good_low - weak_low, 1e-9) * 100.0)
    if good_high < v <= weak_high:
        return _clip_score((weak_high - v) / max(weak_high - good_high, 1e-9) * 100.0)
    return float(default)


def _quantum_daily_swing_score(row, volume_score, value_score):
    """
    量子雷達隔日沖核心：
    流動性 + 收盤位置 + 健康漲幅 + 振幅控制 + 均線 / 量比加權。
    """
    pct = float(row.get("pct") or 0)
    pos20 = _clip_score(row.get("pos20", 50))
    amplitude = abs(float(row.get("amplitude") or 0))
    vol_ratio = float(row.get("vol_ratio") or 0)

    liquidity = float(volume_score) * 0.52 + float(value_score) * 0.48
    momentum = _radar_score_peak(pct, 1.2, 5.8, -0.5, 9.4, default=45.0)
    amp_score = _radar_score_peak(amplitude, 1.2, 6.8, 0.1, 12.0, default=55.0)
    volume_ratio_score = _radar_score_peak(vol_ratio, 1.20, 4.50, 0.55, 8.00, default=50.0)

    close = float(row.get("close") or 0)
    ma5 = float(row.get("ma5") or 0)
    ma10 = float(row.get("ma10") or 0)
    ma20 = float(row.get("ma20") or 0)

    trend_checks = [
        close > ma5 if ma5 > 0 else False,
        close > ma10 if ma10 > 0 else False,
        close > ma20 if ma20 > 0 else False,
        ma5 >= ma10 if ma5 > 0 and ma10 > 0 else False,
        ma10 >= ma20 if ma10 > 0 and ma20 > 0 else False,
    ]
    trend_score = sum(bool(x) for x in trend_checks) / max(len(trend_checks), 1) * 100.0

    score = (
        liquidity * 0.24 +
        pos20 * 0.25 +
        momentum * 0.24 +
        amp_score * 0.11 +
        volume_ratio_score * 0.08 +
        trend_score * 0.08
    )

    # 風險扣分：不追過熱、不追弱收盤，貼近原量子雷達的風控精神。
    if pct <= 0:
        score -= 18
    if pos20 < 55:
        score -= 12
    if pct >= 8.8:
        score -= 14
    if amplitude >= 9.5:
        score -= 10

    return round(_clip_score(score), 1)





def apply_hcx_daily_radar_ranking(command, rows):
    """
    V6.5：
    使用量子雷達多方/空方/隔日沖核心分數排序。
    先嚴格，後保底；避免會員查詢「當沖多、當沖空、隔日沖」都沒有結果。
    """
    if not rows:
        return []

    vols = [float(r.get("volume_lots") or 0) for r in rows]
    vals = [float(r.get("value_m") or 0) for r in rows]
    volume_scores = _percentile_list(vols)
    value_scores = _percentile_list(vals)

    scored = []

    for r, volume_score, value_score in zip(rows, volume_scores, value_scores):
        pct = float(r.get("pct") or 0)
        pos20 = _clip_score(r.get("pos20", 50))
        oper = _operability_score(r)
        win_rate = float(r.get("win_rate") or 50)
        samples = int(r.get("samples") or 0)
        sample_factor = min(samples / 12, 1.0)

        long_pct_score = _clip_score(max(pct, 0) / 10 * 100)
        short_pct_score = _clip_score(max(-pct, 0) / 10 * 100)

        long_score = (
            float(volume_score) * 0.28 +
            float(value_score) * 0.22 +
            long_pct_score * 0.30 +
            pos20 * 0.20
        )

        short_score = (
            float(volume_score) * 0.28 +
            float(value_score) * 0.22 +
            short_pct_score * 0.30 +
            (100 - pos20) * 0.20
        )

        swing_score = _quantum_daily_swing_score(r, volume_score, value_score)

        pro_long = long_score * 0.78 + oper * 0.14 + win_rate * (0.08 * sample_factor)
        pro_short = short_score * 0.78 + oper * 0.14 + win_rate * (0.08 * sample_factor)
        pro_swing = swing_score * 0.80 + oper * 0.10 + win_rate * (0.10 * sample_factor)

        rr = dict(r)
        rr["volume_score"] = volume_score
        rr["value_score"] = value_score
        rr["long_score"] = round(long_score, 1)
        rr["short_score"] = round(short_score, 1)
        rr["swing_score"] = round(swing_score, 1)
        rr["operability_score"] = round(oper, 1)

        if command == "當沖多":
            rr["score"] = round(long_score, 1)
            rr["rank_score"] = round(pro_long, 1)
            rr["trade_kind"] = "intraday_long"
            rr["signal"] = "🟥 偏多當沖"
            rr["_strict_keep"] = (long_score >= 55 and pct >= -2.5 and pos20 >= 40 and oper >= 30)

        elif command == "當沖空":
            rr["score"] = round(short_score, 1)
            rr["rank_score"] = round(pro_short, 1)
            rr["trade_kind"] = "intraday_short"
            rr["signal"] = "🟩 偏空當沖"
            rr["_strict_keep"] = (short_score >= 55 and pct <= 2.5 and pos20 <= 60 and oper >= 30)

        elif command == "隔日沖":
            rr["score"] = round(swing_score, 1)
            rr["rank_score"] = round(pro_swing, 1)
            rr["trade_kind"] = "swing"
            rr["signal"] = "🟧 隔日沖觀察"
            rr["_strict_keep"] = (swing_score >= 50 and oper >= 25)

        elif command == "當沖股":
            if long_score >= short_score:
                rr["score"] = round(long_score, 1)
                rr["rank_score"] = round(pro_long, 1)
                rr["trade_kind"] = "intraday_long"
                rr["signal"] = "🟥 偏多當沖"
                rr["_strict_keep"] = (long_score >= 52 and oper >= 25)
            else:
                rr["score"] = round(short_score, 1)
                rr["rank_score"] = round(pro_short, 1)
                rr["trade_kind"] = "intraday_short"
                rr["signal"] = "🟩 偏空當沖"
                rr["_strict_keep"] = (short_score >= 52 and oper >= 25)
        else:
            continue

        scored.append(rr)

    def _sort_key(x):
        return (
            float(x.get("score") or 0),
            float(x.get("rank_score") or 0),
            float(x.get("value_m") or 0),
            float(x.get("volume_lots") or 0),
        )

    top_n = get_quantum_top_n()

    # 第一層：嚴格條件
    strict = [r for r in scored if r.get("_strict_keep")]
    strict = sorted(strict, key=_sort_key, reverse=True)
    if len(strict) >= top_n:
        return strict[:get_quantum_deep_limit()]

    # 第二層：每日可出榜保底
    loose = [r for r in scored if float(r.get("score") or 0) >= 20]
    loose = sorted(loose, key=_sort_key, reverse=True)
    return loose[:get_quantum_deep_limit()]


def format_quantum_top_report(command, rows):
    title_map = {
        "當沖多": "🔴 當沖多 TOP 5｜職業操盤精選",
        "當沖空": "🟢 當沖空 TOP 5｜職業操盤精選",
        "隔日沖": "🟠 隔日沖 TOP 5｜選股日報精選",
        "當沖股": "⚡ 最佳當沖股 TOP 5｜多空綜合精選",
    }

    if not rows:
        return f"""⚡ HCX-AI量子雷達
🕒 查詢時間：{query_time_text()}
📌 指令：{command}

本次暫無適合出手的股票。
可稍後盤中再查，或等待量能與方向更明確。
"""

    lines = [
        "⚡ HCX-AI量子雷達",
        f"🕒 查詢時間：{query_time_text()}",
        f"{title_map.get(command, command)}",
        "━━━━━━━━━━━━━━",
    ]

    for idx, r in enumerate(rows[:get_quantum_top_n()], 1):
        direction = r.get("signal", "職業當沖觀察")
        lines.append(
            f"{idx}. {r['code']} {r['name']}｜{direction}\n"
            f"   收盤 {fmt_price(r['close'])}｜漲跌 {r['pct']:+.2f}%｜量比 {r['vol_ratio']:.2f}\n"
            f"   🏆 AI勝率 {r['win_rate']:.1f}%｜樣本 {r['samples']}｜職業評分 {r['rank_score']:.1f}\n"
            f"   🎯 建議進場價：{fmt_price(r['entry'])}\n"
            f"   ✅ 建議停利價：{fmt_price(r['take_profit'])}\n"
            f"   🛑 建議停損價：{fmt_price(r['stop'])}\n"
            f"{format_tick_profit_line(r)}"
        )

    lines.extend([
        "━━━━━━━━━━━━━━",
        "⚠️ 此為HCX-AI量子雷達篩選結果，不保證獲利。",
        "⚠️ 當沖實戰請搭配1分K轉折、量能與停損紀律。",
    ])

    text = "\n".join(lines)
    return text[:4800]


def get_quantum_workers():
    """
    Render 免費機不要開太大，避免 yfinance 擋流量或 CPU 爆掉。
    """
    try:
        n = int(os.environ.get("QUANTUM_WORKERS", "6"))
        return max(3, min(n, 8))
    except Exception:
        return 6


def build_official_fallback_metrics(item, kind):
    """
    官方資料保底：
    如果 yfinance / TWSE 月線資料暫時失敗，不要整個指令空榜。
    以官方收盤與成交量做簡易估算，讓每日提醒至少能出榜。
    """
    code = item.get("code")
    close = float(item.get("close") or 0)
    volume = float(item.get("volume") or 0)
    volume_lots = float(item.get("volume_lots") or 0)
    value_m = float(item.get("official_value_m") or 0)
    t = tick_size(close)

    if close <= 0:
        return None

    high20 = close * 1.035
    low20 = close * 0.965
    atr = max(close * 0.025, t * 3)
    pct = 0.0
    pos20 = 50.0
    vol_ratio = 1.0 if volume_lots > 0 else 0.5

    win_rate = 50.0
    samples = 0

    return {
        "close": close,
        "pct": pct,
        "volume": volume,
        "volume_lots": volume_lots,
        "value_m": value_m,
        "day_high": close,
        "day_low": close,
        "amplitude": 2.5,
        "ma5": close,
        "ma10": close,
        "ma20": close,
        "ma60": close,
        "pos20": pos20,
        "vol_ratio": vol_ratio,
        "atr": atr,
        "high20": high20,
        "low20": low20,
        "win_rate": win_rate,
        "samples": samples,
        "score": 0.0,
        "rank_score": 0.0,
        "entry": None,
        "take_profit": None,
        "stop": None,
        "basis": "",
        "signal": "職業當沖觀察",
        "trade_kind": kind,
    }


def process_quantum_item(item, kind):
    """
    單檔日K整理，供 ThreadPoolExecutor 並行使用。
    V6.5 修正：若下載日K失敗，改用官方資料保底，不讓三個指令全部空榜。
    """
    code = item["code"]
    meta = get_stock_meta(code)

    try:
        df, source = get_stock_data(code, meta["market"])
    except Exception as e:
        print(f"日K下載失敗 {code}: {e}", flush=True)
        df, source = None, None

    if df is not None and not df.empty and len(df) >= 60:
        metrics = score_quantum_candidate(df, kind, code=code, market=meta["market"])
    else:
        metrics = build_official_fallback_metrics(item, kind)
        if metrics is None:
            return None

    # 與官方初選資料合併，避免 yfinance volume 偶發缺值
    official_lots = float(item.get("volume_lots") or 0)
    official_value_m = float(item.get("official_value_m") or 0)
    metrics["volume_lots"] = max(float(metrics.get("volume_lots") or 0), official_lots)
    metrics["value_m"] = max(float(metrics.get("value_m") or 0), official_value_m)

    if metrics.get("close", 0) < 5:
        return None

    return {
        "code": code,
        "name": meta["name"],
        "market": meta["market"],
        **metrics,
    }



def attach_trade_plan_to_top_rows(rows, kind):
    """
    只對前5名計算分K價位，且改成並行處理。
    速度會比一檔一檔慢慢抓快很多。
    """
    top_n = get_quantum_top_n()
    top_rows = rows[:top_n]

    if not top_rows:
        return []

    workers = min(5, max(1, len(top_rows)))
    final_rows = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(_attach_one_trade_plan, dict(r), kind) for r in top_rows]
        for fut in as_completed(futures):
            try:
                final_rows.append(fut.result())
            except Exception as e:
                print(f"TOP5分K計畫略過：{e}", flush=True)

    # 並行完成順序不固定，重新依排名分數排序
    final_rows = sorted(final_rows, key=lambda x: (x["rank_score"], x["win_rate"], x["score"]), reverse=True)
    return final_rows


def run_quantum_scan(command):
    kind = QUANTUM_COMMANDS.get(command)
    if not kind:
        return "指令錯誤，請輸入：當沖股、當沖多、當沖空、隔日沖"

    # B方案：600 → 100 → 30 → 5
    universe600 = get_quantum_universe()
    universe100 = select_stage1_candidates(universe600, command)

    results = []

    workers = get_quantum_workers()
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = [executor.submit(process_quantum_item, item, kind) for item in universe100]
        for fut in as_completed(futures):
            try:
                r = fut.result()
                if r is not None:
                    results.append(r)
            except Exception as e:
                print(f"量子並行掃描略過：{e}", flush=True)
                continue

    # 若日K下載全部失敗，改用官方資料保底，避免空榜
    if not results:
        for item in universe100[:get_quantum_stage1_limit()]:
            r = process_quantum_item(item, kind)
            if r is not None:
                results.append(r)

    # 第二層：量子雷達分數排序
    results = apply_hcx_daily_radar_ranking(command, results)

    # 第三層：TOP5 補分K進場/停利/停損與Tick提醒
    results = attach_trade_plan_to_top_rows(results, kind)
    return format_quantum_top_report(command, results)


def run_quantum_scan_and_push(user_id, command):
    try:
        text = run_quantum_scan(command)
        push_text_message(user_id, text)
    except Exception as e:
        traceback.print_exc()
        try:
            push_text_message(
                user_id,
                f"⚠️ {command} 掃描發生錯誤：{e}\n請稍後再試，或先確認 Render 是否已部署 V6.1 正式版。"
            )
        except Exception:
            pass



def start_quantum_scan(user_id, command):
    """
    LINE Reply Token 有時間限制，所以先回覆「已開始掃描」，
    實際排名完成後用 push message 推回。
    """
    t = threading.Thread(
        target=run_quantum_scan_and_push,
        args=(user_id, command),
        daemon=True
    )
    t.start()

    return f"""⚡ 已收到「{command}」指令

系統正在啟動 HCX-AI 量子雷達篩選中...

📊 掃描模式：選股日報核心 TOP 5
🕒 查詢時間：{query_time_text()}

稍後會自動推播結果給你。
"""


@app.route("/")
def home():
    return f"HCX AI LINE BOT 運作中｜{APP_VERSION}"


@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", "")
    body = request.get_data(as_text=True)

    print("========== LINE Webhook 收到資料 ==========", flush=True)
    print(body, flush=True)

    try:
        handler.handle(body, signature)
    except InvalidSignatureError:
        print("簽章錯誤 InvalidSignatureError", flush=True)
        return "Bad Signature", 400
    except Exception as e:
        print("callback 發生錯誤：", str(e), flush=True)
        traceback.print_exc()
        return "OK", 200

    return "OK"


@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    print("========== 收到使用者訊息 ==========", flush=True)
    print(event.message.text, flush=True)

    user_id = get_event_user_id(event)
    source_type = get_event_source_type(event)

    raw_msg = event.message.text.strip()
    msg = raw_msg.replace("\n", "").replace("/", "").replace("股票", "").strip()
    quantum_command = detect_quantum_command(raw_msg)
    reminder_command = parse_reminder_command(raw_msg)

    print(f"來源類型 source_type={source_type}", flush=True)
    print(f"使用者 user_id={user_id}", flush=True)
    print(f"量子指令 quantum_command={quantum_command}", flush=True)
    print(f"提醒指令 reminder_command={reminder_command}", flush=True)

    # 支援：2330、/2330、股票2330、請查2330
    match = re.search(r"(\d{4})", msg)

    try:
        # 先處理不需要會員權限的基本指令
        if msg in ["版本", "version", "Version"]:
            reply = f"""🌈 HCX AI 股票分析師

目前版本：
{APP_VERSION}

會員限制：
{"已啟用 🔒" if MEMBER_ONLY_MODE else "未啟用 🔓"}

你的ID：
{user_id or "無法取得 userId"}
"""

        elif msg in ["我的ID", "我的id", "ID", "id", "會員ID", "會員id"]:
            reply = f"""🪪 HCX AI 會員識別ID

你的 LINE userId：

{user_id or "無法取得 userId"}

請把這組 ID 傳給管理員開通會員權限。
"""

        elif "更新名稱" in msg or "清除快取" in msg:
            # 更新名稱只有管理員可用；未設定會員模式時允許使用
            if MEMBER_ONLY_MODE and user_id not in get_admin_user_ids():
                reply = "🔒 此指令限管理員使用。"
            else:
                fetch_market_meta(force=True)
                reply = "✅ 已重新抓取 TWSE / TPEx 官方股票名稱快取。請再輸入股票代號測試。"

        else:
            # 群組 / 多人聊天室不建議開放，避免會員內容被轉傳到群組
            if source_type and source_type != "user":
                reply = """🔒 HCX AI 會員限定提醒

本服務限定「一對一好友聊天室」使用。
請不要在群組或多人聊天室查詢，避免會員內容外流。
"""

            # 會員白名單檢查
            elif not is_authorized_user(user_id):
                reply = member_block_message(user_id)

            elif reminder_command:
                reply = handle_reminder_command(user_id, reminder_command)

            elif quantum_command in QUANTUM_COMMANDS:
                reply = start_quantum_scan(user_id, quantum_command)

            elif match:
                code = match.group(1)
                reply = stock_ai(code)

            else:
                reply = f"""🌈 HCX AI 股票分析師

請輸入 4 碼股票代號，例如：

🚀 2330 台積電
⚡ 2454 聯發科
🏭 2317 鴻海
🧪 1717 長興

我會幫你分析：
✅ 股票名稱
✅ 現價與漲跌幅
✅ 均線趨勢
✅ 主力成本估算
✅ 支撐壓力
✅ 做多價位
✅ 做空價位
✅ 停損點
✅ 目標價

量子選股指令：
⚡ 輸入「當沖股」：列出最好操作的當沖股 TOP 5
🔴 輸入「當沖多」：列出當沖多 TOP 5
🟢 輸入「當沖空」：列出當沖空 TOP 5
🟠 輸入「隔日沖」：列出隔日沖 TOP 5

自動提醒指令：
⏰ 設定提醒 當沖多 08:50
⏰ 設定提醒 當沖空 09:00
⏰ 設定提醒 隔日沖 13:35
📋 我的提醒
🗑️ 取消提醒 當沖多

也支援：
/當沖股、今日當沖、最佳當沖、當沖
/當沖多、當沖 多、當衝多、我要當沖多
/當沖空、當沖 空、當衝空、我要當沖空
/隔日沖、隔日衝

指令：
輸入「版本」可確認目前是否已部署最新版。
輸入「我的ID」可取得會員開通用 ID。
"""

        print("========== 準備回覆 ==========", flush=True)
        print(reply, flush=True)

        with ApiClient(configuration) as api_client:
            line_bot_api = MessagingApi(api_client)
            line_bot_api.reply_message(
                ReplyMessageRequest(
                    reply_token=event.reply_token,
                    messages=[TextMessage(text=reply)]
                )
            )

        print("========== 回覆成功 ==========", flush=True)

    except Exception as e:
        print("handle_message 發生錯誤：", str(e), flush=True)
        traceback.print_exc()


# 啟動會員提醒排程
start_reminder_scheduler()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
