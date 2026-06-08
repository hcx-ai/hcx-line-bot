from flask import Flask, request
from linebot.v3.webhook import WebhookHandler
from linebot.v3.messaging import (
    Configuration,
    ApiClient,
    MessagingApi,
    ReplyMessageRequest,
    TextMessage
)
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.webhooks import MessageEvent, TextMessageContent

import os
import re
import math
import time
import traceback
from datetime import datetime, timezone, timedelta
from io import StringIO

import pandas as pd
import yfinance as yf
import requests


# ============================================================
# HCX AI 股票分析師 LINE Bot
# V5 黑暗量子雷達強化版
# 重點：
# 1. 參考黑暗量子雷達：先抓 TWSE / TPEx 官方全市場資料，代號與名稱一起建立快取
# 2. 股票名稱不再只靠 yfinance，避免 2330 2330、1717 1717
# 3. 加入主力成本估算、支撐壓力、台股 Tick 合法價位
# 4. 加入做多 / 做空價位說明
# ============================================================

APP_VERSION = "V5.3.1 黑暗量子雷達強化版｜只顯示查詢時間＋職業級成本雷達"

app = Flask(__name__)

configuration = Configuration(access_token=os.environ["LINE_CHANNEL_ACCESS_TOKEN"])
handler = WebhookHandler(os.environ["LINE_CHANNEL_SECRET"])

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
    參考黑暗量子雷達邏輯：
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
🧠 算法模式：{cost_info["mode"]}
🧩 籌碼判斷：{chip_status}

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
🧮【台股 Tick 價格修正】
目前股價級距 Tick：{fmt_price(current_tick)} 元
✅ 下方進出場價位已自動修正成可掛單價位
✅ 千元股會用 5 元跳動，不會再出現不合法小數價

━━━━━━━━━━━━━━
{plan}

━━━━━━━━━━━━━━
⚠️ 風險提醒
本訊息為程式估算與技術分析，不代表保證獲利。
主力成本為5/10/20日VWAP與大量成交區加權估算，非券商實際持股成本。
"""

    except Exception as e:
        print("stock_ai 發生錯誤：", str(e), flush=True)
        traceback.print_exc()
        return f"查詢 {code} 時發生錯誤，請稍後再試。"


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

    raw_msg = event.message.text.strip()
    msg = raw_msg.replace("\n", "").replace("/", "").replace("股票", "").strip()

    # 支援：2330、/2330、股票2330、請查2330
    match = re.search(r"(\d{4})", msg)

    try:
        if match:
            code = match.group(1)
            reply = stock_ai(code)
        elif "版本" in msg:
            reply = f"HCX AI 股票分析師目前版本：{APP_VERSION}"
        elif "更新名稱" in msg or "清除快取" in msg:
            fetch_market_meta(force=True)
            reply = "✅ 已重新抓取 TWSE / TPEx 官方股票名稱快取。請再輸入股票代號測試。"
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
✅ 台股 Tick 合法價位

指令：
輸入「版本」可確認目前是否已部署最新版。
輸入「更新名稱」可重新抓取官方股票名稱快取。
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
