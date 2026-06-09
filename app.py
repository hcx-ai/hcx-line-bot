# -*- coding: utf-8 -*-
"""
HCX-AI LINE 股票推播雷達 V7.0 穩定版
功能：股票查詢、當沖股/當沖多/當沖空/隔日沖、會員限制、會員自訂提醒、LINE 觸控式快速選單。
Render Start Command：gunicorn app:app --bind 0.0.0.0:$PORT
"""

import os
import re
import json
import time
import math
import traceback
import threading
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
import yfinance as yf
from flask import Flask, request, jsonify

from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, PushMessageRequest, TextMessage,
)
try:
    from linebot.v3.messaging import QuickReply, QuickReplyItem, MessageAction
    HAS_QUICK_REPLY = True
except Exception:
    HAS_QUICK_REPLY = False

try:
    from linebot.v3.messaging import FlexMessage, FlexContainer
    HAS_FLEX_MENU = True
except Exception:
    HAS_FLEX_MENU = False

from linebot.v3.webhooks import MessageEvent, TextMessageContent

APP_VERSION = "V7.5.2 穩定版"
TAIPEI_TZ = timezone(timedelta(hours=8))
app = Flask(__name__)
configuration = Configuration(access_token=os.environ.get("LINE_CHANNEL_ACCESS_TOKEN", ""))
handler = WebhookHandler(os.environ.get("LINE_CHANNEL_SECRET", ""))
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Mozilla/5.0 HCX-AI-LineBot/7.0"})

UNIVERSE_CACHE = {"ts": 0, "data": []}
DATA_CACHE = {}
INTRADAY_CACHE = {}
STOCK_NAME_CACHE = {"ts": 0, "map": {}}
US_META_CACHE = {}
SCAN_LOCK = threading.Lock()
REMINDER_FILE = Path(os.environ.get("REMINDER_SCHEDULE_FILE", "/tmp/hcx_line_reminders_v7.json"))
REMINDER_LOCK = threading.Lock()
REMINDER_SCHEDULES = {}
REMINDER_STARTED = False

FALLBACK_STOCKS = {
    "2330":"台積電","2317":"鴻海","2454":"聯發科","2881":"富邦金","2308":"台達電",
    "2382":"廣達","2412":"中華電","2882":"國泰金","2303":"聯電","3711":"日月光投控",
    "2603":"長榮","2609":"陽明","2615":"萬海","2618":"長榮航","2324":"仁寶",
    "2356":"英業達","3231":"緯創","2357":"華碩","2376":"技嘉","6669":"緯穎",
    "3017":"奇鋐","3324":"雙鴻","2368":"金像電","2383":"台光電","3037":"欣興",
    "8046":"南電","2313":"華通","2408":"南亞科","1605":"華新","1717":"長興",
    "1303":"南亞","1301":"台塑","2002":"中鋼","2891":"中信金","2886":"兆豐金",
    "2884":"玉山金","2892":"第一金","1101":"台泥","1102":"亞泥","2207":"和泰車",
    "1593":"祺驊","1580":"新麥","1781":"合世","1402":"遠東新","2327":"國巨",
}


def now_taipei(): return datetime.now(TAIPEI_TZ)
def query_time_text(): return now_taipei().strftime("%Y-%m-%d %H:%M:%S")
def clean_text(x): return str(x or "").strip()
def safe_float(x, default=0.0):
    try:
        s = str(x).replace(",", "").replace("--", "").replace("X", "").strip()
        return float(s) if s else default
    except Exception:
        return default

def safe_int(x, default=0):
    try: return int(float(str(x).replace(",", "").strip()))
    except Exception: return default

def clip(v, lo=0, hi=100):
    try: return max(lo, min(hi, float(v)))
    except Exception: return 50.0

def parse_bool_env(name, default=False):
    v = str(os.environ.get(name, str(default))).strip().lower()
    return v in ("1","true","yes","y","on")

def parse_id_list(name):
    raw = os.environ.get(name, "").replace("\n", ",").replace("，", ",").replace(";", ",")
    return {x.strip() for x in raw.split(",") if x.strip()}

def is_member_only(): return parse_bool_env("MEMBER_ONLY_MODE", False)
def is_admin_user(user_id): return user_id in parse_id_list("ADMIN_USER_IDS")
def is_authorized_user(user_id):
    if not is_member_only(): return True
    return user_id in parse_id_list("AUTHORIZED_USER_IDS") or is_admin_user(user_id)

def fmt_price(x):
    try:
        x = float(x)
        if x >= 1000: return f"{x:.0f}"
        if x >= 100: return f"{x:.1f}".rstrip("0").rstrip(".")
        return f"{x:.2f}".rstrip("0").rstrip(".")
    except Exception: return "-"

def fmt_pct(x):
    try: return f"{float(x):+.2f}%"
    except Exception: return "-"

def tick_size(price):
    price = float(price)
    if price < 10: return 0.01
    if price < 50: return 0.05
    if price < 100: return 0.10
    if price < 500: return 0.50
    if price < 1000: return 1.0
    return 5.0

def round_by_tick(price, mode="nearest"):
    price = float(price); t = tick_size(price)
    if mode == "up": return math.ceil(price / t) * t
    if mode == "down": return math.floor(price / t) * t
    return round(price / t) * t

def calc_atr14(df):
    try:
        high = pd.to_numeric(df["High"], errors="coerce")
        low = pd.to_numeric(df["Low"], errors="coerce")
        close = pd.to_numeric(df["Close"], errors="coerce")
        pc = close.shift(1)
        tr = pd.concat([(high-low).abs(), (high-pc).abs(), (low-pc).abs()], axis=1).max(axis=1)
        atr = float(tr.rolling(14).mean().iloc[-1])
        return atr if math.isfinite(atr) and atr > 0 else max(float(close.iloc[-1]) * 0.02, tick_size(close.iloc[-1]) * 3)
    except Exception:
        return 1.0

def calc_tick_profit_info(entry, take_profit):
    try:
        entry = float(entry)
        t = tick_size(entry)
        cost_rate = 0.001425 * 0.6 * 2 + 0.0015
        cost = entry * 1000 * cost_rate
        tick_profit = t * 1000
        breakeven = max(1, math.ceil(cost / max(tick_profit, 1)))
        return f"🔥 回本門檻：{breakeven} Tick"
    except Exception:
        return "🔥 回本門檻：依盤中成交價試算"


# LINE 訊息

def build_quick_reply():
    """
    LINE 底部 Quick Reply 原生是橫向滑動。
    完整九宮格請點「主選單」。
    """
    if not HAS_QUICK_REPLY:
        return None
    labels = [
        ("主選單", "主選單"),
        ("08:50沖多", "設定提醒 當沖多 08:50"),
        ("09:00沖空", "設定提醒 當沖空 09:00"),
        ("13:00隔沖", "設定提醒 隔日沖 13:00"),
    ]
    return QuickReply(items=[QuickReplyItem(action=MessageAction(label=a, text=b)) for a, b in labels])



def _flex_button(label, text, style="secondary"):
    return {
        "type": "button",
        "style": style,
        "height": "sm",
        "action": {"type": "message", "label": label, "text": text},
    }


def build_main_menu_flex():
    """
    V7.5 九宮格快捷鍵：3欄 x 3列，共9個功能，每格都有標題。
    """
    if not HAS_FLEX_MENU:
        return None

    def _row(a1, t1, a2, t2, a3, t3, s1="secondary", s2="secondary", s3="secondary"):
        return {
            "type": "box",
            "layout": "horizontal",
            "spacing": "xs",
            "contents": [
                _flex_button(a1, t1, s1),
                _flex_button(a2, t2, s2),
                _flex_button(a3, t3, s3),
            ],
        }

    bubble = {
        "type": "bubble",
        "size": "mega",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                {"type": "text", "text": "⚡ HCX-AI 量子雷達", "weight": "bold", "size": "lg"},
                {"type": "text", "text": "請直接點選下方功能", "size": "sm", "color": "#666666"},
                _row("版本號", "版本", "當沖多", "當沖多", "當沖空", "當沖空", "secondary", "primary", "primary"),
                _row("隔日沖", "隔日沖", "波段股", "波段股", "小鈴鐺", "我的提醒", "primary", "primary", "secondary"),
                _row("提醒多", "設定提醒 當沖多 08:50", "提醒空", "設定提醒 當沖空 09:00", "提醒隔", "設定提醒 隔日沖 13:00"),
                {"type": "text", "text": "也可輸入：請開機、股票代號、主選單", "size": "xs", "color": "#888888", "wrap": True},
            ],
        },
    }
    try:
        return FlexMessage(
            alt_text="HCX-AI 九宮格快捷鍵",
            contents=FlexContainer.from_json(json.dumps(bubble, ensure_ascii=False)),
        )
    except Exception as e:
        print(f"Flex選單建立失敗：{e}", flush=True)
        return None



def reply_main_menu(reply_token):
    flex = build_main_menu_flex()
    if flex is None:
        reply_text(reply_token, help_message())
        return

    with ApiClient(configuration) as api_client:
        api = MessagingApi(api_client)
        api.reply_message(ReplyMessageRequest(reply_token=reply_token, messages=[flex]))


def make_text_message(text, menu=True):
    text = str(text)[:4900]
    if menu and HAS_QUICK_REPLY:
        try: return TextMessage(text=text, quick_reply=build_quick_reply())
        except Exception: pass
    return TextMessage(text=text)

def reply_text(reply_token, text, menu=True):
    with ApiClient(configuration) as api_client:
        api = MessagingApi(api_client)
        api.reply_message(ReplyMessageRequest(reply_token=reply_token, messages=[make_text_message(text, menu)]))

def push_text(user_id, text, menu=False):
    with ApiClient(configuration) as api_client:
        api = MessagingApi(api_client)
        api.push_message(PushMessageRequest(to=user_id, messages=[make_text_message(text, menu)]))

def push_long_text(user_id, text):
    text = str(text)
    parts = []
    while len(text) > 4800:
        cut = text.rfind("\n", 0, 4800)
        if cut < 1000: cut = 4800
        parts.append(text[:cut]); text = text[cut:].strip()
    if text: parts.append(text)
    for part in parts[:5]:
        push_text(user_id, part, menu=False)
        time.sleep(0.25)

def member_block_message(user_id):
    return f"""🔒 HCX-AI 會員限定

你的 LINE userId：
{user_id}

請將這組 ID 提供給管理員開通。"""

# 市場資料

def is_common_stock(code, name):
    code = str(code); name = str(name or "")
    if not re.fullmatch(r"\d{4}", code): return False
    bad = ["ETF","ETN","指數","期貨","反","槓桿","債","權證","牛","熊","購","售","受益","基金"]
    return not any(w in name for w in bad)

def guess_field(row, keys):
    for k in keys:
        if k in row: return row.get(k)
    for k, v in row.items():
        ks = str(k).lower()
        if any(str(t).lower() in ks for t in keys): return v
    return None

def fetch_taiwan_stock_name_map(force=False):
    """
    上市 + 上櫃完整名稱快取。
    目的：單股查詢時不再受 TOP 600 掃描名單限制，避免 6153 這類股票名稱顯示成代號。
    """
    now = time.time()
    if not force and STOCK_NAME_CACHE.get("map") and now - STOCK_NAME_CACHE.get("ts", 0) < 21600:
        return STOCK_NAME_CACHE["map"]

    mp = {}
    try:
        r = SESSION.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", timeout=12)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            for row in data:
                code = clean_text(guess_field(row, ["Code", "證券代號", "股票代號"]))
                name = clean_text(guess_field(row, ["Name", "證券名稱", "股票名稱"]))
                if is_common_stock(code, name):
                    mp[code] = {"name": name, "market": "上市"}
    except Exception as e:
        print(f"上市名稱快取失敗：{e}", flush=True)

    try:
        r = SESSION.get("https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes", timeout=12)
        r.raise_for_status()
        data = r.json()
        if isinstance(data, list):
            for row in data:
                code = clean_text(guess_field(row, ["SecuritiesCompanyCode", "Code", "代號", "股票代號"]))
                name = clean_text(guess_field(row, ["CompanyName", "Name", "名稱", "股票名稱"]))
                if is_common_stock(code, name):
                    mp[code] = {"name": name, "market": "上櫃"}
    except Exception as e:
        print(f"上櫃名稱快取失敗：{e}", flush=True)

    extra = dict(FALLBACK_STOCKS)
    extra.update({
        "6153": "嘉聯益",
    })
    for code, name in extra.items():
        if re.fullmatch(r"\d{4}", str(code)) and code not in mp:
            mp[str(code)] = {"name": name, "market": "上市"}

    STOCK_NAME_CACHE.update({"ts": now, "map": mp})
    return mp

def get_us_stock_meta(symbol):
    symbol = str(symbol or "").upper().strip()
    if symbol in US_META_CACHE:
        return US_META_CACHE[symbol]
    name = symbol
    try:
        info = yf.Ticker(symbol).get_info()
        if isinstance(info, dict):
            name = info.get("shortName") or info.get("longName") or symbol
    except Exception:
        try:
            info = yf.Ticker(symbol).info
            if isinstance(info, dict):
                name = info.get("shortName") or info.get("longName") or symbol
        except Exception:
            pass
    meta = {"symbol": symbol, "name": name}
    US_META_CACHE[symbol] = meta
    return meta

def is_us_symbol_text(text):
    text = str(text or "").strip()
    m = re.fullmatch(r"(?:美股\s*)?([A-Za-z][A-Za-z0-9\.\-]{0,9})", text)
    if not m:
        return ""
    sym = m.group(1).upper()
    block = {"MENU", "VERSION", "VER", "WAKE", "WAKEUP"}
    if sym in block:
        return ""
    return sym

def fetch_twse_all():
    out = []
    try:
        r = SESSION.get("https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL", timeout=12)
        r.raise_for_status(); data = r.json()
        if isinstance(data, list):
            for row in data:
                code = clean_text(guess_field(row, ["Code","證券代號","股票代號"]))
                name = clean_text(guess_field(row, ["Name","證券名稱","股票名稱"]))
                close = safe_float(guess_field(row, ["ClosingPrice","收盤價","Close"]))
                volume = safe_float(guess_field(row, ["TradeVolume","成交股數","Volume"]))
                value = safe_float(guess_field(row, ["TradeValue","成交金額","Value"]))
                if is_common_stock(code, name) and close > 0:
                    out.append({"code":code,"name":name,"market":"上市","close":close,"volume":volume,"value":value})
    except Exception as e:
        print(f"TWSE 抓取失敗：{e}", flush=True)
    return out

def fetch_tpex_all():
    out = []
    urls = ["https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"]
    for url in urls:
        try:
            r = SESSION.get(url, timeout=12); r.raise_for_status(); data = r.json()
            if isinstance(data, list):
                for row in data:
                    code = clean_text(guess_field(row, ["SecuritiesCompanyCode","Code","代號","股票代號"]))
                    name = clean_text(guess_field(row, ["CompanyName","Name","名稱","股票名稱"]))
                    close = safe_float(guess_field(row, ["Close","ClosingPrice","收盤價"]))
                    volume = safe_float(guess_field(row, ["TradingShares","TradeVolume","成交股數","Volume"]))
                    value = safe_float(guess_field(row, ["TransactionAmount","TradeValue","成交金額","Value"]))
                    if is_common_stock(code, name) and close > 0:
                        out.append({"code":code,"name":name,"market":"上櫃","close":close,"volume":volume,"value":value})
        except Exception as e:
            print(f"TPEx 抓取失敗：{e}", flush=True)
    return out

def fetch_market_universe(force=False):
    now = time.time()
    if not force and UNIVERSE_CACHE["data"] and now - UNIVERSE_CACHE["ts"] < 1800:
        return UNIVERSE_CACHE["data"]
    name_map = fetch_taiwan_stock_name_map(force=force)
    rows = fetch_twse_all() + fetch_tpex_all()
    exists = {r["code"] for r in rows}
    for code, meta in name_map.items():
        if code not in exists:
            rows.append({"code": code, "name": meta.get("name", code), "market": meta.get("market", "上市"), "close": 0, "volume": 0, "value": 0})
    for r in rows:
        if not r.get("value"):
            r["value"] = float(r.get("close") or 0) * float(r.get("volume") or 0)
    rows = [r for r in rows if is_common_stock(r["code"], r["name"])]
    rows = sorted(rows, key=lambda x: (float(x.get("value") or 0), float(x.get("volume") or 0)), reverse=True)
    raw_limit = max(80, min(safe_int(os.environ.get("HCX_RAW_UNIVERSE", 600), 600), 900))
    rows = rows[:raw_limit]
    UNIVERSE_CACHE.update({"ts":now, "data":rows})
    return rows

def get_stock_meta(code):
    code = str(code).strip()
    if re.fullmatch(r"\d{4}", code):
        name_map = fetch_taiwan_stock_name_map()
        if code in name_map:
            return {"code": code, "name": name_map[code].get("name", code), "market": name_map[code].get("market", "上市")}
    for r in fetch_market_universe():
        if r["code"] == code:
            return {"code": code, "name": r["name"], "market": r.get("market", "上市")}
    return {"code": code, "name": FALLBACK_STOCKS.get(code, code), "market": "上市"}

def yahoo_symbols(code, market="上市"):
    if market == "美股":
        return [str(code).upper()]
    return [f"{code}.TWO", f"{code}.TW"] if market == "上櫃" else [f"{code}.TW", f"{code}.TWO"]

def normalize_yf_df(df):
    if df is None or df.empty: return None
    if isinstance(df.columns, pd.MultiIndex): df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    for c in ["Open","High","Low","Close","Volume"]:
        if c not in df.columns: return None
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["High","Low","Close"])
    return None if df.empty else df

def download_daily(code, market="上市", period="4mo"):
    key = ("D", code, market, period); now = time.time()
    if key in DATA_CACHE and now - DATA_CACHE[key]["ts"] < 600:
        return DATA_CACHE[key]["df"], DATA_CACHE[key]["symbol"]
    for sym in yahoo_symbols(code, market):
        try:
            df = yf.download(sym, period=period, interval="1d", progress=False, auto_adjust=False, threads=False)
            df = normalize_yf_df(df)
            if df is not None and len(df) >= 25:
                DATA_CACHE[key] = {"ts":now,"df":df,"symbol":sym}
                return df, sym
        except Exception as e:
            print(f"日K失敗 {code} {sym}: {e}", flush=True)
    return None, ""

def download_intraday(code, market, interval="5m", period="5d"):
    key = ("I", code, market, interval, period); now = time.time()
    if key in INTRADAY_CACHE and now - INTRADAY_CACHE[key]["ts"] < 600:
        return INTRADAY_CACHE[key]["df"]
    for sym in yahoo_symbols(code, market):
        try:
            df = yf.download(sym, period=period, interval=interval, progress=False, auto_adjust=False, threads=False)
            df = normalize_yf_df(df)
            if df is not None and not df.empty:
                INTRADAY_CACHE[key] = {"ts":now,"df":df}
                return df
        except Exception as e:
            print(f"分K失敗 {code} {sym}: {e}", flush=True)
    return None

# 選股

def estimate_win_rate(df, mode):
    try:
        d = df.copy().tail(80)
        close = pd.to_numeric(d["Close"], errors="coerce")
        vol = pd.to_numeric(d["Volume"], errors="coerce")
        pct = close.pct_change() * 100
        vr = vol / vol.rolling(20).mean()
        next_ret = close.shift(-1) / close - 1
        if mode == "intraday_short":
            sig = (pct < -1.0) & (vr > 1.0); wins = next_ret < 0
        elif mode == "swing":
            sig = (pct > -0.5) & (pct < 7.5) & (vr > 0.8); wins = next_ret > 0
        else:
            sig = (pct > -1.5) & (vr > 0.9); wins = next_ret > 0
        sample = int(sig.sum())
        if sample < 3: return 50.0, sample
        return float((wins & sig).sum() / sample * 100), sample
    except Exception:
        return 50.0, 0

def score_stock(code, meta, command):
    df, _ = download_daily(code, meta.get("market","上市"))
    if df is None or df.empty: return None
    try:
        close_s = pd.to_numeric(df["Close"], errors="coerce")
        high_s = pd.to_numeric(df["High"], errors="coerce")
        low_s = pd.to_numeric(df["Low"], errors="coerce")
        vol_s = pd.to_numeric(df["Volume"], errors="coerce")
        close = float(close_s.iloc[-1]); prev = float(close_s.iloc[-2])
        pct = (close - prev) / prev * 100 if prev else 0
        volume = float(vol_s.iloc[-1]); volume_lots = volume / 1000
        vol_ma20 = float(vol_s.rolling(20).mean().iloc[-1]) if len(vol_s) >= 20 else max(volume, 1)
        vol_ratio = volume / vol_ma20 if vol_ma20 > 0 else 1
        ma5 = float(close_s.rolling(5).mean().iloc[-1])
        ma20 = float(close_s.rolling(20).mean().iloc[-1])
        ma60 = float(close_s.rolling(60).mean().iloc[-1]) if len(close_s) >= 60 else ma20
        high20 = float(high_s.tail(20).max()); low20 = float(low_s.tail(20).min())
        pos20 = (close - low20) / max(high20 - low20, 0.01) * 100
        atr = calc_atr14(df)
        value_m = close * volume / 1_000_000
        value_m = max(value_m, float(meta.get("official_value") or 0) / 1_000_000)
        volume_lots = max(volume_lots, float(meta.get("official_volume") or 0) / 1000)
        liq = clip(math.log10(max(value_m, 1)) / 4 * 100)
        volscore = clip(math.log10(max(volume_lots, 1)) / 4 * 100)
        vrscore = clip(vol_ratio / 2.5 * 100)
        trend_long = sum([close>ma5, close>ma20, ma5>=ma20, ma20>=ma60]) * 25
        trend_short = sum([close<ma5, close<ma20, ma5<=ma20, ma20<=ma60]) * 25
        pct_long = clip((pct + 2.0) / 8.0 * 100)
        pct_short = clip((-pct + 2.0) / 8.0 * 100)
        pos = clip(pos20)
        long_score = liq*0.25 + volscore*0.15 + vrscore*0.15 + trend_long*0.25 + pct_long*0.10 + pos*0.10
        short_score = liq*0.25 + volscore*0.15 + vrscore*0.15 + trend_short*0.25 + pct_short*0.10 + (100-pos)*0.10
        swing_health = clip(100 - abs(pct - 2.0) * 12)
        swing_score = liq*0.25 + volscore*0.15 + trend_long*0.20 + swing_health*0.20 + pos*0.20
        if command == "當沖空": mode, base, signal = "intraday_short", short_score, "🟩 偏空當沖"
        elif command == "隔日沖": mode, base, signal = "swing", swing_score, "🟧 隔日沖觀察"
        elif command == "波段股": mode, base, signal = "swing", swing_score, "🟦 波段觀察"
        elif command == "當沖股" and short_score > long_score: mode, base, signal = "intraday_short", short_score, "🟩 偏空當沖"
        else: mode, base, signal = "intraday_long", long_score, "🟥 偏多當沖"
        win_rate, samples = estimate_win_rate(df, mode)
        rank = base * 0.82 + win_rate * (0.18 * min(samples/12, 1))
        if value_m < safe_float(os.environ.get("HCX_MIN_VALUE_M", 30), 30): rank -= 18
        if volume_lots < safe_float(os.environ.get("HCX_MIN_LOTS", 1000), 1000): rank -= 15
        return {"code":code,"name":meta.get("name",code),"market":meta.get("market","上市"),"close":close,"pct":pct,
                "volume_lots":volume_lots,"value_m":value_m,"vol_ratio":vol_ratio,"ma5":ma5,"ma20":ma20,"ma60":ma60,
                "high20":high20,"low20":low20,"atr":atr,"score":round(base,1),"rank_score":round(rank,1),
                "win_rate":round(win_rate,1),"samples":samples,"trade_kind":mode,"signal":signal}
    except Exception as e:
        print(f"score_stock 錯誤 {code}: {e}", flush=True); return None

def build_trade_plan(row):
    code = row["code"]; market = row.get("market","上市"); close = float(row["close"])
    atr = float(row.get("atr") or close*0.02); kind = row.get("trade_kind","intraday_long"); t = tick_size(close)
    support5 = row.get("low20", close-atr); resistance5 = row.get("high20", close+atr)
    support30, resistance30 = support5, resistance5
    try:
        df5 = download_intraday(code, market, "5m", "5d")
        if df5 is not None and not df5.empty:
            recent = df5.tail(48); support5 = float(recent["Low"].min()); resistance5 = float(recent["High"].max())
    except Exception: pass
    try:
        df30 = download_intraday(code, market, "30m", "10d")
        if df30 is not None and not df30.empty:
            recent = df30.tail(10); support30 = float(recent["Low"].min()); resistance30 = float(recent["High"].max())
    except Exception: pass
    min_gap = max(atr*0.18, t*3)
    if kind == "intraday_short":
        entry = round_by_tick(min(close, support5), "down")
        stop = round_by_tick(max(resistance30, entry + min_gap), "up")
        take = round_by_tick(entry - max(stop-entry, min_gap)*0.60, "down")
    else:
        entry = round_by_tick(min(close, max(support5, support30)), "nearest")
        stop = round_by_tick(min(support30, entry - min_gap), "down")
        take = round_by_tick(max(resistance5, entry + max(entry-stop, min_gap)*0.60), "up")
    row.update({"entry":entry,"stop":stop,"take_profit":take,"tick_line":calc_tick_profit_info(entry, take)})
    return row

def run_quantum_scan(command):
    universe = fetch_market_universe()
    deep_n = safe_int(os.environ.get("HCX_DEEP_SCAN", 120), 120)
    if command == "波段股":
        deep_n = max(deep_n, safe_int(os.environ.get("HCX_SWING_DEEP_SCAN", 180), 180))
    deep_n = max(40, min(deep_n, 260))
    candidates = universe[:deep_n]
    rows = []
    workers = max(3, min(safe_int(os.environ.get("HCX_WORKERS", 8), 8), 10))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        for item in candidates:
            meta = {"name":item.get("name"),"market":item.get("market","上市"),"official_value":item.get("value",0),"official_volume":item.get("volume",0)}
            futures.append(executor.submit(score_stock, item["code"], meta, command))
        for fut in as_completed(futures):
            try:
                r = fut.result()
                if r is not None: rows.append(r)
            except Exception as e: print(f"候選股評分失敗：{e}", flush=True)
    if not rows:
        for code, name in list(FALLBACK_STOCKS.items())[:60]:
            r = score_stock(code, get_stock_meta(code), command)
            if r: rows.append(r)
    rows = sorted(rows, key=lambda x:(float(x.get("rank_score") or 0), float(x.get("value_m") or 0)), reverse=True)[:20]
    final = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(build_trade_plan, dict(r)) for r in rows[:5]]
        for fut in as_completed(futures):
            try: final.append(fut.result())
            except Exception as e: print(f"交易計畫失敗：{e}", flush=True)
    final = sorted(final, key=lambda x:float(x.get("rank_score") or 0), reverse=True)
    return format_quantum_report(command, final)

def format_quantum_report(command, rows):
    titles = {
        "當沖股": "⚡ 最佳當沖股 TOP 5",
        "當沖多": "🔴 當沖多 TOP 5",
        "當沖空": "🟢 當沖空 TOP 5",
        "隔日沖": "🟠 隔日沖 TOP 5",
        "波段股": "🟦 波段股 TOP 5",
    }
    if not rows:
        return f"""⚡ HCX-AI量子雷達
🕒 時間：{query_time_text()}
📌 指令：{command}

本次暫無適合出手的股票。
可稍後盤中再查，或等待量能與方向更明確。"""

    rank_labels = ["A", "B", "C", "D", "E"]
    lines = [
        "⚡ HCX-AI量子雷達",
        f"🕒 時間：{query_time_text()}",
        titles.get(command, command),
        "━━━━━━━━━━━━━━",
    ]

    for i, r in enumerate(rows[:5]):
        label = rank_labels[i] if i < len(rank_labels) else chr(65 + i)
        lines.append(
            f"{label}. {r['code']} {r['name']}｜{r.get('signal','')}\n"
            f"   收盤 {fmt_price(r['close'])}｜漲跌 {fmt_pct(r['pct'])}｜量比 {float(r.get('vol_ratio') or 0):.2f}\n"
            f"   🏆 AI勝率 {float(r.get('win_rate') or 0):.1f}%｜職業評分 {float(r.get('rank_score') or 0):.1f}\n"
            f"   🎯 建議進場價：{fmt_price(r.get('entry'))}\n"
            f"   ✅ 建議停利價：{fmt_price(r.get('take_profit'))}\n"
            f"   🛑 建議停損價：{fmt_price(r.get('stop'))}\n"
            f"   {r.get('tick_line','')}"
        )

    lines += [
        "━━━━━━━━━━━━━━",
        "⚠️ 本訊息為HCX-AI量子雷達估算，不保證獲利。",
        "⚠️ 當沖請配合1分K轉折、盤中量能與紀律停損。",
    ]
    return "\n".join(lines)[:4800]


# 單檔分析

def analyze_one_stock(code):
    meta = get_stock_meta(code); df, _ = download_daily(code, meta["market"], "6mo")
    if df is None or df.empty or len(df) < 30: return f"查不到 {code} 的股票資料，請確認代號。"
    close_s = pd.to_numeric(df["Close"], errors="coerce"); high_s = pd.to_numeric(df["High"], errors="coerce"); low_s = pd.to_numeric(df["Low"], errors="coerce")
    close = float(close_s.iloc[-1]); prev = float(close_s.iloc[-2]); change = close-prev; pct = change/prev*100 if prev else 0
    ma5 = float(close_s.rolling(5).mean().iloc[-1]); ma20 = float(close_s.rolling(20).mean().iloc[-1]); ma60 = float(close_s.rolling(60).mean().iloc[-1]) if len(close_s)>=60 else ma20
    bb_mid_raw = float(close_s.rolling(20).mean().iloc[-1])
    bb_std = float(close_s.rolling(20).std(ddof=0).iloc[-1])
    bb_upper = round_by_tick(bb_mid_raw + 2 * bb_std)
    bb_mid = round_by_tick(bb_mid_raw)
    bb_lower = round_by_tick(bb_mid_raw - 2 * bb_std)
    high20 = float(high_s.tail(20).max()); low20 = float(low_s.tail(20).min()); atr = calc_atr14(df)
    trend = "偏多" if close > ma5 > ma20 else "偏空" if close < ma5 < ma20 else "震盪"
    advice = "短線偏多，留意回測支撐後是否再轉強。" if trend == "偏多" else "短線偏弱，若反彈無量仍需保守。" if trend == "偏空" else "目前偏震盪，等突破壓力或跌破支撐再決定方向。"
    plan = build_trade_plan({"code":code,"name":meta['name'],"market":meta['market'],"close":close,"atr":atr,"high20":high20,"low20":low20,"trade_kind":"intraday_long"})
    return f"""⚡ HCX-AI量子雷達
🕒 時間：{query_time_text()}

🏷️ 股票：{code} {meta['name']}
💰 現價：{fmt_price(close)}
📊 漲跌：{fmt_price(change)}
📈 漲跌幅：{fmt_pct(pct)}

⚡ MA5：{fmt_price(ma5)}
🌙 MA20：{fmt_price(ma20)}
🏔️ MA60：{fmt_price(ma60)}
🌊 ATR14：{fmt_price(atr)}

📊 日K布林通道
🔺 上軌：{fmt_price(bb_upper)}
➖ 中軌：{fmt_price(bb_mid)}
🔻 下軌：{fmt_price(bb_lower)}

🧱 壓力：{fmt_price(high20)}
🧱 支撐：{fmt_price(low20)}

🧭 趨勢判斷：{trend}
🧠 AI建議：{advice}

🎯 建議進場價：{fmt_price(plan['entry'])}
✅ 建議停利價：{fmt_price(plan['take_profit'])}
🛑 建議停損價：{fmt_price(plan['stop'])}
{plan['tick_line']}

⚠️ 本訊息為程式估算，不保證獲利。"""

# 指令

def analyze_us_stock(symbol):
    symbol = str(symbol or "").upper().strip()
    meta = get_us_stock_meta(symbol)
    df, _ = download_daily(symbol, "美股", "6mo")
    if df is None or df.empty or len(df) < 30:
        return f"查不到美股 {symbol} 的資料，請確認代號。"

    close_s = pd.to_numeric(df["Close"], errors="coerce")
    high_s = pd.to_numeric(df["High"], errors="coerce")
    low_s = pd.to_numeric(df["Low"], errors="coerce")

    close = float(close_s.iloc[-1])
    prev = float(close_s.iloc[-2])
    change = close - prev
    pct = change / prev * 100 if prev else 0

    ma5 = float(close_s.rolling(5).mean().iloc[-1])
    ma20 = float(close_s.rolling(20).mean().iloc[-1])
    ma60 = float(close_s.rolling(60).mean().iloc[-1]) if len(close_s) >= 60 else ma20

    bb_mid = float(close_s.rolling(20).mean().iloc[-1])
    bb_std = float(close_s.rolling(20).std(ddof=0).iloc[-1])
    bb_upper = bb_mid + 2 * bb_std
    bb_lower = bb_mid - 2 * bb_std

    high20 = float(high_s.tail(20).max())
    low20 = float(low_s.tail(20).min())
    atr = calc_atr14(df)

    trend = "偏多" if close > ma5 > ma20 else "偏空" if close < ma5 < ma20 else "震盪"
    advice = "短線偏多，留意回測支撐後是否再轉強。" if trend == "偏多" else "短線偏弱，若反彈無量仍需保守。" if trend == "偏空" else "目前偏震盪，等突破壓力或跌破支撐再決定方向。"

    entry = round(close, 2)
    stop = round(max(close - atr * 1.2, low20), 2) if trend != "偏空" else round(high20, 2)
    take = round(max(close + atr * 1.5, high20), 2) if trend != "偏空" else round(max(close - atr * 1.5, low20), 2)

    return f"""⚡ HCX-AI量子雷達
🕒 時間：{query_time_text()}

🇺🇸 美股：{symbol} {meta.get('name', symbol)}
💰 現價：{close:.2f}
📊 漲跌：{change:+.2f}
📈 漲跌幅：{pct:+.2f}%

⚡ MA5：{ma5:.2f}
🌙 MA20：{ma20:.2f}
🏔️ MA60：{ma60:.2f}
🌊 ATR14：{atr:.2f}

📊 日K布林通道
🔺 上軌：{bb_upper:.2f}
➖ 中軌：{bb_mid:.2f}
🔻 下軌：{bb_lower:.2f}

🧱 壓力：{high20:.2f}
🧱 支撐：{low20:.2f}

🧭 趨勢判斷：{trend}
🧠 AI建議：{advice}

🎯 建議進場價：{entry:.2f}
✅ 建議停利價：{take:.2f}
🛑 建議停損價：{stop:.2f}

⚠️ 本訊息為程式估算，不保證獲利。"""

# 指令

def normalize_text(text):
    t = str(text or "").strip().replace("\n", "").replace(" ", "").replace("　", "").replace("/", "")
    for w in ["我要", "幫我", "查詢", "請查"]: t = t.replace(w, "")
    return t

def detect_quantum_command(text):
    t = normalize_text(text)
    if any(w in t for w in ["波段股","波段","中線股","中線","波段選股"]): return "波段股"
    if any(w in t for w in ["當沖股","當衝股","最佳當沖","今日當沖"]): return "當沖股"
    if any(w in t for w in ["當沖多","當衝多","沖多","衝多","當沖做多"]): return "當沖多"
    if any(w in t for w in ["當沖空","當衝空","沖空","衝空","當沖做空"]): return "當沖空"
    if any(w in t for w in ["隔日沖","隔日衝","隔日"]): return "隔日沖"
    return None

def help_message():
    return f"""🌈 HCX-AI 量子雷達

請輸入 4 碼股票代號，例如：
🚀 2330 台積電
⚡ 2454 聯發科
🏭 2317 鴻海
🧪 1717 長興

量子選股：
⚡ 當沖股
🔴 當沖多
🟢 當沖空
🟠 隔日沖
🟦 波段股

觸控選單：
🔳 主選單

自動提醒：
⏰ 設定提醒 當沖多 08:50
⏰ 設定提醒 當沖空 09:00
⏰ 設定提醒 隔日沖 13:00
📋 我的提醒
🗑️ 取消提醒 當沖多

其他：
主選單
請開機
版本
我的ID"""

# 提醒

def load_reminders():
    global REMINDER_SCHEDULES
    try:
        if REMINDER_FILE.exists():
            data = json.loads(REMINDER_FILE.read_text(encoding="utf-8"))
            if isinstance(data, dict): REMINDER_SCHEDULES = data
    except Exception as e: print(f"讀取提醒失敗：{e}", flush=True)

def save_reminders():
    try:
        REMINDER_FILE.parent.mkdir(parents=True, exist_ok=True)
        REMINDER_FILE.write_text(json.dumps(REMINDER_SCHEDULES, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e: print(f"儲存提醒失敗：{e}", flush=True)

def parse_reminder_command(text):
    raw = str(text or "").strip(); compact = normalize_text(raw)
    if compact in ["我的提醒","提醒列表","查提醒","查看提醒"]: return {"action":"list"}
    if compact in ["取消全部提醒","刪除全部提醒","關閉全部提醒","停止全部提醒"]: return {"action":"cancel_all"}
    if any(w in compact for w in ["取消提醒","刪除提醒","停止提醒","關閉提醒"]):
        cmd = detect_quantum_command(compact)
        return {"action":"cancel","command":cmd} if cmd else {"action":"cancel_all"}
    if not any(w in raw for w in ["提醒","定時","每天","每日","排程","鬧鐘"]): return None
    cmd = detect_quantum_command(raw); m = re.search(r"([01]?\d|2[0-3])\s*[:：]?\s*([0-5]\d)", raw)
    if not cmd or not m: return {"action":"help"}
    return {"action":"set","command":cmd,"time":f"{int(m.group(1)):02d}:{int(m.group(2)):02d}"}

def reminder_help():
    return """⏰ HCX-AI 自動提醒設定

範例：
設定提醒 當沖股 08:50
設定提醒 當沖多 08:50
設定提醒 當沖空 09:00
設定提醒 隔日沖 13:00

查詢：我的提醒
取消：取消提醒 當沖多
取消全部提醒"""

def handle_reminder(user_id, parsed):
    if not user_id: return "⚠️ 無法取得你的 LINE userId，請用一對一聊天室設定。"
    action = parsed.get("action")
    if action == "help": return reminder_help()
    with REMINDER_LOCK:
        REMINDER_SCHEDULES.setdefault(user_id, {})
        if action == "list":
            data = REMINDER_SCHEDULES.get(user_id, {})
            if not data: return "📋 目前尚未設定提醒。\n\n範例：設定提醒 當沖多 08:50"
            lines = ["📋 你的自動提醒", "━━━━━━━━━━━━━━"]
            for cmd, item in data.items():
                if item.get("enabled", True): lines.append(f"✅ {cmd}｜每天 {item.get('time')}")
            lines += ["━━━━━━━━━━━━━━", "取消範例：取消提醒 當沖多"]
            return "\n".join(lines)
        if action == "set":
            REMINDER_SCHEDULES[user_id][parsed["command"]] = {"time":parsed["time"],"enabled":True,"last_sent_date":"","updated_at":query_time_text()}
            save_reminders()
            return f"""✅ 已設定自動提醒

📌 類型：{parsed['command']}
⏰ 時間：每天 {parsed['time']}
📆 週一到週五自動推播

查詢設定請輸入：我的提醒"""
        if action == "cancel":
            cmd = parsed.get("command")
            if cmd in REMINDER_SCHEDULES.get(user_id, {}):
                REMINDER_SCHEDULES[user_id].pop(cmd, None); save_reminders(); return f"✅ 已取消「{cmd}」提醒。"
            return f"目前沒有設定「{cmd}」提醒。"
        if action == "cancel_all":
            REMINDER_SCHEDULES[user_id] = {}; save_reminders(); return "✅ 已取消全部自動提醒。"
    return reminder_help()

def due_reminders_now():
    now = now_taipei()
    if now.weekday() >= 5: return {}
    today = now.strftime("%Y-%m-%d"); hhmm = now.strftime("%H:%M"); due = {}
    with REMINDER_LOCK:
        for uid, data in list(REMINDER_SCHEDULES.items()):
            if not isinstance(data, dict): continue
            for cmd, item in list(data.items()):
                if not isinstance(item, dict) or not item.get("enabled", True): continue
                if item.get("time") != hhmm or item.get("last_sent_date") == today: continue
                item["last_sent_date"] = today; due.setdefault(cmd, []).append(uid)
        if due: save_reminders()
    return due

def run_due_reminders():
    for cmd, users in due_reminders_now().items():
        try:
            result = run_quantum_scan(cmd)
            text = f"⏰ HCX-AI 自動提醒｜{cmd}\n🕒 {query_time_text()}\n\n{result}"
            for uid in users:
                if is_authorized_user(uid): push_long_text(uid, text)
        except Exception as e:
            print(f"提醒推播失敗 {cmd}: {e}", flush=True); traceback.print_exc()

def reminder_loop():
    print("========== HCX-AI 自動提醒排程啟動 ==========", flush=True)
    load_reminders()
    while True:
        try: run_due_reminders()
        except Exception as e: print(f"提醒排程錯誤：{e}", flush=True)
        time.sleep(20)

def start_reminder_loop():
    global REMINDER_STARTED
    if REMINDER_STARTED: return
    REMINDER_STARTED = True
    threading.Thread(target=reminder_loop, daemon=True).start()

# 背景掃描

def start_scan_async(user_id, command):
    def job():
        try:
            with SCAN_LOCK: result = run_quantum_scan(command)
            push_long_text(user_id, result)
        except Exception as e:
            print(f"背景掃描失敗 {command}: {e}", flush=True); traceback.print_exc()
            try: push_text(user_id, f"⚠️ {command} 掃描暫時失敗，請稍後再試。")
            except Exception: pass
    threading.Thread(target=job, daemon=True).start()
    return f"""⚡ 已收到「{command}」指令

系統正在啟動 HCX-AI 量子雷達篩選中...

📊 掃描模式：選股日報核心 TOP 5
🕒 時間：{query_time_text()}

稍後會自動推播結果給你。"""

# Flask

@app.route("/")
def home(): return f"HCX-AI LINE BOT OK｜{APP_VERSION}｜{query_time_text()}"

@app.route("/ping")
def ping(): return jsonify({"ok": True, "version": APP_VERSION, "time": query_time_text()})

@app.route("/cron")
def cron():
    key = request.args.get("key", ""); secret = os.environ.get("CRON_SECRET", "")
    if secret and key != secret: return "Forbidden", 403
    run_due_reminders(); return jsonify({"ok": True, "time": query_time_text()})

@app.route("/callback", methods=["POST"])
def callback():
    signature = request.headers.get("X-Line-Signature", ""); body = request.get_data(as_text=True)
    try: handler.handle(body, signature)
    except InvalidSignatureError:
        print("LINE 簽章錯誤", flush=True); return "Bad Signature", 400
    except Exception as e:
        print(f"callback 錯誤：{e}", flush=True); traceback.print_exc(); return "OK", 200
    return "OK"

@handler.add(MessageEvent, message=TextMessageContent)
def handle_message(event):
    user_id = getattr(event.source, "user_id", "")
    raw = str(event.message.text or "").strip(); msg = normalize_text(raw)
    try:
        if not is_authorized_user(user_id):
            reply_text(event.reply_token, member_block_message(user_id), menu=False); return
        reminder_cmd = parse_reminder_command(raw); quantum_cmd = detect_quantum_command(raw)
        if msg in ["主選單", "選單", "功能選單", "menu", "MENU"]:
            reply_main_menu(event.reply_token)
            return
        if msg in ["請開機", "開機", "開機中", "喚醒", "wake", "wakeup"] or msg.lower() in ["wake", "wake up"]:
            reply_text(event.reply_token, f"✅ HCX-AI 開機中，請等候30秒!\n🕒 {query_time_text()}")
            return
        if msg.lower() in ["版本", "version", "ver"]:
            reply_text(event.reply_token, f"✅ 目前版本：\n{APP_VERSION}"); return
        if msg in ["我的id", "我的ID", "userid", "userID"]:
            reply_text(event.reply_token, f"你的 LINE userId：\n{user_id}", menu=False); return
        if reminder_cmd:
            reply_text(event.reply_token, handle_reminder(user_id, reminder_cmd)); return
        if quantum_cmd:
            reply_text(event.reply_token, start_scan_async(user_id, quantum_cmd)); return
        m = re.search(r"\d{4}", msg)
        if m:
            reply_text(event.reply_token, analyze_one_stock(m.group(0))); return
        us_symbol = is_us_symbol_text(raw)
        if us_symbol:
            reply_text(event.reply_token, analyze_us_stock(us_symbol)); return
        reply_main_menu(event.reply_token)
    except Exception as e:
        print(f"handle_message 錯誤：{e}", flush=True); traceback.print_exc()
        try: reply_text(event.reply_token, "⚠️ 系統暫時忙碌，請稍後再試。")
        except Exception: pass

start_reminder_loop()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
