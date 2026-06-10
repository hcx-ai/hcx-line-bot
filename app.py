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
import uuid
from pathlib import Path
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
import requests
import yfinance as yf
from flask import Flask, request, jsonify, send_file

from linebot.v3 import WebhookHandler
from linebot.v3.exceptions import InvalidSignatureError
from linebot.v3.messaging import (
    Configuration, ApiClient, MessagingApi,
    ReplyMessageRequest, PushMessageRequest, TextMessage, ImageMessage,
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

APP_VERSION = "V7.9.1 穩定版"
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
TRIANGLE_AUTH_CACHE = {"ts": 0, "users": set()}
TRIANGLE_AUTH_FILE = Path(os.environ.get("HCX_TRIANGLE_AUTH_FILE", "/tmp/hcx_triangle_auth.json"))
TRIANGLE_PASSWORD = os.environ.get("HCX_TRIANGLE_PASSWORD", "694509")
PREMIUM_PASSWORD = os.environ.get("HCX_PREMIUM_PASSWORD", TRIANGLE_PASSWORD or "694509")
PREMIUM_PENDING_AUTH = {}
PREMIUM_COMMANDS = {"三角收斂", "布林戰法"}
PUBLIC_BASE_URL = (os.environ.get("PUBLIC_BASE_URL") or os.environ.get("RENDER_EXTERNAL_URL") or "https://hcx-line-bot.onrender.com").rstrip("/")
CHART_DIR = Path(os.environ.get("HCX_CHART_DIR", "/tmp/hcx_charts"))
try:
    CHART_DIR.mkdir(parents=True, exist_ok=True)
except Exception:
    pass
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
def is_advanced_user(user_id):
    return is_admin_user(user_id) or str(user_id) in parse_id_list("ADVANCED_USER_IDS")
def is_authorized_user(user_id):
    if not is_member_only(): return True
    return user_id in parse_id_list("AUTHORIZED_USER_IDS") or is_advanced_user(user_id)

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

def calc_week_trend(weekly_df):
    try:
        if weekly_df is None or weekly_df.empty or len(weekly_df) < 20:
            return "資料不足", 50
        close = pd.to_numeric(weekly_df["Close"], errors="coerce").dropna()
        if len(close) < 20:
            return "資料不足", 50
        ma10 = close.rolling(10).mean()
        ma30 = close.rolling(30).mean() if len(close) >= 30 else ma10
        c = float(close.iloc[-1])
        m10 = float(ma10.iloc[-1])
        m30 = float(ma30.iloc[-1])
        if c > m10 and m10 >= m30:
            return "偏多", 100
        if c < m10 and m10 <= m30:
            return "偏空", 25
        return "盤整", 65
    except Exception:
        return "資料不足", 50

def calc_triangle_profile(df, weekly_df=None):
    """
    V7.7 三角收斂升級：
    週K看大方向，日K判斷收斂型態。
    分類：上升三角收斂 / 標準三角收斂 / 下降三角收斂 / 收斂整理。
    """
    default = {
        "score": 0.0, "pattern": "收斂整理", "week_trend": "資料不足",
        "breakout_price": None, "defense_price": None, "shrink_pct": 0.0
    }
    try:
        d = df.copy().tail(70)
        if d is None or len(d) < 40:
            return default

        high = pd.to_numeric(d["High"], errors="coerce").dropna()
        low = pd.to_numeric(d["Low"], errors="coerce").dropna()
        close = pd.to_numeric(d["Close"], errors="coerce").dropna()
        vol = pd.to_numeric(d["Volume"], errors="coerce").dropna()
        n = min(len(high), len(low), len(close))
        if n < 40:
            return default

        high = high.tail(n)
        low = low.tail(n)
        close = close.tail(n)
        x = np.arange(n, dtype=float)
        last_close = float(close.iloc[-1])

        hi_fit = np.polyfit(x, high.values, 1)
        lo_fit = np.polyfit(x, low.values, 1)
        hi_line = hi_fit[0] * x + hi_fit[1]
        lo_line = lo_fit[0] * x + lo_fit[1]

        width_start = max(float(hi_line[0] - lo_line[0]), 0.01)
        width_end = max(float(hi_line[-1] - lo_line[-1]), 0.01)
        shrink_pct = clip((width_start - width_end) / width_start * 100, 0, 100)

        hi_norm = hi_fit[0] / max(last_close, 1) * 100
        lo_norm = lo_fit[0] / max(last_close, 1) * 100
        flat_th = 0.035

        # 型態分類：優先抓勝率較佳的上升三角
        if lo_norm > flat_th and abs(hi_norm) <= flat_th * 1.3:
            pattern = "上升三角收斂"
            type_score = 100
        elif hi_norm < -flat_th and lo_norm > flat_th:
            pattern = "標準三角收斂"
            type_score = 82
        elif hi_norm < -flat_th and abs(lo_norm) <= flat_th * 1.3:
            pattern = "下降三角收斂"
            type_score = 55
        else:
            pattern = "收斂整理"
            type_score = 62

        pressure_score = 100 if hi_fit[0] <= 0 else clip(100 - hi_fit[0] / max(last_close, 1) * 5000, 0, 100)
        support_score = 100 if lo_fit[0] >= 0 else clip(100 + lo_fit[0] / max(last_close, 1) * 5000, 0, 100)

        ma20 = close.rolling(20).mean()
        sd20 = close.rolling(20).std(ddof=0)
        bb_width = ((ma20 + 2 * sd20) - (ma20 - 2 * sd20)) / ma20.replace(0, np.nan)
        bw_recent = float(bb_width.tail(5).mean())
        bw_base = float(bb_width.tail(45).max())
        bb_score = clip((1 - bw_recent / max(bw_base, 0.001)) * 140, 0, 100)

        inside = float(lo_line[-1]) <= last_close <= float(hi_line[-1])
        inside_score = 100 if inside else 45

        vol_score = 55
        if len(vol) >= 40:
            recent_vol = float(vol.tail(10).mean())
            base_vol = float(vol.tail(40).mean())
            vol_score = clip(100 - recent_vol / max(base_vol, 1) * 35, 35, 100)

        week_trend, week_score = calc_week_trend(weekly_df)

        score = (
            pressure_score * 0.12 +
            support_score * 0.12 +
            shrink_pct * 0.20 +
            bb_score * 0.18 +
            inside_score * 0.08 +
            vol_score * 0.05 +
            type_score * 0.15 +
            week_score * 0.10
        )

        # 實戰偏好：上升三角 + 週K偏多，給額外加分；下降三角降低排序
        if pattern == "上升三角收斂" and week_trend == "偏多":
            score += 8
        elif pattern == "標準三角收斂" and week_trend in ["偏多", "盤整"]:
            score += 3
        elif pattern == "下降三角收斂":
            score -= 10
        if week_trend == "偏空":
            score -= 8

        breakout = round_by_tick(max(float(hi_line[-1]), float(high.tail(10).max())), "up")
        defense = round_by_tick(min(float(lo_line[-1]), float(low.tail(10).min())), "down")

        return {
            "score": round(float(clip(score, 0, 100)), 1),
            "pattern": pattern,
            "week_trend": week_trend,
            "breakout_price": breakout,
            "defense_price": defense,
            "shrink_pct": round(float(shrink_pct), 1)
        }
    except Exception as e:
        print(f"三角收斂評分失敗：{e}", flush=True)
        return default

def calc_triangle_convergence(df):
    return calc_triangle_profile(df).get("score", 0.0)


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
        ("布林戰", "布林戰法"),
        ("08:50沖多", "設定提醒 當沖多 08:50"),
        ("10:00沖空", "設定提醒 當沖空 10:00"),
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
                _row("布林戰", "布林戰法", "三角收", "三角收斂", "當沖多", "當沖多", "primary", "primary", "primary"),
                _row("當沖空", "當沖空", "隔日沖", "隔日沖", "波段股", "波段股", "primary", "primary", "primary"),
                _row("小鈴鐺", "我的提醒", "提醒多", "設定提醒 當沖多 08:50", "提醒空", "設定提醒 當沖空 10:00", "secondary", "secondary", "secondary"),
                _row("提醒隔", "設定提醒 隔日沖 13:00", "會員", "會員等級", "版本", "版本"),
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

def public_chart_url(filename):
    return f"{PUBLIC_BASE_URL}/chart/{filename}"

def make_stock_chart(code, name, df):
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from matplotlib.patches import Rectangle

        d = df.copy().tail(60)
        if d is None or d.empty or len(d) < 25:
            return ""

        for col in ["Open", "High", "Low", "Close", "Volume"]:
            d[col] = pd.to_numeric(d[col], errors="coerce")
        d = d.dropna(subset=["Open", "High", "Low", "Close"])
        if d.empty:
            return ""

        close = d["Close"]
        ma20 = close.rolling(20).mean()
        sd20 = close.rolling(20).std(ddof=0)
        upper = ma20 + 2 * sd20
        lower = ma20 - 2 * sd20

        x = np.arange(len(d))
        fig = plt.figure(figsize=(9.5, 6.2), dpi=160)
        ax = fig.add_axes([0.08, 0.28, 0.86, 0.62])
        axv = fig.add_axes([0.08, 0.09, 0.86, 0.15], sharex=ax)

        # Bollinger river
        ax.plot(x, upper.values, linewidth=1, alpha=0.8)
        ax.plot(x, ma20.values, linewidth=1, alpha=0.7)
        ax.plot(x, lower.values, linewidth=1, alpha=0.8)
        ax.fill_between(x, lower.values.astype(float), upper.values.astype(float), alpha=0.12)

        # Candles
        width = 0.55
        for i, (_, row) in enumerate(d.iterrows()):
            o, h, l, c = float(row["Open"]), float(row["High"]), float(row["Low"]), float(row["Close"])
            color = "#e53935" if c >= o else "#00a65a"  # 台股：紅漲綠跌
            ax.vlines(i, l, h, color=color, linewidth=1)
            bottom = min(o, c)
            height = max(abs(c - o), max(c, o) * 0.001)
            ax.add_patch(Rectangle((i - width/2, bottom), width, height, facecolor=color, edgecolor=color, linewidth=0.8, alpha=0.9))
            axv.bar(i, float(row.get("Volume", 0))/1000, color=color, width=0.65, alpha=0.85)

        # Triangle convergence lines
        recent_n = min(45, len(d))
        xx = np.arange(recent_n, dtype=float)
        high = d["High"].tail(recent_n).values.astype(float)
        low = d["Low"].tail(recent_n).values.astype(float)
        hi_fit = np.polyfit(xx, high, 1)
        lo_fit = np.polyfit(xx, low, 1)
        start = len(d) - recent_n
        tri_x = np.arange(start, len(d))
        rel_x = np.arange(recent_n)
        ax.plot(tri_x, hi_fit[0]*rel_x + hi_fit[1], linewidth=1.3)
        ax.plot(tri_x, lo_fit[0]*rel_x + lo_fit[1], linewidth=1.3)

        last_close = float(d["Close"].iloc[-1])
        ax.axhline(last_close, linestyle="--", linewidth=0.8, alpha=0.7)
        ax.text(len(d)-1, last_close, f" {fmt_price(last_close)}", va="center", fontsize=8)

        title = f"{code} {name}｜日K布林＋三角收斂"
        ax.set_title(title, fontsize=11)
        ax.grid(True, alpha=0.25)
        axv.grid(True, alpha=0.2)
        axv.set_ylabel("量", fontsize=8)

        labels = []
        positions = []
        idx_list = list(d.index)
        for i in range(0, len(idx_list), max(1, len(idx_list)//6)):
            positions.append(i)
            try:
                labels.append(pd.to_datetime(idx_list[i]).strftime("%m/%d"))
            except Exception:
                labels.append(str(i))
        axv.set_xticks(positions)
        axv.set_xticklabels(labels, fontsize=8)
        plt.setp(ax.get_xticklabels(), visible=False)

        filename = f"{code}_{uuid.uuid4().hex[:10]}.png"
        path = CHART_DIR / filename
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)
        return public_chart_url(filename)
    except Exception as e:
        print(f"圖表產生失敗 {code}: {e}", flush=True)
        return ""


def build_stock_snapshot_for_card(code, name, df, market="上市"):
    try:
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
        high20 = float(high_s.tail(20).max())
        low20 = float(low_s.tail(20).min())
        atr = calc_atr14(df)

        trend = "偏多" if close > ma5 > ma20 else "偏空" if close < ma5 < ma20 else "震盪"

        if market == "美股":
            entry = round(close, 2)
            stop = round(min(low20, close - max(atr * 0.3, 0.3)), 2)
            take_profit = round(max(high20, entry + max(entry - stop, 0.3) * 0.6), 2)
            breakeven = "-"
        else:
            entry = round_by_tick(min(close, max(low20, ma20)), "nearest")
            stop = round_by_tick(min(low20, entry - max(atr * 0.3, tick_size(close) * 3)), "down")
            take_profit = round_by_tick(max(high20, entry + max(entry - stop, tick_size(close) * 3) * 0.6), "up")
            cost_rate = 0.001425 * 0.6 * 2 + 0.0015
            cost = entry * 1000 * cost_rate
            one_tick_profit = tick_size(entry) * 1000
            breakeven = str(max(1, math.ceil(cost / max(one_tick_profit, 1))))

        return {
            "code": str(code),
            "name": str(name),
            "close": close,
            "change": change,
            "pct": pct,
            "ma5": ma5,
            "ma20": ma20,
            "ma60": ma60,
            "high20": high20,
            "low20": low20,
            "atr": atr,
            "trend": trend,
            "entry": entry,
            "take_profit": take_profit,
            "stop": stop,
            "breakeven": breakeven,
        }
    except Exception as e:
        print(f"摘要圖卡資料失敗 {code}: {e}", flush=True)
        return None

def _flex_text(text, size="sm", color="#111111", weight=None, wrap=True):
    obj = {"type": "text", "text": str(text), "size": size, "color": color, "wrap": wrap}
    if weight:
        obj["weight"] = weight
    return obj

def _flex_kv(label, value, color="#111111"):
    return {
        "type": "box",
        "layout": "horizontal",
        "spacing": "sm",
        "contents": [
            {"type": "text", "text": str(label), "size": "xs", "color": "#666666", "flex": 3},
            {"type": "text", "text": str(value), "size": "xs", "color": color, "weight": "bold", "align": "end", "flex": 5, "wrap": True},
        ],
    }

def make_stock_summary_flex(code, name, df, market="上市"):
    """
    B模式：單股查詢除了文字 + K線圖，再加一張 LINE Flex 摘要圖卡。
    Flex 圖卡用 LINE 原生字型顯示，避免圖片中文字變成方塊。
    """
    if not HAS_FLEX_MENU:
        return None

    snap = build_stock_snapshot_for_card(code, name, df, market)
    if not snap:
        return None

    trend = snap["trend"]
    trend_color = "#d32f2f" if trend == "偏多" else "#00897b" if trend == "偏空" else "#6d4c41"
    pct_color = "#d32f2f" if snap["pct"] >= 0 else "#00897b"
    header_color = "#111827"
    market_tag = "美股" if market == "美股" else "台股"

    body = {
        "type": "bubble",
        "size": "mega",
        "body": {
            "type": "box",
            "layout": "vertical",
            "spacing": "md",
            "contents": [
                _flex_text("⚡ HCX-AI 單股摘要", "lg", header_color, "bold"),
                _flex_text(f"{market_tag}｜{snap['code']} {snap['name']}", "md", "#1f2937", "bold"),
                {
                    "type": "box",
                    "layout": "horizontal",
                    "spacing": "sm",
                    "contents": [
                        {"type": "text", "text": f"現價 {fmt_price(snap['close'])}", "size": "sm", "weight": "bold", "color": "#111827", "flex": 5},
                        {"type": "text", "text": fmt_pct(snap["pct"]), "size": "sm", "weight": "bold", "color": pct_color, "align": "end", "flex": 3},
                    ],
                },
                {"type": "separator"},
                _flex_kv("趨勢判斷", trend, trend_color),
                _flex_kv("壓力 / 支撐", f"{fmt_price(snap['high20'])} / {fmt_price(snap['low20'])}"),
                _flex_kv("MA5 / MA20", f"{fmt_price(snap['ma5'])} / {fmt_price(snap['ma20'])}"),
                _flex_kv("MA60 / ATR14", f"{fmt_price(snap['ma60'])} / {fmt_price(snap['atr'])}"),
                {"type": "separator"},
                _flex_kv("🎯 建議進場", fmt_price(snap["entry"])),
                _flex_kv("✅ 建議停利", fmt_price(snap["take_profit"])),
                _flex_kv("🛑 建議停損", fmt_price(snap["stop"])),
                _flex_kv("🔥 回本門檻", f"{snap['breakeven']} Tick" if snap["breakeven"] != "-" else "依券商成本"),
                _flex_text("本圖卡為系統估算，不保證獲利。", "xxs", "#777777"),
            ],
        },
    }

    try:
        return FlexMessage(
            alt_text=f"HCX-AI {snap['code']} 單股摘要",
            contents=FlexContainer.from_json(json.dumps(body, ensure_ascii=False)),
        )
    except Exception as e:
        print(f"摘要圖卡建立失敗 {code}: {e}", flush=True)
        return None

def reply_text_with_images_and_card(reply_token, text, image_url="", summary_flex=None, menu=True):
    messages = [make_text_message(text, menu)]
    if image_url:
        try:
            messages.append(ImageMessage(original_content_url=image_url, preview_image_url=image_url))
        except Exception as e:
            print(f"圖片訊息建立失敗：{e}", flush=True)
    if summary_flex is not None:
        try:
            messages.append(summary_flex)
        except Exception as e:
            print(f"摘要圖卡加入失敗：{e}", flush=True)

    with ApiClient(configuration) as api_client:
        api = MessagingApi(api_client)
        api.reply_message(ReplyMessageRequest(reply_token=reply_token, messages=messages[:5]))


def reply_text_with_image(reply_token, text, image_url="", menu=True):
    messages = [make_text_message(text, menu)]
    if image_url:
        try:
            messages.append(ImageMessage(original_content_url=image_url, preview_image_url=image_url))
        except Exception as e:
            print(f"圖片訊息建立失敗：{e}", flush=True)
    with ApiClient(configuration) as api_client:
        api = MessagingApi(api_client)
        api.reply_message(ReplyMessageRequest(reply_token=reply_token, messages=messages[:5]))

@app.route("/chart/<filename>")
def chart_file(filename):
    safe = re.sub(r"[^A-Za-z0-9_\-.]", "", str(filename))
    path = CHART_DIR / safe
    if not path.exists():
        return "Not found", 404
    return send_file(str(path), mimetype="image/png")

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

def download_weekly(code, market="上市", period="2y"):
    key = ("W", code, market, period)
    now = time.time()
    if key in DATA_CACHE and now - DATA_CACHE[key]["ts"] < 1800:
        return DATA_CACHE[key]["df"], DATA_CACHE[key]["symbol"]
    for sym in yahoo_symbols(code, market):
        try:
            df = yf.download(sym, period=period, interval="1wk", progress=False, auto_adjust=False, threads=False)
            df = normalize_yf_df(df)
            if df is not None and len(df) >= 20:
                DATA_CACHE[key] = {"ts": now, "df": df, "symbol": sym}
                return df, sym
        except Exception as e:
            print(f"週K失敗 {code} {sym}: {e}", flush=True)
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
        triangle_profile = {}
        if command == "三角收斂":
            weekly_df, _ = download_weekly(code, meta.get("market","上市"), "2y")
            triangle_profile = calc_triangle_profile(df, weekly_df)
            triangle_score = triangle_profile.get("score", 0.0)
            mode, base, signal = "triangle", triangle_score, f"🔺 {triangle_profile.get('pattern','三角收斂')}"
        elif command == "當沖空": mode, base, signal = "intraday_short", short_score, "🟩 偏空當沖"
        elif command == "隔日沖": mode, base, signal = "swing", swing_score, "🟧 隔日沖觀察"
        elif command == "波段股": mode, base, signal = "swing", swing_score, "🟦 波段觀察"
        elif command == "當沖股" and short_score > long_score: mode, base, signal = "intraday_short", short_score, "🟩 偏空當沖"
        else: mode, base, signal = "intraday_long", long_score, "🟥 偏多當沖"
        win_rate, samples = estimate_win_rate(df, mode)
        rank = base * 0.82 + win_rate * (0.18 * min(samples/12, 1))
        if value_m < safe_float(os.environ.get("HCX_MIN_VALUE_M", 30), 30): rank -= 18
        if volume_lots < safe_float(os.environ.get("HCX_MIN_LOTS", 1000), 1000): rank -= 15
        row = {"code":code,"name":meta.get("name",code),"market":meta.get("market","上市"),"close":close,"pct":pct,
                "volume_lots":volume_lots,"value_m":value_m,"vol_ratio":vol_ratio,"ma5":ma5,"ma20":ma20,"ma60":ma60,
                "high20":high20,"low20":low20,"atr":atr,"score":round(base,1),"rank_score":round(rank,1),
                "win_rate":round(win_rate,1),"samples":samples,"trade_kind":mode,"signal":signal}
        if triangle_profile:
            row.update(triangle_profile)
        return row
    except Exception as e:
        print(f"score_stock 錯誤 {code}: {e}", flush=True); return None

def _is_reasonable_price_level(level, close, down_pct=0.12, up_pct=0.16):
    try:
        level = float(level)
        close = float(close)
        if close <= 0 or level <= 0:
            return False
        return close * (1 - down_pct) <= level <= close * (1 + up_pct)
    except Exception:
        return False

def _pick_near_level(levels, close, side="support", down_pct=0.08, up_pct=0.06):
    vals = []
    for lv in levels:
        if _is_reasonable_price_level(lv, close, down_pct=down_pct, up_pct=up_pct):
            vals.append(float(lv))
    if not vals:
        return None
    if side == "support":
        below = [v for v in vals if v <= close * 1.01]
        return max(below) if below else min(vals, key=lambda x: abs(x-close))
    else:
        above = [v for v in vals if v >= close * 0.99]
        return min(above) if above else min(vals, key=lambda x: abs(x-close))

def build_trade_plan(row):
    """
    V7.9.1 修正：
    當沖多/空不可直接採用距離現價太遠的 20日低點、5分K低點或異常資料。
    若支撐/壓力與現價差距過大，改用「現價附近」回測點，避免出現收盤199、進場100.5這種不合理價位。
    """
    code = row["code"]
    market = row.get("market", "上市")
    close = float(row["close"])
    atr = float(row.get("atr") or close * 0.02)
    kind = row.get("trade_kind", "intraday_long")
    t = tick_size(close)

    daily_support = row.get("low20", close - atr)
    daily_resistance = row.get("high20", close + atr)
    ma20 = row.get("ma20", None)

    support5 = daily_support
    resistance5 = daily_resistance
    support30 = daily_support
    resistance30 = daily_resistance

    try:
        df5 = download_intraday(code, market, "5m", "5d")
        if df5 is not None and not df5.empty:
            recent = df5.tail(48)
            s5 = float(pd.to_numeric(recent["Low"], errors="coerce").min())
            r5 = float(pd.to_numeric(recent["High"], errors="coerce").max())
            if _is_reasonable_price_level(s5, close, down_pct=0.10, up_pct=0.05):
                support5 = s5
            if _is_reasonable_price_level(r5, close, down_pct=0.05, up_pct=0.12):
                resistance5 = r5
    except Exception:
        pass

    try:
        df30 = download_intraday(code, market, "30m", "10d")
        if df30 is not None and not df30.empty:
            recent = df30.tail(10)
            s30 = float(pd.to_numeric(recent["Low"], errors="coerce").min())
            r30 = float(pd.to_numeric(recent["High"], errors="coerce").max())
            if _is_reasonable_price_level(s30, close, down_pct=0.10, up_pct=0.05):
                support30 = s30
            if _is_reasonable_price_level(r30, close, down_pct=0.05, up_pct=0.12):
                resistance30 = r30
    except Exception:
        pass

    # 當沖用的最小風控距離，不讓停損太貼，也不讓價位飛太遠。
    min_gap = max(atr * 0.22, close * 0.006, t * 4)
    max_pullback = max(t * 2, min(atr * 0.18, close * 0.012))

    if kind == "triangle":
        entry_raw = row.get("breakout_price") or _pick_near_level([resistance5, resistance30, daily_resistance], close, "resistance", down_pct=0.04, up_pct=0.18) or close
        stop_raw = row.get("defense_price") or _pick_near_level([support5, support30, daily_support, ma20], close, "support", down_pct=0.12, up_pct=0.04)
        entry = round_by_tick(float(entry_raw), "up")
        if stop_raw is None or float(stop_raw) < close * 0.86:
            stop_raw = entry - max(min_gap, close * 0.018)
        stop = round_by_tick(float(stop_raw), "down")
        take = round_by_tick(entry + max(entry - stop, min_gap) * 1.20, "up")

    elif kind == "intraday_short":
        near_res = _pick_near_level([resistance5, resistance30, daily_resistance, ma20], close, "resistance", down_pct=0.04, up_pct=0.08)
        near_sup = _pick_near_level([support5, support30, daily_support], close, "support", down_pct=0.10, up_pct=0.03)

        entry_raw = close if near_res is None else min(max(close, near_res), close * 1.035)
        entry = round_by_tick(entry_raw, "nearest")
        stop = round_by_tick(entry + max(min_gap, close * 0.012), "up")

        target_raw = near_sup if near_sup is not None else entry - max(stop - entry, min_gap) * 0.85
        take = round_by_tick(min(target_raw, entry - t * 2), "down")

    else:
        # 當沖多：只選現價附近的支撐。太遠的支撐視為波段支撐，不可當作當沖進場價。
        near_sup = _pick_near_level([support5, support30, daily_support, ma20], close, "support", down_pct=0.08, up_pct=0.03)
        near_res = _pick_near_level([resistance5, resistance30, daily_resistance], close, "resistance", down_pct=0.03, up_pct=0.10)

        if near_sup is not None:
            entry_raw = min(close, max(float(near_sup), close - max_pullback))
        else:
            entry_raw = close - max_pullback

        entry = round_by_tick(entry_raw, "nearest")
        stop = round_by_tick(entry - max(min_gap, close * 0.012), "down")

        target_raw = near_res if near_res is not None else entry + max(entry - stop, min_gap) * 0.90
        take = round_by_tick(max(target_raw, entry + t * 2), "up")

    # 最後防呆：若價位仍離現價太誇張，改回現價附近。
    if kind != "triangle" and abs(float(entry) - close) / max(close, 1) > 0.08:
        if kind == "intraday_short":
            entry = round_by_tick(close, "nearest")
            stop = round_by_tick(entry + max(min_gap, close * 0.012), "up")
            take = round_by_tick(entry - max(min_gap, close * 0.012) * 0.85, "down")
        else:
            entry = round_by_tick(close - max_pullback, "nearest")
            stop = round_by_tick(entry - max(min_gap, close * 0.012), "down")
            take = round_by_tick(entry + max(entry - stop, min_gap) * 0.90, "up")

    row.update({
        "entry": entry,
        "stop": stop,
        "take_profit": take,
        "tick_line": calc_tick_profit_info(entry, take)
    })
    return row

def run_quantum_scan(command):
    universe = fetch_market_universe()
    deep_n = safe_int(os.environ.get("HCX_DEEP_SCAN", 120), 120)
    if command == "波段股":
        deep_n = max(deep_n, safe_int(os.environ.get("HCX_SWING_DEEP_SCAN", 180), 180))
    if command == "三角收斂":
        deep_n = max(deep_n, safe_int(os.environ.get("HCX_TRIANGLE_DEEP_SCAN", 220), 220))
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
        "三角收斂": "🔺 三角收斂 TOP 5",
        "布林戰法": "📈 布林戰法 TOP 5",
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
        if command == "三角收斂":
            lines.append(
                f"{label}. {r['code']} {r['name']}｜{r.get('signal','')}\n"
                f"   收盤 {fmt_price(r['close'])}｜漲跌 {fmt_pct(r['pct'])}｜量比 {float(r.get('vol_ratio') or 0):.2f}\n"
                f"   🏆 AI勝率 {float(r.get('win_rate') or 0):.1f}%｜職業評分 {float(r.get('rank_score') or 0):.1f}\n"
                f"   📐 型態：{r.get('pattern','收斂整理')}\n"
                f"   📊 週K方向：{r.get('week_trend','資料不足')}｜收斂幅度 {float(r.get('shrink_pct') or 0):.1f}%\n"
                f"   🎯 突破觀察價：{fmt_price(r.get('breakout_price') or r.get('entry'))}\n"
                f"   ✅ 建議停利價：{fmt_price(r.get('take_profit'))}\n"
                f"   🛑 防守價：{fmt_price(r.get('defense_price') or r.get('stop'))}\n"
                f"   {r.get('tick_line','')}"
            )
        else:
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


# 布林戰法選股

def boll_add_bbands(df, n=20, k=2.0):
    d = df.copy()
    close = pd.to_numeric(d["Close"], errors="coerce")
    mid = close.rolling(n).mean()
    std = close.rolling(n).std(ddof=0)
    d["BB_MID"] = mid
    d["BB_UP"] = mid + k * std
    d["BB_LOW"] = mid - k * std
    d["BB_W"] = (d["BB_UP"] - d["BB_LOW"]) / d["BB_MID"].replace(0, np.nan)
    return d

def boll_slope_pct(series, lb=5):
    try:
        s = pd.to_numeric(series, errors="coerce").dropna()
        if len(s) < lb + 1:
            return 0.0
        seg = s.iloc[-(lb + 1):]
        base = float(seg.iloc[0])
        if base == 0 or not math.isfinite(base):
            return 0.0
        return float(seg.iloc[-1] - seg.iloc[0]) / abs(base)
    except Exception:
        return 0.0

def boll_is_opening_up(d, lb=5, slope_min=0.0015):
    try:
        up1 = boll_slope_pct(d["BB_UP"], lb) > slope_min
        up2 = boll_slope_pct(d["BB_MID"], lb) > slope_min
        up3 = boll_slope_pct(d["BB_LOW"], lb) > slope_min
        widen = float(d["BB_W"].iloc[-1]) > float(d["BB_W"].iloc[-(lb + 1)])
        return up1 and up2 and up3 and widen
    except Exception:
        return False

def boll_is_opening_down(d, lb=5, slope_min=0.0015):
    try:
        dn1 = boll_slope_pct(d["BB_UP"], lb) < -slope_min
        dn2 = boll_slope_pct(d["BB_MID"], lb) < -slope_min
        dn3 = boll_slope_pct(d["BB_LOW"], lb) < -slope_min
        widen = float(d["BB_W"].iloc[-1]) > float(d["BB_W"].iloc[-(lb + 1)])
        return dn1 and dn2 and dn3 and widen
    except Exception:
        return False

def boll_signal_buy_mid(d, tol=0.005, bars=3):
    try:
        x = d.iloc[-(bars + 1):].copy()
        mid = x["BB_MID"]
        close = x["Close"]
        low = x["Low"]
        touched_mid = (low <= mid * (1 + tol)).any()
        cross_up = (close.iloc[-2] < mid.iloc[-2]) and (close.iloc[-1] >= mid.iloc[-1])
        near_mid_today = abs(float(close.iloc[-1]) / max(float(mid.iloc[-1]), 0.01) - 1) <= tol
        return bool(touched_mid and (cross_up or near_mid_today))
    except Exception:
        return False

def boll_signal_buy_low(d, tol=0.005, bars=3):
    try:
        x = d.iloc[-(bars + 1):].copy()
        lowband = x["BB_LOW"]
        close = x["Close"]
        low = x["Low"]
        touched_low = (low <= lowband * (1 + tol)).any()
        rebound = close.iloc[-1] > lowband.iloc[-1]
        return bool(touched_low and rebound)
    except Exception:
        return False

def boll_bw_label(bw_pct):
    try:
        bw = float(bw_pct)
        if bw < 8: return "🧊 很低"
        if bw < 15: return "🔥 低"
        if bw < 25: return "🔥🔥 中"
        return "🚀 高"
    except Exception:
        return ""

def score_bollinger_stock(code, meta):
    try:
        df, _ = download_daily(code, meta.get("market", "上市"), "1y")
        if df is None or df.empty or len(df) < 45:
            return None

        d = boll_add_bbands(df, 20, 2.0).dropna()
        if len(d) < 28:
            return None

        last = d.iloc[-1]
        close = float(last["Close"])
        bb_up = float(last["BB_UP"])
        bb_mid = float(last["BB_MID"])
        bb_low = float(last["BB_LOW"])
        bbw_pct = float(last["BB_W"]) * 100.0
        if bbw_pct < 3.0:
            return None

        up = boll_is_opening_up(d)
        down = boll_is_opening_down(d)
        if not (up or down):
            return None

        vol_s = pd.to_numeric(d["Volume"], errors="coerce")
        vol_ma20 = float(vol_s.rolling(20).mean().iloc[-1]) if len(vol_s) >= 20 else max(float(vol_s.iloc[-1]), 1)
        vol_ratio = float(vol_s.iloc[-1]) / max(vol_ma20, 1)

        signal = ""
        note = ""
        kind = ""
        entry = take = stop = None

        if close > bb_up:
            signal = "突破上軌警戒"
            note = "股價突破上軌，偏短線過熱，適合列入停利/警戒觀察。"
            kind = "sell_watch"
            entry = round_by_tick(close, "nearest")
            take = round_by_tick(close, "nearest")
            stop = round_by_tick(bb_mid, "down")
            base_score = 58 + min(bbw_pct, 35) * 0.8 + min(vol_ratio, 3) * 4
        elif up and boll_signal_buy_mid(d):
            signal = "多：買中軌→上軌"
            note = "三線開口向上，回踩靠近中軌後轉強。"
            kind = "long_mid"
            entry = round_by_tick(bb_mid, "nearest")
            take = round_by_tick(bb_up, "up")
            stop = round_by_tick(bb_low, "down")
            close_to_mid = abs(close / max(bb_mid, 0.01) - 1)
            base_score = 72 + min(bbw_pct, 35) * 0.55 + min(vol_ratio, 3) * 5 - close_to_mid * 600
        elif down and boll_signal_buy_low(d):
            signal = "弱：買下軌→中軌"
            note = "三線開口向下，觸下軌後反彈，屬弱勢反彈型。"
            kind = "weak_rebound"
            entry = round_by_tick(bb_low, "nearest")
            take = round_by_tick(bb_mid, "up")
            stop = round_by_tick(min(float(d["Low"].tail(10).min()), bb_low * 0.98), "down")
            base_score = 62 + min(bbw_pct, 35) * 0.45 + min(vol_ratio, 3) * 4
        else:
            return None

        pct = 0.0
        try:
            prev = float(d["Close"].iloc[-2])
            pct = (close - prev) / prev * 100 if prev else 0
        except Exception:
            pass

        return {
            "code": code,
            "name": meta.get("name", code),
            "market": meta.get("market", "上市"),
            "close": close,
            "pct": pct,
            "bb_up": bb_up,
            "bb_mid": bb_mid,
            "bb_low": bb_low,
            "bbw_pct": bbw_pct,
            "vol_ratio": vol_ratio,
            "signal": signal,
            "note": note,
            "kind": kind,
            "entry": entry,
            "take_profit": take,
            "stop": stop,
            "rank_score": round(float(clip(base_score, 0, 100)), 1),
            "tick_line": calc_tick_profit_info(entry or close, take or close),
        }
    except Exception as e:
        print(f"布林戰法評分失敗 {code}: {e}", flush=True)
        return None

def run_bollinger_scan():
    universe = fetch_market_universe()
    deep_n = max(60, min(safe_int(os.environ.get("HCX_BOLL_DEEP_SCAN", 220), 220), 280))
    candidates = universe[:deep_n]
    rows = []
    workers = max(3, min(safe_int(os.environ.get("HCX_WORKERS", 8), 8), 10))
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = []
        for item in candidates:
            meta = {"name": item.get("name"), "market": item.get("market", "上市")}
            futures.append(executor.submit(score_bollinger_stock, item["code"], meta))
        for fut in as_completed(futures):
            try:
                r = fut.result()
                if r is not None:
                    rows.append(r)
            except Exception as e:
                print(f"布林候選股失敗：{e}", flush=True)

    rows = sorted(rows, key=lambda x: (float(x.get("rank_score") or 0), float(x.get("vol_ratio") or 0)), reverse=True)
    return format_bollinger_report(rows[:12])

def format_bollinger_report(rows):
    if not rows:
        return f"""⚡ HCX-AI量子雷達
🕒 時間：{query_time_text()}
📌 指令：布林戰法

本次暫無符合布林戰法的股票。
可稍後盤中再查，或等待布林通道開口與回踩訊號更明確。"""

    main_rows = [r for r in rows if r.get("kind") != "sell_watch"][:5]
    sell_rows = [r for r in rows if r.get("kind") == "sell_watch"][:5]
    if not main_rows:
        main_rows = rows[:5]

    labels = ["A", "B", "C", "D", "E"]
    lines = [
        "⚡ HCX-AI量子雷達",
        f"🕒 時間：{query_time_text()}",
        "📈 布林戰法 TOP 5",
        "━━━━━━━━━━━━━━",
    ]

    for i, r in enumerate(main_rows[:5]):
        label = labels[i] if i < len(labels) else chr(65+i)
        lines.append(
            f"{label}. {r['code']} {r['name']}｜{r.get('signal','')}｜{boll_bw_label(r.get('bbw_pct'))}\n"
            f"   收盤 {fmt_price(r['close'])}｜漲跌 {fmt_pct(r.get('pct',0))}｜量比 {float(r.get('vol_ratio') or 0):.2f}\n"
            f"   中軌 {fmt_price(r.get('bb_mid'))}｜上軌 {fmt_price(r.get('bb_up'))}｜下軌 {fmt_price(r.get('bb_low'))}\n"
            f"   📏 帶寬 {float(r.get('bbw_pct') or 0):.2f}%｜職業評分 {float(r.get('rank_score') or 0):.1f}\n"
            f"   🎯 觀察買點：{fmt_price(r.get('entry'))}\n"
            f"   ✅ 第一目標：{fmt_price(r.get('take_profit'))}\n"
            f"   🛑 防守價：{fmt_price(r.get('stop'))}\n"
            f"   🧭 {r.get('note','')}"
        )

    if sell_rows:
        lines += ["━━━━━━━━━━━━━━", "🟢 突破上軌警戒"]
        for i, r in enumerate(sell_rows[:5], start=1):
            lines.append(
                f"{i}. {r['code']} {r['name']}｜收盤 {fmt_price(r['close'])}｜上軌 {fmt_price(r.get('bb_up'))}｜帶寬 {float(r.get('bbw_pct') or 0):.2f}%"
            )

    lines += [
        "━━━━━━━━━━━━━━",
        "⚠️ 布林戰法為技術型態篩選，不保證獲利。",
        "⚠️ 請搭配成交量、1分K轉折與紀律停損。",
    ]
    return "\\n".join(lines)[:4800]

def run_command_scan(command):
    if command == "布林戰法":
        return run_bollinger_scan()
    return run_quantum_scan(command)


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
    if any(w in t for w in ["布林戰法","布林選股","布林通道","布林股","BOLL","boll","布林"]): return "布林戰法"
    if any(w in t for w in ["三角收斂","三角整理","收斂股","三角選股","三角型態"]): return "三角收斂"
    if any(w in t for w in ["波段股","波段","中線股","中線","波段選股"]): return "波段股"
    if any(w in t for w in ["當沖股","當衝股","最佳當沖","今日當沖"]): return "當沖股"
    if any(w in t for w in ["當沖多","當衝多","沖多","衝多","當沖做多"]): return "當沖多"
    if any(w in t for w in ["當沖空","當衝空","沖空","衝空","當沖做空"]): return "當沖空"
    if any(w in t for w in ["隔日沖","隔日衝","隔日"]): return "隔日沖"
    return None

def load_triangle_users():
    now = time.time()
    if TRIANGLE_AUTH_CACHE.get("users") and now - TRIANGLE_AUTH_CACHE.get("ts", 0) < 300:
        return TRIANGLE_AUTH_CACHE["users"]
    users = set()
    try:
        if TRIANGLE_AUTH_FILE.exists():
            data = json.loads(TRIANGLE_AUTH_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list):
                users = set(str(x) for x in data)
    except Exception as e:
        print(f"讀取三角收斂權限失敗：{e}", flush=True)
    TRIANGLE_AUTH_CACHE.update({"ts": now, "users": users})
    return users

def save_triangle_users(users):
    try:
        TRIANGLE_AUTH_FILE.parent.mkdir(parents=True, exist_ok=True)
        TRIANGLE_AUTH_FILE.write_text(json.dumps(sorted(list(users)), ensure_ascii=False, indent=2), encoding="utf-8")
        TRIANGLE_AUTH_CACHE.update({"ts": time.time(), "users": set(users)})
    except Exception as e:
        print(f"儲存三角收斂權限失敗：{e}", flush=True)

def is_premium_authorized(user_id):
    return str(user_id) in load_triangle_users() or is_advanced_user(str(user_id))

def is_triangle_authorized(user_id):
    return is_premium_authorized(user_id)

def set_pending_premium_command(user_id, command):
    if user_id and command:
        PREMIUM_PENDING_AUTH[str(user_id)] = {"command": command, "ts": time.time()}

def pop_pending_premium_command(user_id):
    item = PREMIUM_PENDING_AUTH.pop(str(user_id), None)
    if not item:
        return ""
    if time.time() - float(item.get("ts", 0)) > 600:
        return ""
    return item.get("command", "")

def _premium_password_ok(raw_text):
    txt = str(raw_text or "").strip()
    compact = normalize_text(txt)
    candidates = {
        normalize_text(PREMIUM_PASSWORD),
        normalize_text(TRIANGLE_PASSWORD),
        "694509",
        "HCX694509",
    }
    candidates = {x for x in candidates if x}
    if compact in candidates:
        return True
    # 允許會員回覆「密碼 694509」「專屬密碼694509」
    return any(pw in compact for pw in ["694509", "HCX694509"])

def handle_premium_password(user_id, raw_text):
    if not _premium_password_ok(raw_text):
        return "", ""
    users = load_triangle_users()
    users.add(str(user_id))
    save_triangle_users(users)
    pending = pop_pending_premium_command(user_id)
    if pending:
        return f"✅ 專屬密碼正確，已開通付費功能\n即將執行：{pending}", pending
    return "✅ 專屬密碼正確，已開通付費功能\n可使用：三角收斂、布林戰法", ""

def handle_triangle_password(user_id, raw_text):
    msg, _cmd = handle_premium_password(user_id, raw_text)
    return msg

def premium_password_message(command="付費功能"):
    return f"🔒 {command} 為付費功能\n請直接回覆專屬密碼後再使用。"

def triangle_password_message():
    return premium_password_message("三角收斂")


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
🔺 三角收斂（付費）
📈 布林戰法（付費）

觸控選單：
🔳 主選單

自動提醒：
⏰ 設定提醒 當沖多 08:50
⏰ 設定提醒 當沖空 10:00
⏰ 設定提醒 隔日沖 13:00
📋 我的提醒
🗑️ 取消提醒 當沖多

其他：
主選單
請開機
版本
會員等級
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
設定提醒 當沖空 10:00
設定提醒 隔日沖 13:00
設定提醒 布林戰法 09:10

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
            result = run_command_scan(cmd)
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
            with SCAN_LOCK: result = run_command_scan(command)
            push_long_text(user_id, result)
        except Exception as e:
            print(f"背景掃描失敗 {command}: {e}", flush=True); traceback.print_exc()
            try: push_text(user_id, f"⚠️ {command} 掃描暫時失敗，請稍後再試。")
            except Exception: pass
    threading.Thread(target=job, daemon=True).start()
    return f"""⚡ 已收到「{command}」指令

系統正在啟動 HCX-AI 量子雷達篩選中...

📊 掃描模式：HCX-AI 策略 TOP 5
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
        pw_msg, pw_command = handle_premium_password(user_id, raw)
        if pw_msg:
            if pw_command:
                reply_text(event.reply_token, pw_msg + "\n\n" + start_scan_async(user_id, pw_command))
            else:
                reply_text(event.reply_token, pw_msg)
            return
        reminder_cmd = parse_reminder_command(raw); quantum_cmd = detect_quantum_command(raw)
        if msg in ["主選單", "選單", "功能選單", "menu", "MENU"]:
            reply_main_menu(event.reply_token)
            return
        if msg in ["請開機", "開機", "開機中", "喚醒", "wake", "wakeup"] or msg.lower() in ["wake", "wake up"]:
            reply_text(event.reply_token, f"✅ HCX-AI 開機中，請等候30秒!\n🕒 {query_time_text()}")
            return
        if msg.lower() in ["版本", "version", "ver"]:
            reply_text(event.reply_token, f"✅ 目前版本：\n{APP_VERSION}"); return
        if msg in ["會員等級", "我的等級", "等級"]:
            level = "💎 進階會員" if is_advanced_user(user_id) else "✅ 一般會員"
            reply_text(event.reply_token, f"你的會員等級：\n{level}", menu=False); return
        if msg in ["我的id", "我的ID", "userid", "userID"]:
            reply_text(event.reply_token, f"你的 LINE userId：\n{user_id}", menu=False); return
        if reminder_cmd:
            reply_text(event.reply_token, handle_reminder(user_id, reminder_cmd)); return
        if quantum_cmd:
            if quantum_cmd in PREMIUM_COMMANDS and not is_premium_authorized(user_id):
                set_pending_premium_command(user_id, quantum_cmd)
                reply_text(event.reply_token, premium_password_message(quantum_cmd))
                return
            reply_text(event.reply_token, start_scan_async(user_id, quantum_cmd)); return
        m = re.search(r"\d{4}", msg)
        if m:
            code = m.group(0)
            text = analyze_one_stock(code)
            image_url = ""
            summary_flex = None
            try:
                meta = get_stock_meta(code)
                df, _ = download_daily(code, meta.get("market", "上市"), "6mo")
                if df is not None and not df.empty:
                    image_url = make_stock_chart(code, meta.get("name", code), df)
                    summary_flex = make_stock_summary_flex(code, meta.get("name", code), df, meta.get("market", "上市"))
            except Exception as e:
                print(f"單股圖片/圖卡處理失敗 {code}: {e}", flush=True)
            reply_text_with_images_and_card(event.reply_token, text, image_url, summary_flex); return
        us_symbol = is_us_symbol_text(raw)
        if us_symbol:
            text = analyze_us_stock(us_symbol)
            image_url = ""
            summary_flex = None
            try:
                df, _ = download_daily(us_symbol, "美股", "6mo")
                if df is not None and not df.empty:
                    us_name = get_us_stock_meta(us_symbol).get("name", us_symbol)
                    image_url = make_stock_chart(us_symbol, us_name, df)
                    summary_flex = make_stock_summary_flex(us_symbol, us_name, df, "美股")
            except Exception as e:
                print(f"美股圖片/圖卡處理失敗 {us_symbol}: {e}", flush=True)
            reply_text_with_images_and_card(event.reply_token, text, image_url, summary_flex); return
        reply_main_menu(event.reply_token)
    except Exception as e:
        print(f"handle_message 錯誤：{e}", flush=True); traceback.print_exc()
        try: reply_text(event.reply_token, "⚠️ 系統暫時忙碌，請稍後再試。")
        except Exception: pass

start_reminder_loop()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
