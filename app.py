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
import traceback
from io import StringIO

import pandas as pd
import yfinance as yf
import requests

app = Flask(__name__)

configuration = Configuration(access_token=os.environ["LINE_CHANNEL_ACCESS_TOKEN"])
handler = WebhookHandler(os.environ["LINE_CHANNEL_SECRET"])


# 常用股票名稱備援表：避免官方來源臨時失效時完全抓不到名稱
FALLBACK_STOCK_NAMES = {
    "2330": "台積電",
    "2454": "聯發科",
    "2317": "鴻海",
    "1717": "長興",
    "9105": "泰金寶-DR",
    "2303": "聯電",
    "1303": "南亞",
    "2002": "中鋼",
    "2603": "長榮",
    "2609": "陽明",
    "2615": "萬海",
    "2409": "友達",
    "3481": "群創",
    "2357": "華碩",
    "2382": "廣達",
    "3231": "緯創",
    "4938": "和碩",
    "2308": "台達電",
    "2412": "中華電",
    "2881": "富邦金",
    "2882": "國泰金",
    "2884": "玉山金",
    "2891": "中信金",
    "2892": "第一金",
}


def clean_stock_name_text(text):
    text = str(text).strip()
    text = re.sub(r"\s+", " ", text)
    return text


def get_stock_name_from_isin(code, mode):
    """
    從 TWSE ISIN 公開資料抓股票名稱。
    mode=2 上市，mode=4 上櫃。
    """
    try:
        url = f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}"
        headers = {
            "User-Agent": "Mozilla/5.0"
        }
        r = requests.get(url, headers=headers, timeout=10)
        r.encoding = "big5"

        tables = pd.read_html(StringIO(r.text))
        if not tables:
            return None

        df = tables[0]

        # 通常第一欄是「有價證券代號及名稱」
        first_col = df.columns[0]

        for value in df[first_col].astype(str):
            value = clean_stock_name_text(value)

            # 例：1717　長興
            if value.startswith(code):
                # 移除代號後取名稱
                name = value.replace(code, "", 1).strip()
                name = re.split(r"\s+", name)[0].strip()

                if name and name not in ["nan", "有價證券代號及名稱"]:
                    return name

    except Exception:
        return None

    return None


def get_stock_name_from_openapi(code):
    """
    從 TWSE/TPEx OpenAPI 抓股票名稱。
    """
    urls = [
        "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
        "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O",
    ]

    for url in urls:
        try:
            r = requests.get(url, timeout=8)
            if r.status_code != 200:
                continue

            data = r.json()
            for item in data:
                sid = str(
                    item.get("公司代號")
                    or item.get("股票代號")
                    or item.get("有價證券代號")
                    or ""
                ).strip()

                name = str(
                    item.get("公司簡稱")
                    or item.get("公司名稱")
                    or item.get("股票名稱")
                    or item.get("有價證券名稱")
                    or ""
                ).strip()

                if sid == code and name:
                    return name

        except Exception:
            continue

    return None


def get_stock_name_from_yfinance(code):
    """
    yfinance 名稱備援，可能是英文名稱。
    """
    symbols = [f"{code}.TW", f"{code}.TWO", code]

    for symbol in symbols:
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.get_info()
            name = info.get("shortName") or info.get("longName")
            if name:
                return str(name).strip()
        except Exception:
            continue

    return None


def get_stock_name(code):
    """
    股票名稱多來源查詢順序：
    1. 內建常用備援表
    2. TWSE OpenAPI / TPEx OpenAPI
    3. TWSE ISIN 上市 / 上櫃
    4. yfinance 名稱
    5. 最後回傳代號
    """
    if code in FALLBACK_STOCK_NAMES:
        return FALLBACK_STOCK_NAMES[code]

    name = get_stock_name_from_openapi(code)
    if name:
        return name

    name = get_stock_name_from_isin(code, 2)
    if name:
        return name

    name = get_stock_name_from_isin(code, 4)
    if name:
        return name

    name = get_stock_name_from_yfinance(code)
    if name:
        return name

    return code


def normalize_yfinance_df(df):
    """
    yfinance 有時會回傳 MultiIndex 欄位，這裡統一整理成 Open/High/Low/Close/Volume。
    """
    if df is None or df.empty:
        return df

    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]

    needed = ["Open", "High", "Low", "Close", "Volume"]
    for col in needed:
        if col not in df.columns:
            return None

    return df[needed].copy()


def download_from_yfinance(code):
    """
    Yahoo Finance 多格式查詢：
    上市 .TW、上櫃 .TWO、純代號。
    """
    symbols = [f"{code}.TW", f"{code}.TWO", code]

    for symbol in symbols:
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
                return df, symbol

        except Exception:
            continue

    return None, None


def download_from_twse(code):
    """
    TWSE 備援：上市股票日收盤資料。
    """
    try:
        today = pd.Timestamp.today()
        dfs = []

        for i in range(8):
            d = today - pd.DateOffset(months=i)
            date_str = d.strftime("%Y%m%d")
            url = "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
            params = {
                "response": "json",
                "date": date_str,
                "stockNo": code
            }

            r = requests.get(url, params=params, timeout=10)
            data = r.json()

            if data.get("stat") != "OK":
                continue

            rows = data.get("data", [])
            fields = data.get("fields", [])

            if not rows:
                continue

            temp = pd.DataFrame(rows, columns=fields)
            dfs.append(temp)

        if not dfs:
            return None, None

        raw = pd.concat(dfs, ignore_index=True)

        def roc_to_date(x):
            y, m, d = str(x).split("/")
            return pd.Timestamp(int(y) + 1911, int(m), int(d))

        def to_float(x):
            x = str(x).replace(",", "").replace("--", "").strip()
            if x == "":
                return None
            return float(x)

        df = pd.DataFrame()
        df["Date"] = raw["日期"].apply(roc_to_date)
        df["Open"] = raw["開盤價"].apply(to_float)
        df["High"] = raw["最高價"].apply(to_float)
        df["Low"] = raw["最低價"].apply(to_float)
        df["Close"] = raw["收盤價"].apply(to_float)
        df["Volume"] = raw["成交股數"].apply(to_float)

        df = df.dropna()
        df = df.drop_duplicates("Date").sort_values("Date").set_index("Date")

        if len(df) >= 60:
            return df, "TWSE上市備援"

    except Exception:
        traceback.print_exc()

    return None, None


def download_from_tpex(code):
    """
    TPEx 備援：上櫃股票日收盤資料。
    """
    try:
        today = pd.Timestamp.today()
        dfs = []

        for i in range(8):
            d = today - pd.DateOffset(months=i)
            date_str = f"{d.year - 1911}/{d.month:02d}"
            url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/tradingStock"
            params = {
                "code": code,
                "date": date_str,
                "id": "",
                "response": "csv"
            }

            r = requests.get(url, params=params, timeout=10)
            text = r.text

            if "日期" not in text or "收盤" not in text:
                continue

            lines = [line for line in text.splitlines() if len(line.split(",")) >= 7]
            csv_text = "\n".join(lines)

            temp = pd.read_csv(StringIO(csv_text))
            dfs.append(temp)

        if not dfs:
            return None, None

        raw = pd.concat(dfs, ignore_index=True)
        raw.columns = [str(c).strip().replace('"', "") for c in raw.columns]

        def roc_to_date(x):
            x = str(x).replace('"', "").strip()
            y, m, d = x.split("/")
            return pd.Timestamp(int(y) + 1911, int(m), int(d))

        def to_float(x):
            x = str(x).replace(",", "").replace('"', "").replace("--", "").strip()
            if x == "":
                return None
            return float(x)

        df = pd.DataFrame()
        df["Date"] = raw["日期"].apply(roc_to_date)
        df["Open"] = raw["開盤"].apply(to_float)
        df["High"] = raw["最高"].apply(to_float)
        df["Low"] = raw["最低"].apply(to_float)
        df["Close"] = raw["收盤"].apply(to_float)
        df["Volume"] = raw["成交股數"].apply(to_float)

        df = df.dropna()
        df = df.drop_duplicates("Date").sort_values("Date").set_index("Date")

        if len(df) >= 60:
            return df, "TPEx上櫃備援"

    except Exception:
        traceback.print_exc()

    return None, None


def get_stock_data(code):
    """
    多元查詢順序：
    1. yfinance 上市 / 上櫃 / 純代號
    2. TWSE 上市備援
    3. TPEx 上櫃備援
    """
    df, source = download_from_yfinance(code)
    if df is not None:
        return df, source

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


def stock_ai(code):
    try:
        stock_name = get_stock_name(code)
        df, used_source = get_stock_data(code)

        if df is None or df.empty or len(df) < 60:
            return f"查不到 {code} {stock_name} 的股票資料，請確認代號，或稍後再試。"

        close_series = pd.to_numeric(df["Close"], errors="coerce")
        high_series = pd.to_numeric(df["High"], errors="coerce")
        low_series = pd.to_numeric(df["Low"], errors="coerce")
        volume_series_all = pd.to_numeric(df["Volume"], errors="coerce") if "Volume" in df.columns else None

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

        trend = "偏多" if close > ma20 > ma60 else "偏空" if close < ma20 < ma60 else "震盪"

        # ATR 風控
        prev_close = close_series.shift(1)
        tr1 = high_series - low_series
        tr2 = (high_series - prev_close).abs()
        tr3 = (low_series - prev_close).abs()
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        atr14 = float(tr.rolling(14).mean().iloc[-1])

        # 主力成本估算：近20日成交量加權均價 VWAP
        if volume_series_all is not None:
            volume20 = volume_series_all.tail(20)
            price20 = close_series.tail(20)
            total_volume = float(volume20.sum())
            if total_volume > 0:
                main_cost = float((price20 * volume20).sum() / total_volume)
            else:
                main_cost = ma20
        else:
            main_cost = ma20

        main_cost_diff = close - main_cost
        main_cost_pct = main_cost_diff / main_cost * 100 if main_cost else 0

        if close > main_cost:
            main_cost_status = "股價在估算主力成本上方，籌碼相對有撐。"
        elif close < main_cost:
            main_cost_status = "股價在估算主力成本下方，短線需留意賣壓。"
        else:
            main_cost_status = "股價接近估算主力成本，短線觀察方向。"

        # 進出場點位
        breakout_buy = max(high20, close)
        breakout_stop = breakout_buy - atr14 * 1.5
        breakout_target1 = breakout_buy + atr14 * 2.0
        breakout_target2 = breakout_buy + atr14 * 3.0

        pullback_buy_low = max(low20, ma20 - atr14 * 0.5)
        pullback_buy_high = ma20 + atr14 * 0.3
        pullback_stop = pullback_buy_low - atr14 * 1.2
        pullback_target = recent_high

        short_trigger = min(low20, close)
        short_stop = short_trigger + atr14 * 1.5
        short_target = short_trigger - atr14 * 2.0

        if trend == "偏多":
            advice = "站上月線與季線，短線偏多。可優先觀察突破買進，或回測 MA20 不破後承接。"
            main_plan = f"""🚀 偏多進場規劃
突破買進：{breakout_buy:.2f}
停損：{breakout_stop:.2f}
第一目標：{breakout_target1:.2f}
第二目標：{breakout_target2:.2f}

🛡️ 回測承接
買進區間：{pullback_buy_low:.2f} ~ {pullback_buy_high:.2f}
停損：{pullback_stop:.2f}
目標：{pullback_target:.2f}"""
        elif trend == "偏空":
            advice = "跌破均線結構，短線偏弱。偏保守者先觀望，若要操作以反彈不過壓力或跌破支撐為主。"
            main_plan = f"""📉 偏空觀察規劃
跌破支撐：{short_trigger:.2f}
空方停損：{short_stop:.2f}
空方目標：{short_target:.2f}

⚠️ 若站回 MA20：{ma20:.2f}
偏空看法要降低。"""
        else:
            advice = "目前均線糾結，屬於震盪盤。建議等突破壓力或跌破支撐再決定方向。"
            main_plan = f"""🔄 震盪區間規劃
區間壓力：{high20:.2f}
區間支撐：{low20:.2f}

突破壓力轉強：{high20:.2f}
跌破支撐轉弱：{low20:.2f}

區間內不追高，等方向確認。"""

        return f"""📈 HCX AI 股票分析師

股票：{code} {stock_name}
資料來源：{used_source}

現價：{close:.2f}
漲跌：{change:.2f}
漲跌幅：{pct:.2f}%

MA5：{ma5:.2f}
MA20：{ma20:.2f}
MA60：{ma60:.2f}
ATR14：{atr14:.2f}

主力成本估算：{main_cost:.2f}
成本乖離：{main_cost_diff:.2f} / {main_cost_pct:.2f}%
籌碼判斷：{main_cost_status}

壓力：{high20:.2f}
支撐：{low20:.2f}

趨勢判斷：{trend}

AI建議：
{advice}

{main_plan}

備註：主力成本為近20日成交量加權均價估算，非券商實際持股成本。
"""

    except Exception as e:
        print("stock_ai 發生錯誤：", str(e), flush=True)
        traceback.print_exc()
        return f"查詢 {code} 時發生錯誤，請稍後再試。"


@app.route("/")
def home():
    return "HCX AI LINE BOT 運作中"


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

    msg = event.message.text.strip().replace("\n", "").replace("/", "")

    try:
        if msg.isdigit() and len(msg) == 4:
            reply = stock_ai(msg)
        else:
            reply = "請輸入4碼股票代號，例如：2330、2454、2317、1717"

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
