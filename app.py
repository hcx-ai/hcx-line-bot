```python
from flask import Flask, request
from linebot import LineBotApi, WebhookHandler
from linebot.models import TextSendMessage
from linebot.exceptions import InvalidSignatureError
from linebot.models.events import MessageEvent
from linebot.models.messages import TextMessage
import os
import traceback
import yfinance as yf

app = Flask(__name__)

line_bot_api = LineBotApi(os.environ["LINE_CHANNEL_ACCESS_TOKEN"])
handler = WebhookHandler(os.environ["LINE_CHANNEL_SECRET"])


def stock_ai(code):
    try:
        symbols = [f"{code}.TW", f"{code}.TWO"]

        df = None
        used_symbol = None

        for symbol in symbols:
            temp = yf.download(symbol, period="6mo", interval="1d", progress=False, auto_adjust=False)

            if temp is not None and not temp.empty:
                df = temp
                used_symbol = symbol
                break

        if df is None or df.empty:
            return f"查不到 {code} 的股票資料，請確認代號。"

        close_series = df["Close"]
        high_series = df["High"]
        low_series = df["Low"]

        close = float(close_series.iloc[-1])
        prev = float(close_series.iloc[-2])
        change = close - prev
        pct = change / prev * 100

        ma20 = float(close_series.rolling(20).mean().iloc[-1])
        ma60 = float(close_series.rolling(60).mean().iloc[-1])
        high20 = float(high_series.tail(20).max())
        low20 = float(low_series.tail(20).min())

        trend = "偏多" if close > ma20 > ma60 else "偏空" if close < ma20 < ma60 else "震盪"

        if trend == "偏多":
            advice = "站上月線與季線，短線偏多，可觀察回測不破 MA20。"
        elif trend == "偏空":
            advice = "跌破均線結構，短線偏弱，建議保守觀察。"
        else:
            advice = "目前均線糾結，屬於震盪盤，建議等突破方向。"

        return f"""📈 HCX AI 股票分析師

股票代號：{code}
資料來源：{used_symbol}

現價：{close:.2f}
漲跌：{change:.2f}
漲跌幅：{pct:.2f}%

MA20：{ma20:.2f}
MA60：{ma60:.2f}

壓力：{high20:.2f}
支撐：{low20:.2f}

趨勢判斷：{trend}

AI建議：
{advice}
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


@handler.add(MessageEvent, message=TextMessage)
def handle_message(event):
    print("========== 收到使用者訊息 ==========", flush=True)
    print(event.message.text, flush=True)

    msg = event.message.text.strip().replace("\n", "").replace("/", "")

    try:
        if msg.isdigit() and len(msg) == 4:
            reply = stock_ai(msg)
        else:
            reply = "請輸入4碼股票代號，例如：2330、2454、2317"

        print("========== 準備回覆 ==========", flush=True)
        print(reply, flush=True)

        line_bot_api.reply_message(
            event.reply_token,
            TextSendMessage(text=reply)
        )

        print("========== 回覆成功 ==========", flush=True)

    except Exception as e:
        print("handle_message 發生錯誤：", str(e), flush=True)
        traceback.print_exc()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
```
