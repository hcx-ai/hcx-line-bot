from flask import Flask, request

app = Flask(__name__)

@app.route("/")
def home():
    return "HCX LINE BOT 運作中"

@app.route("/callback", methods=["POST"])
def callback():
    return "OK"

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
