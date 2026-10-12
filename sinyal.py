import os
import requests
import pandas as pd
import yfinance as yf

TICKERS = """
ATAGY EUYO SKYLP IDGYO VKFYO RODRG VKING GRNYO AVTUR MTRYO
ATLAS MZHLD GLBMD AKYHO LUKSK TDGYO DOGUB AGYO ERSU OYAYO
CUSAN SILVR ULAS IHYAY SAMAT BYDNR DESPC PAMEL HUBVC BAKAB
TSGYO ORCAY HATEK BRKSN IZINV SANKO YKSLN COSMO ICUGS SEKFK
ZEDUR DERIM DITAS IHEVA OBASE CEOEM DOFER BEYAZ KRTEK OYLUM
""".split()
SYMBOLS = [t + ".IS" for t in TICKERS]

MIN_AVG_TL_VOLUME = 3_000_000  # son 20 gün ortalama günlük işlem hacmi (TL)


def rsi(close, n=14):
    d = close.diff()
    up = d.clip(lower=0).ewm(alpha=1 / n, adjust=False).mean()
    down = (-d.clip(upper=0)).ewm(alpha=1 / n, adjust=False).mean()
    return 100 - 100 / (1 + up / down)


def atr(df, n=14):
    pc = df["Close"].shift()
    tr = pd.concat(
        [df["High"] - df["Low"], (df["High"] - pc).abs(), (df["Low"] - pc).abs()],
        axis=1,
    ).max(axis=1)
    return tr.ewm(alpha=1 / n, adjust=False).mean()


def check(df):
    close = df["Close"]
    e9 = close.ewm(span=9, adjust=False).mean()
    e21 = close.ewm(span=21, adjust=False).mean()
    # EMA9, son 2 gün içinde EMA21'i yukarı kesti
    cross = e9.iloc[-1] > e21.iloc[-1] and (e9 <= e21).iloc[-3:-1].any()
    r = rsi(close).iloc[-1]
    # bugünkü hacim 20 günlük ortalamanın 1.5 katından fazla
    vol_ok = df["Volume"].iloc[-1] > 1.5 * df["Volume"].iloc[-21:-1].mean()
    # yeterli likidite
    liq = (close * df["Volume"]).iloc[-20:].mean() >= MIN_AVG_TL_VOLUME
    if cross and 40 <= r <= 65 and vol_ok and liq:
        price = close.iloc[-1]
        stop = price - 2 * atr(df).iloc[-1]
        return price, r, stop
    return None


def main():
    data = yf.download(
        SYMBOLS, period="6mo", group_by="ticker", auto_adjust=True, progress=False
    )
    hits = []
    for s in SYMBOLS:
        try:
            df = data[s].dropna()
            if len(df) < 40:
                continue
            res = check(df)
            if res:
                hits.append((s.replace(".IS", ""), *res))
        except Exception:
            continue

    if hits:
        lines = [
            f"{t}: {p:.2f} TL | RSI {r:.0f} | stop ~{st:.2f}" for t, p, r, st in hits
        ]
        msg = "AL sinyali adayları:\n" + "\n".join(lines)
    else:
        msg = "Bugün sinyal yok."
    msg += "\n\nYatırım tavsiyesi değildir."

    requests.post(
        f"https://api.telegram.org/bot{os.environ['TELEGRAM_TOKEN']}/sendMessage",
        data={"chat_id": os.environ["TELEGRAM_CHAT_ID"], "text": msg},
        timeout=30,
    )


if __name__ == "__main__":
    main()
