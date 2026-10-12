import json
import os
import sys
import time
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import yfinance as yf

TZ = ZoneInfo("Europe/Istanbul")
STATE_FILE = "state.json"

TICKERS = """
ATAGY EUYO SKYLP IDGYO VKFYO RODRG VKING GRNYO AVTUR MTRYO
ATLAS MZHLD GLBMD AKYHO LUKSK TDGYO DOGUB AGYO ERSU OYAYO
CUSAN SILVR ULAS IHYAY SAMAT BYDNR DESPC PAMEL HUBVC BAKAB
TSGYO ORCAY HATEK BRKSN IZINV SANKO YKSLN COSMO ICUGS SEKFK
ZEDUR DERIM DITAS IHEVA OBASE CEOEM DOFER BEYAZ KRTEK OYLUM
PRDGS MEDTR DURKN IHGZT ACSEL ISSEN SANEL SMART EYGYO OZRDN
BIZIM SEYKM VERTU FADE DOKTA ARZUM INTEM SEKUR DCTTR YAYLA
SONME YESIL BAYRK DGGYO GSDDE DUNYH TLMAN ETYAT PSDTC OYYAT
MAKIM GEDZA MARBL KLMSN OZGYO EUKYO SERNT TMPOL DGNMO SEGYO
ANGEN HURGZ KONKA BMSCH RNPOL UNLU EDATA MEKAG SOKE MHRGY
BIGTK OTTO BLCYT INGRM GEDIK PNLSN ARFYE CONSE BOSSA EGSER
MNDTR BORLS AVHOL DMSAS MSGYO PRKME MERKO PKART ISYAT SEGMN
ARTMS KUTPO EKSUN RUBNS EPLAS MAKTK KIMMR OSMEN OSTIM TURGG
BRKVY INFO KNFRT BEGYO DESA MEPET DZGYO AKMGY SNICA SOHOE
MACKO GARFA SKTAS ENSRI AHSGY HKTM YAPRK HOROZ ULUFA AVOD
VRGYO PLTUR BKRGY DGATE BULGS EGEPO NIBAS PKENT AZTEK GOLDA
BNTAS AKSUE DYOBY BINBN DAGI MERCN ULUSE ARENA A1YEN FLAP
ELITE RTALB LXGYO SANFM KZGYO KAPLM GOODY ZGYO PRKAB DURDO
MTRKS GZNMI KRGYO FMIZP HDFGS AYCES ALKA PINSU ATAKP GLRYH
RUZYE SKYMD DARDL ONRYT KFEIN EMKEL IHLGM A1CAP TEZOL GENKM
""".split()
SYMBOLS = [t + ".IS" for t in TICKERS]

# ---- ayarlar ----
MIN_AVG_TL_VOLUME = 3_000_000  # ortalama günlük işlem hacmi (TL) alt sınırı
MIN_CHANGE = 2.0    # gün içi: önceki kapanışa göre en az % artış
MAX_CHANGE = 6.0    # bundan fazlaysa çoğunlukla geç kalınmış demektir
MIN_REL_VOL = 2.0   # o ana kadarki hacim / beklenen hacim
NEAR_HIGH = 0.985   # fiyat günün zirvesinin en az %98,5'inde olmalı
MAX_SPREAD = 1.5    # alış-satış farkı en fazla % (borsapy kaynağında)
DATA_DELAY_MIN = 15  # veri gecikmesi (dk), beklenen hacim hesabı için


def now_tr():
    return datetime.now(TZ)


# ---------- yardımcılar ----------
def send(text):
    r = requests.post(
        f"https://api.telegram.org/bot{os.environ['TELEGRAM_TOKEN']}/sendMessage",
        data={"chat_id": os.environ["TELEGRAM_CHAT_ID"], "text": text},
        timeout=30,
    )
    r.raise_for_status()


def load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f)


def download(period, interval):
    for attempt in range(3):
        try:
            data = yf.download(
                SYMBOLS,
                period=period,
                interval=interval,
                group_by="ticker",
                auto_adjust=True,
                progress=False,
                threads=True,
            )
            if data is not None and not data.empty:
                return data
        except Exception as e:
            print("indirme hatası:", repr(e))
        time.sleep(5 * (attempt + 1))
    return None


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


# ---------- günlük sinyal (kapanış sonrası, yfinance) ----------
def check_daily(df):
    close = df["Close"]
    e9 = close.ewm(span=9, adjust=False).mean()
    e21 = close.ewm(span=21, adjust=False).mean()
    cross = e9.iloc[-1] > e21.iloc[-1] and (e9 <= e21).iloc[-3:-1].any()
    r = rsi(close).iloc[-1]
    vol_ok = df["Volume"].iloc[-1] > 1.5 * df["Volume"].iloc[-21:-1].mean()
    liq = (close * df["Volume"]).iloc[-20:].mean() >= MIN_AVG_TL_VOLUME
    if cross and 40 <= r <= 65 and vol_ok and liq:
        price = close.iloc[-1]
        stop = price - 2 * atr(df).iloc[-1]
        return price, r, stop
    return None


def run_daily():
    data = download("6mo", "1d")
    if data is None:
        raise RuntimeError("Günlük veri indirilemedi")
    hits = []
    for s in SYMBOLS:
        try:
            df = data[s].dropna()
            if len(df) < 40:
                continue
            res = check_daily(df)
            if res:
                hits.append((s[:-3], *res))
        except Exception:
            continue
    if hits:
        lines = [
            f"{t}: {p:.2f} TL | RSI {r:.0f} | stop ~{st:.2f}" for t, p, r, st in hits
        ]
        msg = "AL sinyali adayları (günlük):\n" + "\n".join(lines)
    else:
        msg = "Bugün günlük sinyal yok."
    send(msg + "\n\nYatırım tavsiyesi değildir.")


# ---------- gün içi: borsapy (TradingView) kaynağı ----------
def session_fraction(now):
    """Seansın (10:00-18:00) ne kadarının geçtiği, veri gecikmesi düşülerek."""
    minutes = now.hour * 60 + now.minute - 10 * 60 - DATA_DELAY_MIN
    return min(max(minutes / 480, 0.08), 1.0)


def fetch_quotes():
    import borsapy as bp

    stream = bp.TradingViewStream()
    stream.connect()
    quotes = {}
    try:
        for t in TICKERS:
            stream.subscribe(t)
        deadline = time.time() + 60
        for t in TICKERS:
            timeout = max(0.5, min(4.0, deadline - time.time()))
            try:
                q = stream.wait_for_quote(t, timeout=timeout)
            except Exception:
                q = None
            if q and q.get("last"):
                quotes[t] = dict(q)
    finally:
        try:
            stream.disconnect()
        except Exception:
            pass
    return quotes


def get_avg_stats(tickers):
    """Son ~20 seansın ortalama hacmi (adet) ve işlem hacmi (TL)."""
    import borsapy as bp

    today = pd.Timestamp(now_tr().date())
    out = {}
    for t in tickers:
        try:
            df = bp.Ticker(t).history(period="1ay")
            df = df.dropna(subset=["Close", "Volume"])
            idx = pd.to_datetime(df.index)
            if idx.tz is not None:
                idx = idx.tz_localize(None)
            df.index = idx
            df = df[df.index.normalize() < today].tail(20)
            if len(df) < 5:
                continue
            out[t] = {
                "vol": float(df["Volume"].mean()),
                "tl": float((df["Close"] * df["Volume"]).mean()),
            }
        except Exception as e:
            print("ortalama hacim hatası", t, repr(e))
    return out


def check_quote(t, q, avg, frac):
    last, prev = q.get("last"), q.get("prev_close")
    if not last or not prev:
        return None
    change = q.get("change_percent")
    if change is None:
        change = (last / prev - 1) * 100
    if not (MIN_CHANGE <= change <= MAX_CHANGE):
        return None

    a = avg.get(t)
    if not a or a["tl"] < MIN_AVG_TL_VOLUME or a["vol"] <= 0:
        return None
    rel_vol = (q.get("volume") or 0) / (a["vol"] * frac)
    if rel_vol < MIN_REL_VOL:
        return None

    high = q.get("high") or last
    opn = q.get("open") or 0
    if last < NEAR_HIGH * high or (opn and last <= opn):
        return None

    bid, ask = q.get("bid") or 0, q.get("ask") or 0
    spread = (ask - bid) / last * 100 if bid and ask and ask >= bid else None
    if spread is not None and spread > MAX_SPREAD:
        return None

    return {
        "price": last,
        "change": change,
        "rel_vol": rel_vol,
        "bid": bid,
        "ask": ask,
        "spread": spread,
        "low": q.get("low"),
    }


def scan_borsapy(state, now):
    quotes = fetch_quotes()
    if not quotes:
        raise RuntimeError("borsapy'den quote gelmedi")
    today = str(now.date())
    if state.get("avg_date") != today or not state.get("avg"):
        avg = get_avg_stats(TICKERS)
        if avg:
            state["avg"], state["avg_date"] = avg, today
    avg = state.get("avg", {})
    if not avg:
        raise RuntimeError("Ortalama hacim verisi alınamadı")
    frac = session_fraction(now)
    hits = []
    for t, q in quotes.items():
        if t in state["alerted"]:
            continue
        try:
            res = check_quote(t, q, avg, frac)
        except Exception:
            continue
        if res:
            hits.append((t, res))
    return hits


# ---------- gün içi: yfinance yedek kaynağı ----------
def check_intraday(df, today):
    df = df.dropna(subset=["Close", "Volume"]).copy()
    if df.empty:
        return None
    idx = df.index
    if idx.tz is None:
        idx = idx.tz_localize("UTC")
    df.index = idx.tz_convert(TZ)
    df["d"] = [t.date() for t in df.index]
    td = df[df["d"] == today]
    prior = df[df["d"] < today]
    if len(td) < 3 or prior.empty:
        return None

    prev_close = prior["Close"].iloc[-1]
    price = td["Close"].iloc[-1]
    change = (price / prev_close - 1) * 100
    if not (MIN_CHANGE <= change <= MAX_CHANGE):
        return None

    g = prior.groupby("d")
    day_vol = g["Volume"].sum()
    day_bars = g["Volume"].count()
    if len(day_vol) < 2 or day_vol.mean() <= 0:
        return None
    avg_tl = (prior["Close"] * prior["Volume"]).groupby(prior["d"]).sum().mean()
    if avg_tl < MIN_AVG_TL_VOLUME:
        return None

    frac = min(max(len(td) / day_bars.median(), 0.05), 1.0)
    rel_vol = td["Volume"].sum() / (day_vol.mean() * frac)
    if rel_vol < MIN_REL_VOL:
        return None

    typical = (td["High"] + td["Low"] + td["Close"]) / 3
    vwap = (typical * td["Volume"]).sum() / td["Volume"].sum()
    day_high = td["High"].max()
    if price < vwap or price < NEAR_HIGH * day_high:
        return None

    return {
        "price": price,
        "change": change,
        "rel_vol": rel_vol,
        "vwap": vwap,
        "low": td["Low"].min(),
    }


def scan_yfinance(skip, today):
    data = download("5d", "5m")
    if data is None:
        raise RuntimeError("Gün içi veri indirilemedi")
    hits = []
    for s in SYMBOLS:
        t = s[:-3]
        if t in skip:
            continue
        try:
            res = check_intraday(data[s], today)
        except Exception:
            continue
        if res:
            hits.append((t, res))
    return hits


# ---------- gün içi ana akış ----------
def fmt(t, r):
    parts = [f"{t}: {r['price']:.2f} TL", f"%{r['change']:+.1f}", f"hacim {r['rel_vol']:.1f}x"]
    if r.get("spread") is not None:
        parts.append(f"alış {r['bid']:.2f} / satış {r['ask']:.2f} (fark %{r['spread']:.1f})")
    if r.get("vwap"):
        parts.append(f"VWAP {r['vwap']:.2f}")
    if r.get("low"):
        parts.append(f"günün dibi {r['low']:.2f}")
    return " | ".join(parts)


def run_intraday():
    now = now_tr()
    mins = now.hour * 60 + now.minute
    if now.weekday() >= 5 or not (10 * 60 + 20 <= mins < 18 * 60):
        print("Piyasa kapalı ya da açılış gürültüsü dönemi, çıkılıyor")
        return
    today = now.date()
    state = load_state()
    if state.get("date") != str(today):
        state["date"] = str(today)
        state["alerted"] = []

    source = "borsapy"
    try:
        hits = scan_borsapy(state, now)
    except Exception as e:
        print("borsapy başarısız, yfinance'e dönülüyor:", repr(e))
        source = "yfinance"
        hits = scan_yfinance(set(state["alerted"]), today)

    if hits:
        hits.sort(key=lambda x: x[1]["rel_vol"], reverse=True)
        send(
            f"⚡ Gün içi momentum adayı (kaynak: {source}, veri ~15 dk gecikmeli):\n"
            + "\n".join(fmt(t, r) for t, r in hits)
            + "\n\nTahmin değil, momentum filtresi. Yatırım tavsiyesi değildir."
        )
        state["alerted"] += [t for t, _ in hits]
    save_state(state)


# ---------- test modu ----------
def run_test():
    lines = []
    t0 = time.time()
    try:
        quotes = fetch_quotes()
        lines.append(
            f"borsapy: {len(quotes)}/{len(TICKERS)} hisse için quote geldi ({time.time() - t0:.0f} sn)"
        )
        sample = list(quotes)[:3]
        for t in sample:
            q = quotes[t]
            lines.append(
                f"{t}: son {q.get('last')} | önceki {q.get('prev_close')} | "
                f"alış {q.get('bid')} | satış {q.get('ask')} | hacim {q.get('volume')} | "
                f"değişim %{q.get('change_percent')}"
            )
        for t, a in get_avg_stats(sample).items():
            lines.append(f"{t} ort. günlük hacim: {a['vol']:,.0f} adet / {a['tl']:,.0f} TL")
    except Exception as e:
        lines.append(f"borsapy HATA: {type(e).__name__}: {e}")
    send("🔧 Test sonucu\n" + "\n".join(lines))


# ---------- giriş ----------
def notify_error(mode, e):
    try:
        if mode == "anlik":
            state = load_state()
            today = str(now_tr().date())
            if state.get("error_date") == today:
                return  # günde en fazla 1 hata mesajı
            state["error_date"] = today
            save_state(state)
        send(f"⚠️ Script hata verdi ({mode}): {type(e).__name__}")
    except Exception:
        pass


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "gunluk"
    try:
        if mode == "anlik":
            run_intraday()
        elif mode == "test":
            run_test()
        else:
            run_daily()
    except Exception as e:
        print("HATA:", repr(e))
        notify_error(mode, e)
        raise


if __name__ == "__main__":
    main()
