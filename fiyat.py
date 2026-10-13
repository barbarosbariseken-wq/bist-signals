"""Fiyat sorgusu: verdiğin fiyata yakın olan hisseleri listeler (tüm BIST; olmazsa takip listesindeki 200 hisse).
Kullanım: python fiyat.py FIYAT [TOLERANS_YUZDE] [EN_AZ_GUNLUK_YUZDE]   örn: python fiyat.py 8,50 5 9
Tüm BIST için TradingView tarayıcı servisi, yedek olarak Yahoo kullanılır (seans içinde ~15 dk gecikmeli, seans dışında son kapanış)."""
import sys

import pandas as pd
import requests
import yfinance as yf

from sinyal import SYMBOLS, download, now_tr, send

MAX_ROWS = 40


def parse_num(x):
    return float(str(x).strip().replace(",", ".").replace("TL", "").replace("tl", ""))


def all_bist():
    """TradingView tarayıcısından tüm BIST hisseleri: {sembol: (fiyat, günlük %)}. Başarısızsa boş döner."""
    body = {
        "filter": [{"left": "type", "operation": "equal", "right": "stock"}],
        "options": {"lang": "en"},
        "markets": ["turkey"],
        "symbols": {"query": {"types": []}, "tickers": []},
        "columns": ["name", "close", "change"],
        "sort": {"sortBy": "name", "sortOrder": "asc"},
        "range": [0, 1500],
    }
    try:
        r = requests.post("https://scanner.tradingview.com/turkey/scan", json=body, timeout=30)
        r.raise_for_status()
        out = {}
        for item in r.json().get("data", []):
            d = item.get("d") or []
            if len(d) >= 3 and d[0] and d[1]:
                out[str(d[0])] = (float(d[1]), None if d[2] is None else float(d[2]))
        return out
    except Exception as e:
        print("tarayıcı servisi başarısız:", repr(e))
        return {}


def from_watchlist():
    """Yedek: sadece takip listesindeki 200 hisse, Yahoo'dan."""
    data = download("5d", "1d")
    if data is None:
        raise RuntimeError("Fiyat verisi indirilemedi")
    out, last_date = {}, None
    for s in SYMBOLS:
        try:
            c = data[s]["Close"].dropna()
            if len(c) < 1:
                continue
            last = float(c.iloc[-1])
            prev = float(c.iloc[-2]) if len(c) > 1 else None
            out[s[:-3]] = (last, (last / prev - 1) * 100 if prev else None)
            d = pd.Timestamp(c.index[-1]).date()
            last_date = max(last_date, d) if last_date else d
        except Exception:
            continue
    return out, last_date


def extras(tickers):
    """Aday hisseler için 6 aylık dip ve zirve: {sembol: (dip, dip_tarihi, zirve, zirve_tarihi)}. Başarısızsa boş."""
    out = {}
    if not tickers:
        return out
    try:
        data = yf.download([t + ".IS" for t in tickers], period="6mo", interval="1d",
                           group_by="ticker", auto_adjust=True, progress=False, threads=True)
        for t in tickers:
            try:
                try:
                    df = data[t + ".IS"]
                except Exception:
                    df = data  # tek hissede bazı sürümler düz tablo döndürür
                lo, hi = df["Low"].dropna(), df["High"].dropna()
                out[t] = (float(lo.min()), lo.idxmin(), float(hi.max()), hi.idxmax())
            except Exception:
                continue
    except Exception as e:
        print("geçmiş veri alınamadı:", repr(e))
    return out


def run(price, tol, min_day=None):
    quotes = all_bist()
    last_date = None
    if len(quotes) >= 300:
        scope = f"tüm BIST ({len(quotes)} hisse)"
    else:
        quotes, last_date = from_watchlist()
        scope = f"sadece takip listesindeki {len(quotes)} hisse (tüm BIST verisi alınamadı)"
    rows = []
    for t, (last, day) in quotes.items():
        diff = (last / price - 1) * 100
        if min_day is not None and (day is None or day < min_day):
            continue
        if abs(diff) <= tol:
            rows.append((abs(diff), t, last, diff, day))
    rows.sort()
    stale = ""
    if last_date and last_date != now_tr().date():
        stale = f"\nSon veri: {last_date:%d.%m.%Y} kapanışı"
    md = f", bugün en az %{min_day:g} yükselmiş olanlar" if min_day is not None else ""
    head = f"💲 {price:g} TL'ye yakın hisseler (±%{tol:g}{md})\nKapsam: {scope}{stale}"
    if not rows:
        send(head + "\n\nBu aralıkta hisse yok. Toleransı artırıp tekrar dene.")
        return
    lines = []
    ex = extras([r[1] for r in rows[:15]])
    for _, t, last, diff, day in rows[:MAX_ROWS]:
        d = f" | bugün %{day:+.1f}" if day is not None else ""
        e = ""
        if t in ex:
            lo, lod, hi, hid = ex[t]
            e = f"\n   6 ay: dip {lo:.2f} ({pd.Timestamp(lod):%d.%m}), zirve {hi:.2f} ({pd.Timestamp(hid):%d.%m})"
        lines.append(f"{t}: {last:.2f} TL (fark %{diff:+.1f}){d}{e}")
    extra = f"\n… ve {len(rows) - MAX_ROWS} hisse daha" if len(rows) > MAX_ROWS else ""
    send(head + f"\nToplam {len(rows)} hisse, yakından uzağa:\n\n" + "\n".join(lines) + extra
         + "\n\nFiyatlar yaklaşık 15 dk gecikmeli olabilir. Yatırım tavsiyesi değildir.")


def main():
    if len(sys.argv) < 2:
        raise SystemExit("Kullanım: python fiyat.py FIYAT [TOLERANS]")
    price = parse_num(sys.argv[1])
    tol = parse_num(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].strip() else 5.0
    min_day = parse_num(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3].strip() else None
    try:
        run(price, tol, min_day)
    except Exception as e:
        print("HATA:", repr(e))
        try:
            send(f"⚠️ Fiyat sorgusu hata verdi: {type(e).__name__}: {e}")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
