"""Fiyat sorgusu: verdiğin fiyata yakın olan hisseleri listeler (200 hisse arasından).
Kullanım: python fiyat.py FIYAT [TOLERANS_YUZDE]   örn: python fiyat.py 8,50 5
Fiyatlar Yahoo'dan gelir (seans içinde yaklaşık 15 dk gecikmeli, seans dışında son kapanış)."""
import sys

import pandas as pd

from sinyal import SYMBOLS, download, now_tr, send

MAX_ROWS = 40


def parse_num(x):
    return float(str(x).strip().replace(",", ".").replace("TL", "").replace("tl", ""))


def run(price, tol):
    data = download("5d", "1d")
    if data is None:
        raise RuntimeError("Fiyat verisi indirilemedi")
    rows, last_date = [], None
    for s in SYMBOLS:
        try:
            c = data[s]["Close"].dropna()
            if len(c) < 1:
                continue
            last = float(c.iloc[-1])
            prev = float(c.iloc[-2]) if len(c) > 1 else None
            day = (last / prev - 1) * 100 if prev else None
            diff = (last / price - 1) * 100
            if abs(diff) <= tol:
                rows.append((abs(diff), s[:-3], last, diff, day))
            d = pd.Timestamp(c.index[-1]).date()
            last_date = max(last_date, d) if last_date else d
        except Exception:
            continue
    rows.sort()
    stale = ""
    if last_date and last_date != now_tr().date():
        stale = f" (son veri: {last_date:%d.%m.%Y} kapanışı)"
    head = f"💲 {price:g} TL'ye yakın hisseler (±%{tol:g}){stale}"
    if not rows:
        send(head + "\n\nBu aralıkta hisse yok. Toleransı artırıp tekrar dene.")
        return
    lines = []
    for _, t, last, diff, day in rows[:MAX_ROWS]:
        d = f" | bugün %{day:+.1f}" if day is not None else ""
        lines.append(f"{t}: {last:.2f} TL (fark %{diff:+.1f}){d}")
    extra = f"\n… ve {len(rows) - MAX_ROWS} hisse daha" if len(rows) > MAX_ROWS else ""
    send(head + f"\nToplam {len(rows)} hisse, yakından uzağa:\n\n" + "\n".join(lines) + extra
         + "\n\nFiyatlar yaklaşık 15 dk gecikmeli olabilir. Yatırım tavsiyesi değildir.")


def main():
    if len(sys.argv) < 2:
        raise SystemExit("Kullanım: python fiyat.py FIYAT [TOLERANS]")
    price = parse_num(sys.argv[1])
    tol = parse_num(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].strip() else 5.0
    try:
        run(price, tol)
    except Exception as e:
        print("HATA:", repr(e))
        try:
            send(f"⚠️ Fiyat sorgusu hata verdi: {type(e).__name__}: {e}")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
