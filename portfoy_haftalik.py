"""Haftalık portföy raporu: hafta başında portföyün değeri ne kadardı, hafta sonunda ne kadar oldu.
Hafta içinde hisse ekleyip çıkardıysan ya da lot değiştirdiysen bunu şifreli portföy defterinden
(portfoy_defter.py) bilir ve her günü o günkü portföyüne göre hesaplar. Kazanç = her gün, o günün
başında elinde olan hisselerin fiyat değişimi. Aldığın ya da sattığın hissenin parası kâr/zarar sayılmaz.
Hafta başı değeri = geçen haftanın son kapanışı. Fiyatlar günlük kapanış fiyatlarıdır (Yahoo)."""
import os
from datetime import timedelta

import pandas as pd
import yfinance as yf

from sinyal import TZ, now_tr, send
from portfoy import parse_portfolio
import portfoy_defter as dfr

INDEX = "XU100.IS"


def load_daily(syms):
    tk = [s + ".IS" for s in syms] + [INDEX]
    data = yf.download(tk, period="1mo", interval="1d", group_by="ticker",
                       auto_adjust=True, progress=False, threads=True)
    out = {}
    for t in tk:
        try:
            try:
                df = data[t]
            except Exception:
                df = data
            df = df.dropna(subset=["Close"]).copy()
            idx = pd.to_datetime(df.index)
            if idx.tz is not None:
                idx = idx.tz_convert(TZ).tz_localize(None)
            df.index = idx.normalize()
            if len(df):
                out[t] = df["Close"].astype(float)
        except Exception:
            continue
    return out


def build_ledger(today_str, current, warn):
    """Defter + bugünkü PORTFOY (bugün değiştirdiysen henüz kaydedilmemiş olabilir)."""
    ledger = {}
    if os.environ.get("SNAPSHOT_KEY", "").strip():
        try:
            ledger = dfr.load()
        except ValueError as e:
            warn.append(str(e))
    else:
        warn.append("SNAPSHOT_KEY yok: hafta içi değişiklikler bilinemez, güncel portföy tüm hafta sende sayıldı.")
    last = dfr.latest_before(ledger, today_str)
    if current and (last is None or last[1] != current):
        ledger[today_str] = current
    return ledger


def run():
    now = now_tr()
    today = now.date()
    today_str = today.strftime("%Y-%m-%d")
    monday = today - timedelta(days=today.weekday())
    pf = parse_portfolio(os.environ.get("PORTFOY", ""))
    current = dfr.holdings(pf)
    warn = []
    ledger = build_ledger(today_str, current, warn)
    base = dfr.holdings(parse_portfolio(os.environ.get("PORTFOY_HAFTA_BASI", ""))) or None
    if not ledger and not base:
        send("⚠️ Portföy boş ya da lotları yazılı değil. PORTFOY secret'ına hisse ve lot yaz.")
        return

    # hafta boyunca elde bulunan tüm hisseler
    syms = set(current)
    for d, h in ledger.items():
        syms |= set(h)
    syms |= set(base or {})
    px = load_daily(sorted(syms))
    idxs = px.get(INDEX)
    cal_src = idxs if idxs is not None else max(px.values(), key=len)
    dates = [d for d in cal_src.index if d.date() <= today]
    week = [d for d in dates if d.date() >= monday]
    prev = [d for d in dates if d.date() < monday]
    if not week:
        send("⚠️ Haftalık portföy raporu için bu haftaya ait fiyat verisi yok.")
        return
    cal = ([prev[-1]] if prev else []) + week
    if not prev:
        warn.append("Geçen haftanın kapanışı yok; hafta başı olarak ilk kapanış alındı.")

    close = {}
    for s in syms:
        ser = px.get(s + ".IS")
        if ser is not None:
            close[s] = ser.reindex(sorted(set(cal) | set(ser.index))).ffill().reindex(cal)

    def hold(day):
        return dfr.holdings_at(ledger, day.strftime("%Y-%m-%d"), base)

    total_pl, per, missing, events = 0.0, {}, set(), []
    for i in range(1, len(cal)):
        p, d = cal[i - 1], cal[i]
        H = hold(p)
        Hn = hold(d)
        if Hn != H:
            for line in dfr.diff_text(H, Hn):
                events.append(f"{d:%d.%m}: {line}")
        for s, q in H.items():
            c = close.get(s)
            if c is None or pd.isna(c.get(p)) or pd.isna(c.get(d)):
                missing.add(s)
                continue
            pl = q * (float(c[d]) - float(c[p]))
            e = per.setdefault(s, {"pl": 0.0, "entry": float(c[p]), "last": float(c[d]), "first": d, "added": p, "lastday": d,
                                   "qtys": set()})
            e["pl"] += pl
            e["last"] = float(c[d])
            e["lastday"] = d
            e["qtys"].add(q)
            total_pl += pl

    def value(day):
        v = 0.0
        for s, q in hold(day).items():
            c = close.get(s)
            if c is not None and not pd.isna(c.get(day)):
                v += q * float(c[day])
        return v

    v0, v1 = value(cal[0]), value(cal[-1])
    flow = v1 - v0 - total_pl   # hafta içinde eklenen (+) / çıkarılan (−) para, yaklaşık
    base_cap = v0 + max(flow, 0)
    pct = total_pl / base_cap * 100 if base_cap > 0 else None
    icon = "🟢" if total_pl >= 0 else "🔴"

    head = f"📊 Haftalık portföy raporu\n{monday:%d.%m} – {cal[-1]:%d.%m.%Y}\n"
    L = [f"{icon} Hafta başı değer: {v0:,.0f} TL", f"{icon} Hafta sonu değer: {v1:,.0f} TL",
         f"{icon} Haftalık kâr/zarar: {total_pl:+,.0f} TL" + (f" (%{pct:+.1f})" if pct is not None else "")]
    if abs(flow) >= 1:
        L.append(f"Hafta içi alım/satım etkisi: {flow:+,.0f} TL (bu tutar kâr/zarara dahil değildir)")
    # maliyete göre durum (PORTFOY'daki maliyetler, hafta sonunda hâlâ elinde olan hisseler)
    costs = {h["sym"]: h["cost"] for h in pf if h["cost"]}
    held_end = hold(cal[-1])
    cb = cv = 0.0
    for sy, q in held_end.items():
        c_ = close.get(sy)
        if sy in costs and c_ is not None and not pd.isna(c_.get(cal[-1])):
            cb += costs[sy] * q
            cv += q * float(c_[cal[-1]])
    if cb > 0:
        L.append(f"Maliyetine göre toplam: {cv - cb:+,.0f} TL (%{(cv / cb - 1) * 100:+.1f}) | "
                 f"maliyet {cb:,.0f} TL → şimdi {cv:,.0f} TL")
        if any(sy not in costs for sy in held_end):
            L.append("(Maliyeti yazılmayan hisseler bu toplama dahil değil.)")
    if idxs is not None and len(cal) > 1:
        ip = (float(idxs[cal[-1]]) / float(idxs[cal[0]]) - 1) * 100
        if pct is not None:
            diff = pct - ip
            verdict = "endeksten iyi" if diff > 0.5 else "endeksten kötü" if diff < -0.5 else "endeksle aynı"
            L.append(f"BIST 100 bu hafta: %{ip:+.1f} → portföy {verdict} ({diff:+.1f} puan)")
    if events:
        L.append("\nHafta içi portföy değişiklikleri:")
        L += events
    L.append("\nHisse hisse:")
    for s, e in sorted(per.items(), key=lambda kv: -kv[1]["pl"]):
        p_ = (e["last"] / e["entry"] - 1) * 100 if e["entry"] else 0.0
        mark = "🟢" if e["pl"] > 0.5 else "🔴" if e["pl"] < -0.5 else "⚪"
        note = []
        if e["first"] > cal[1]:
            note.append(f"{e['added']:%d.%m} kapanışında eklendi")
        if e["lastday"] < cal[-1]:
            note.append("çıkarıldı")
        if len(e["qtys"]) > 1:
            note.append("lot değişti")
        line = (f"{mark} {s}: {e['entry']:.2f} → {e['last']:.2f} TL (%{p_:+.1f}) | {e['pl']:+,.0f} TL"
                + (f" ({', '.join(note)})" if note else ""))
        if s in costs and s in held_end:
            line += f"\n   maliyetine göre: %{(e['last'] / costs[s] - 1) * 100:+.1f} (maliyet {costs[s]:.2f} TL)"
        elif s in held_end:
            line += "\n   maliyetine göre: maliyet yazılmamış"
        L.append(line)
    if len(per) >= 2:
        b, w = max(per.items(), key=lambda kv: kv[1]["pl"]), min(per.items(), key=lambda kv: kv[1]["pl"])
        t = f"\nEn çok kazandıran: {b[0]} ({b[1]['pl']:+,.0f} TL)"
        if w[1]["pl"] < 0:
            t += f" | En çok kaybettiren: {w[0]} ({w[1]['pl']:+,.0f} TL)"
        L.append(t)
    if missing:
        L.append(f"\nVeri alınamayan: {', '.join(sorted(missing))}")
    for w_ in warn:
        L.append(f"⚠️ {w_}")
    L.append("\nFiyatlar günlük kapanışlardır; gün içinde yaptığın alım/satım kapanış fiyatından sayılır, "
             "brokerindeki rakamdan küçük sapma olabilir. Bilgilendirme amaçlıdır, tavsiye değildir.")
    send(head + "\n" + "\n".join(L))


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("HATA:", repr(e))
        try:
            send(f"⚠️ Haftalık portföy raporu hata verdi: {type(e).__name__}: {e}")
        except Exception:
            pass
        raise
