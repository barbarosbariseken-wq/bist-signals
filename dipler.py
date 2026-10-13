"""Gün sonu 'dipte ve dönme ihtimali olan' hisseler.
200 hissenin her birini kendi 1 yıllık geçmişiyle karşılaştırır:
  - UCUZLUK: 52 haftalık aralığın altında mı, zirveden ne kadar düşmüş, ortalamalarının ne kadar altında, RSI düşük mü
  - DÖNÜŞ İŞARETİ: dipten toparlanma başlamış mı (yükselen dip, RSI dönüşü, hacimli yeşil günler),
    hâlâ yeni dip yapıyorsa (düşen bıçak) puan kırılır
En yüksek puanlı ilk 25'i Telegram'a gönderir ve dip_log.csv'ye yazar; eski listelerin sonradan ne yaptığını da raporlar.
Bu bir tahmin değil, puanlama. Kullanım: python dipler.py"""
import csv
import os

import pandas as pd

from sinyal import MIN_AVG_TL_VOLUME, SYMBOLS, atr, download, now_tr, rsi, send

LOG_FILE = "dip_log.csv"
LOG_FIELDS = ["date", "sym", "close", "score"]
TOP_N = 25
MIN_SCORE = 40
MIN_CHEAP = 24      # 'dipte' sayılmak için ucuzluk puanı en az bu kadar olmalı (dönüş tek başına yetmez)


def clean(df):
    df = df.dropna(subset=["Close"]).copy()
    idx = pd.to_datetime(df.index)
    df.index = (idx.tz_localize(None) if idx.tz is not None else idx).normalize()
    return df


def score(df):
    """(puan, bilgi, nedenler) ya da None."""
    if len(df) < 120:
        return None
    c, o, h, l, v = df["Close"], df["Open"], df["High"], df["Low"], df["Volume"]
    if (c * v).iloc[-20:].mean() < MIN_AVG_TL_VOLUME:
        return None
    last = float(c.iloc[-1])
    lo52, hi52 = float(l.iloc[-250:].min()), float(h.iloc[-250:].max())
    if hi52 <= lo52:
        return None
    pos = (last - lo52) / (hi52 - lo52) * 100
    dd = (1 - last / hi52) * 100
    e50 = float(c.ewm(span=50, adjust=False).mean().iloc[-1])
    below50 = (1 - last / e50) * 100
    r = rsi(c)
    r_now, r_3 = float(r.iloc[-1]), float(r.iloc[-4])

    cheap, why = 0, []
    if pos <= 10:
        cheap += 20
        why.append(f"52 hafta aralığının alt %{pos:.0f}'inde")
    elif pos <= 20:
        cheap += 14
        why.append(f"52 hafta aralığının alt %{pos:.0f}'inde")
    elif pos <= 30:
        cheap += 8
    if dd >= 50:
        cheap += 10
        why.append(f"zirveden %{dd:.0f} aşağıda")
    elif dd >= 35:
        cheap += 7
        why.append(f"zirveden %{dd:.0f} aşağıda")
    elif dd >= 20:
        cheap += 4
    if below50 >= 15:
        cheap += 10
        why.append("50 günlük ortalamanın çok altında")
    elif below50 >= 8:
        cheap += 6
    elif below50 >= 3:
        cheap += 3
    if r_now <= 30:
        cheap += 10
        why.append(f"RSI {r_now:.0f} (aşırı satım)")
    elif r_now <= 40:
        cheap += 6
        why.append(f"RSI {r_now:.0f}")

    rev = 0
    low10 = float(l.iloc[-10:].min())
    bounce = (last / low10 - 1) * 100
    if 2 <= bounce <= 15:
        rev += 8
        why.append(f"son 10 günün dibinden %{bounce:.0f} toparladı")
    if last > float(c.iloc[-2]) > float(c.iloc[-3]):
        rev += 5
        why.append("son 2 gün üst üste yükseldi")
    if r_now < 50 and r_now - r_3 >= 4:
        rev += 10
        why.append("RSI aşağıdan yukarı dönüyor")
    if float(l.iloc[-5:].min()) > float(l.iloc[-15:-5].min()):
        rev += 10
        why.append("yükselen dip oluşuyor")
    avg20 = float(v.iloc[-20:].mean())
    up3 = [(float(c.iloc[i]) > float(o.iloc[i])) and float(v.iloc[i]) >= 1.3 * avg20 for i in (-1, -2, -3)]
    if any(up3):
        rev += 10
        why.append("son 3 günde hacimli yeşil gün")
    if last > float(c.iloc[-6]):
        rev += 7
    rng = float(h.iloc[-1] - l.iloc[-1])
    if float(c.iloc[-1]) > float(o.iloc[-1]) and rng > 0 and (float(c.iloc[-1]) - float(l.iloc[-1])) / rng >= 0.6:
        rev += 5
    knife = float(c.iloc[-1]) <= float(c.iloc[-20:].min()) * 1.005
    if knife:
        rev -= 15
        why.append("DİKKAT: hâlâ yeni dip yapıyor (düşen bıçak)")
    if cheap < MIN_CHEAP:
        return None
    total = max(0, min(100, cheap + rev))
    info = {"close": last, "pos": pos, "dd": dd, "rsi": r_now, "rsi_up": r_now - r_3, "lo52": lo52,
            "hi52": hi52, "cheap": cheap, "rev": max(rev, 0), "knife": knife,
            "atr_pct": float(atr(df).iloc[-1]) / last * 100}
    return total, info, why


def read_log():
    if not os.path.exists(LOG_FILE):
        return []
    with open(LOG_FILE, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_log(rows_all, today_rows, today):
    rows = [r for r in rows_all if r["date"] != str(today)] + today_rows
    with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=LOG_FIELDS)
        w.writeheader()
        w.writerows(rows)


def past_performance(log, frames, today):
    """Yaklaşık 5-12 gün önceki en son listenin ortalama getirisi."""
    dates = sorted({r["date"] for r in log})
    cands = [d for d in dates if 5 <= (today - pd.Timestamp(d).date()).days <= 12]
    if not cands:
        return None
    d = cands[-1]
    rets = []
    for r in log:
        if r["date"] != d:
            continue
        df = frames.get(r["sym"])
        try:
            rets.append((float(df["Close"].iloc[-1]) / float(r["close"]) - 1) * 100)
        except Exception:
            continue
    if len(rets) < 5:
        return None
    pos = sum(1 for x in rets if x > 0)
    return (f"Geçmiş liste ({d}): {len(rets)} hissenin ortalaması %{sum(rets) / len(rets):+.1f}, "
            f"yükselen {pos}/{len(rets)}")


def send_long(text):
    while text:
        if len(text) <= 3900:
            send(text)
            return
        cut = text.rfind("\n\n", 0, 3900)
        cut = cut if cut > 500 else 3900
        send(text[:cut])
        text = text[cut:].lstrip()


def run():
    today = now_tr().date()
    data = download("1y", "1d")
    if data is None:
        raise RuntimeError("Günlük veri indirilemedi")
    frames, scored = {}, []
    for s in SYMBOLS:
        try:
            df = clean(data[s])
        except Exception:
            continue
        t = s[:-3]
        if len(df):
            frames[t] = df
        try:
            res = score(df)
        except Exception as e:
            print("puanlama hatası", t, repr(e))
            continue
        if res:
            scored.append((res[0], t, res[1], res[2]))
    scored.sort(key=lambda x: -x[0])
    top = [x for x in scored if x[0] >= MIN_SCORE][:TOP_N]

    head = f"🔎 Dipte ve dönme ihtimali olan hisseler ({today:%d.%m.%Y})\nİlk {len(top)} (puan 100 üzerinden, ucuzluk + dönüş işareti)"
    if not top:
        send(head + "\n\nBugün yeterli puan alan hisse yok.")
        return
    lines = []
    for i, (sc, t, inf, why) in enumerate(top, 1):
        knife = " ⚠️düşen bıçak" if inf["knife"] else ""
        lines.append(
            f"{i}) {t}: {inf['close']:.2f} TL | puan {sc:.0f}{knife}\n"
            f"   52 hf dip {inf['lo52']:.2f} / zirve {inf['hi52']:.2f} | zirveden %{-inf['dd']:.0f} | RSI {inf['rsi']:.0f}\n"
            f"   " + "; ".join(why[:4])
        )
    log = read_log()
    perf = past_performance(log, frames, today)
    write_log(log, [{"date": str(today), "sym": t, "close": f"{inf['close']:.4f}", "score": f"{sc:.0f}"}
                    for sc, t, inf, _ in top], today)
    foot = ["", "Nasıl okunur: ucuz olmak yükselecek demek değil. Dönüş işareti olmayan (⚠️düşen bıçak) hisseler "
                "daha da düşebilir. Haber/bilanço kontrol edilmedi.", "Bu bir bilgilendirmedir, yatırım tavsiyesi değildir."]
    if perf:
        foot.insert(0, perf)
    send_long(head + "\n\n" + "\n\n".join(lines) + "\n" + "\n".join(foot))


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("HATA:", repr(e))
        try:
            send(f"⚠️ Dip taraması hata verdi: {type(e).__name__}: {e}")
        except Exception:
            pass
        raise
