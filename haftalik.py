"""Haftalık takip listesi.
Pazartesi sabahı: günlük verilere bakıp "bu hafta iyi gidebilir" diyebileceği hisseleri puanlar,
en iyi birkaçını Telegram'a gönderir ve weekly.csv'ye kaydeder.
Cuma akşamı: o listenin hafta boyunca ne yaptığını (karne) gönderir ve weekly_results.csv'ye ekler.
Kullanım: python haftalik.py [otomatik|liste|karne]"""
import csv
import os
import sys

import pandas as pd
import yfinance as yf

from sinyal import MIN_AVG_TL_VOLUME, SYMBOLS, atr, download, now_tr, rsi, send

LIST_FILE = "weekly.csv"
RESULT_FILE = "weekly_results.csv"
LIST_FIELDS = ["week", "sym", "ref_close", "stop", "target", "score", "reasons"]
RESULT_FIELDS = ["week", "sym", "ret_ref", "ret_open", "max_up", "max_down", "outcome"]
MAX_PICKS = 5
MIN_SCORE = 60
STOP_ATR = 2.0
TARGET_ATR = 3.0
INDEX_SYMBOL = "XU100.IS"


# ---------- veri ----------
def clean(df):
    df = df.dropna(subset=["Close"]).copy()
    idx = pd.to_datetime(df.index)
    if idx.tz is not None:
        idx = idx.tz_localize(None)
    df.index = idx.normalize()
    return df


def load(period):
    """(hisse tabloları, endeks tablosu veya None)."""
    data = download(period, "1d")
    if data is None:
        raise RuntimeError("Günlük veri indirilemedi")
    frames = {}
    for s in SYMBOLS:
        try:
            df = clean(data[s])
            if len(df):
                frames[s[:-3]] = df
        except Exception:
            continue
    idx = None
    try:
        raw = yf.download(INDEX_SYMBOL, period=period, interval="1d", auto_adjust=True, progress=False)
        if raw is not None and not raw.empty:
            if isinstance(raw.columns, pd.MultiIndex):
                raw.columns = raw.columns.get_level_values(0)
            idx = clean(raw)
    except Exception as e:
        print("endeks indirilemedi:", repr(e))
    return frames, idx


def week_start(today):
    return today - pd.Timedelta(days=today.weekday())


# ---------- puanlama ----------
def score_stock(df, idx_ret20):
    """(puan, nedenler, bilgi) döner; likit değilse veya veri azsa None."""
    if len(df) < 60:
        return None
    close, vol = df["Close"], df["Volume"]
    if (close * vol).iloc[-20:].mean() < MIN_AVG_TL_VOLUME:
        return None
    last = float(close.iloc[-1])
    e20 = close.ewm(span=20, adjust=False).mean()
    e50 = close.ewm(span=50, adjust=False).mean()
    r14 = float(rsi(close).iloc[-1])
    r5 = (last / float(close.iloc[-6]) - 1) * 100
    r20 = (last / float(close.iloc[-21]) - 1) * 100
    v5, v20 = float(vol.iloc[-5:].mean()), float(vol.iloc[-20:].mean())
    vr = v5 / v20 if v20 else 0
    hi20 = float(df["High"].iloc[-20:].max())
    a = float(atr(df).iloc[-1])
    day1 = (last / float(close.iloc[-2]) - 1) * 100

    score, why = 0, []
    if last > e20.iloc[-1]:
        score += 10
        why.append("fiyat 20 günlük ortalamanın üstünde")
    if e20.iloc[-1] > e50.iloc[-1]:
        score += 10
        why.append("kısa vadeli ortalama uzun vadelinin üstünde (yükseliş trendi)")
    if e50.iloc[-1] > e50.iloc[-6]:
        score += 5
    if r20 >= 3:
        score += 10
        why.append(f"son 20 günde %{r20:+.1f}")
    elif r20 > 0:
        score += 5
    if 0.5 <= r5 <= 12:
        score += 10
        why.append(f"son 5 günde %{r5:+.1f} (ivme var, aşırı değil)")
    if 50 <= r14 <= 68:
        score += 15
        why.append(f"RSI {r14:.0f} (güçlü ama şişmemiş)")
    elif 45 <= r14 < 50 or 68 < r14 <= 75:
        score += 7
    if vr >= 1.3:
        score += 15
        why.append(f"son 5 gün hacmi normalin {vr:.1f} katı")
    elif vr >= 1.1:
        score += 8
    if last >= 0.97 * hi20:
        score += 15
        why.append("20 günlük zirveye çok yakın")
    elif last >= 0.93 * hi20:
        score += 7
    if idx_ret20 is None:
        score += 5
    elif r20 - idx_ret20 > 0:
        score += 10
        why.append(f"endeksten güçlü (endeks son 20 gün %{idx_ret20:+.1f})")
    # cezalar
    if last > e20.iloc[-1] * 1.12:
        score -= 15
        why.append("DİKKAT: 20 günlük ortalamadan çok uzaklaşmış")
    if a / last * 100 > 7:
        score -= 10
        why.append("DİKKAT: çok oynak")
    if day1 > 8:
        score -= 10
        why.append("DİKKAT: son gün çok sert yükselmiş, kovalamak riskli")

    stop = last - STOP_ATR * a
    target = last + TARGET_ATR * a
    return max(score, 0), why, {"close": last, "stop": stop, "target": target}


# ---------- kayıt ----------
def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_replace_week(path, fields, week, new_rows):
    rows = [r for r in read_csv(path) if r.get("week") != week]
    rows.extend(new_rows)
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)


# ---------- pazartesi: liste ----------
def run_list():
    frames, idx = load("1y")
    today = pd.Timestamp(now_tr().date())
    idx_ret20, regime = None, "Endeks verisi alınamadı."
    if idx is not None:
        ic = idx[idx.index < today]["Close"]
        if len(ic) > 25:
            idx_ret20 = (float(ic.iloc[-1]) / float(ic.iloc[-21]) - 1) * 100
            if ic.iloc[-1] > ic.ewm(span=20, adjust=False).mean().iloc[-1]:
                regime = "BIST 100 genel olarak yükseliş eğiliminde."
            else:
                regime = "⚠️ BIST 100 20 günlük ortalamasının altında, piyasa zayıf. Bu hafta temkinli ol."

    scored = []
    for t, df in frames.items():
        df = df[df.index < today]  # bugünkü yarım günü kullanma
        try:
            res = score_stock(df, idx_ret20)
        except Exception as e:
            print("puanlama hatası", t, repr(e))
            continue
        if res:
            scored.append((res[0], t, res[1], res[2]))
    scored.sort(key=lambda x: -x[0])
    picks = [x for x in scored if x[0] >= MIN_SCORE][:MAX_PICKS]
    wk = str(week_start(today).date())

    head = f"📅 Haftalık takip listesi ({wk} haftası)\n{regime}\n"
    if not picks:
        near = "\n".join(f"{t}: {s}/100" for s, t, _, _ in scored[:3])
        send(head + "\nBu hafta güçlü aday çıkmadı (en az 60 puan gerekiyor). En yakın üçü:\n" + near
             + "\n\nBu hafta liste yok. Yatırım tavsiyesi değildir.")
        write_replace_week(LIST_FILE, LIST_FIELDS, wk, [])
        return

    blocks, rows = [], []
    for i, (s, t, why, info) in enumerate(picks, 1):
        c, st, tg = info["close"], info["stop"], info["target"]
        blocks.append(
            f"{i}) {t} - puan {s}/100\n"
            f"Son kapanış: {c:.2f} TL\n"
            f"Neden:\n- " + "\n- ".join(why) + "\n"
            f"Örnek zarar durdur: {st:.2f} TL (-%{(1 - st / c) * 100:.1f})\n"
            f"Örnek hedef: {tg:.2f} TL (+%{(tg / c - 1) * 100:.1f})"
        )
        rows.append({"week": wk, "sym": t, "ref_close": f"{c:.4f}", "stop": f"{st:.4f}",
                     "target": f"{tg:.4f}", "score": s, "reasons": "; ".join(why)})
    send(head + "\nBu hafta iyi gidebilecek adaylar:\n\n" + "\n\n".join(blocks)
         + "\n\nCuma 18:30'da bu listenin hafta boyunca ne yaptığı (karne) gelecek."
         "\nBu bir tahmin değil, geçmiş fiyat desenlerine göre puanlama. Yatırım tavsiyesi değildir.")
    write_replace_week(LIST_FILE, LIST_FIELDS, wk, rows)


# ---------- cuma: karne ----------
def run_report():
    lists = read_csv(LIST_FILE)
    weeks = sorted({r["week"] for r in lists if r.get("sym")})
    if not weeks:
        send("📊 Henüz kayıtlı haftalık liste yok.")
        return
    wk = weeks[-1]
    picks = [r for r in lists if r["week"] == wk and r.get("sym")]
    frames, idx = load("3mo")
    start = pd.Timestamp(wk)
    today = pd.Timestamp(now_tr().date())
    final = today.weekday() >= 4

    out, rets, res_rows = [], [], []
    for p in picks:
        t = p["sym"]
        df = frames.get(t)
        if df is None:
            continue
        w = df[(df.index >= start)]
        if w.empty:
            out.append(f"{t}: bu hafta veri yok")
            continue
        ref, stop, target = float(p["ref_close"]), float(p["stop"]), float(p["target"])
        op, last = float(w["Open"].iloc[0]), float(w["Close"].iloc[-1])
        up = (float(w["High"].max()) / ref - 1) * 100
        down = (float(w["Low"].min()) / ref - 1) * 100
        outcome = "açık"
        for _, d in w.iterrows():
            hit_s, hit_t = d["Low"] <= stop, d["High"] >= target
            if hit_s:  # aynı gün ikisi de olduysa temkinli say: önce stop
                outcome = "zarar durdurdu ❌"
                break
            if hit_t:
                outcome = "hedefe ulaştı ✅"
                break
        r_ref, r_open = (last / ref - 1) * 100, (last / op - 1) * 100
        rets.append(r_ref)
        out.append(f"{t}: cuma kapanışına göre %{r_ref:+.1f} | pazartesi açılışına göre %{r_open:+.1f}"
                   f" | hafta içi en yüksek %{up:+.1f}, en düşük %{down:+.1f} | {outcome}")
        res_rows.append({"week": wk, "sym": t, "ret_ref": f"{r_ref:.2f}", "ret_open": f"{r_open:.2f}",
                         "max_up": f"{up:.2f}", "max_down": f"{down:.2f}", "outcome": outcome})

    idx_txt = ""
    if idx is not None:
        before = idx[idx.index < start]["Close"]
        cur = idx[idx.index >= start]["Close"]
        if len(before) and len(cur):
            idx_txt = f"\nBIST 100 aynı dönemde: %{(float(cur.iloc[-1]) / float(before.iloc[-1]) - 1) * 100:+.1f}"

    title = "📊 Haftalık karne" if final else "📊 Hafta içi durum (hafta henüz bitmedi)"
    msg = [f"{title} ({wk} haftası listesi)", ""] + out
    if rets:
        pos = sum(1 for r in rets if r > 0)
        msg += ["", f"Ortalama: %{sum(rets) / len(rets):+.1f} | yükselen: {pos}/{len(rets)}" + idx_txt]
    if final and res_rows:
        write_replace_week(RESULT_FILE, RESULT_FIELDS, wk, res_rows)
        allr = read_csv(RESULT_FILE)
        per = {}
        for r in allr:
            per.setdefault(r["week"], []).append(float(r["ret_ref"]))
        if len(per) > 1:
            avgs = [sum(v) / len(v) for v in per.values()]
            msg += ["", f"Şimdiye kadar {len(per)} hafta: haftalık ortalama %{sum(avgs) / len(avgs):+.1f}, "
                        f"artıda biten hafta {sum(1 for a in avgs if a > 0)}/{len(avgs)}"]
        if len(per) < 8:
            msg += ["Not: 8 haftadan az veriyle sonuçlar rastlantı olabilir, para koymak için erken."]
    send("\n".join(msg))


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "otomatik"
    if mode == "otomatik":
        mode = "liste" if now_tr().weekday() == 0 else "karne"
    try:
        run_list() if mode == "liste" else run_report()
    except Exception as e:
        print("HATA:", repr(e))
        try:
            send(f"⚠️ Haftalık takip hata verdi: {type(e).__name__}: {e}")
        except Exception:
            pass
        raise


if __name__ == "__main__":
    main()
