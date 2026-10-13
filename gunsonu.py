"""Gün sonu raporu ve ders çıkarma.
1) Bugünkü uyarılar ne yaptı; tutmayanların olası nedenleri (veriden hesaplanır).
2) Haftalık listedeki hisseler bu hafta ne yaptı; düşenlerin olası nedenleri.
3) Yeterli kayıt birikince: hangi kalıp / koşul daha çok tutmuş-tutmamış, hangi filtreyi sıkılaştırmak mantıklı.
4) İsteğe bağlı: ANTHROPIC_API_KEY secret'ı varsa kısa yapay zeka yorumu.
Hiçbir ayarı kendiliğinden değiştirmez, sadece öneri yazar. Kullanım: python gunsonu.py"""
import csv
import os
import sys

import pandas as pd
import requests
import yfinance as yf

from sinyal import now_tr, send

ALERT_FILE = "alerts.csv"
WEEK_FILE = "weekly.csv"
MIN_SAMPLES = 20          # istatistik yorumu için en az uyarı
MIN_BUCKET = 6            # bir koşulun yorumlanması için en az örnek
GAP_POINTS = 15           # genel başarıdan en az bu kadar puan kötü olan koşul işaretlenir
PATTERN_NAMES = {"ivme": "Hızlı yükseliş", "kirilim": "Dar aralık kırılımı",
                 "geri_donus": "Düşüşten dönüş", "tahta": "Alıcı baskısı"}


def fl(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def read_csv(path):
    if not os.path.exists(path):
        return []
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def result(r):
    """'win', 'loss' ya da 'flat'."""
    o = r.get("outcome") or ""
    end = fl(r.get("ret_end"))
    if o == "hedef":
        return "win"
    if o == "stop":
        return "loss"
    if end is None:
        return "flat"
    if end >= 0.5:
        return "win"
    if end <= -1.0:
        return "loss"
    return "flat"


# ---------- bugünkü uyarılar ----------
def alert_reasons(r, idx_day):
    mfe, mae = fl(r.get("mfe")), fl(r.get("mae"))
    spread, burst, day = fl(r.get("spread")), fl(r.get("burst")), fl(r.get("day"))
    index = fl(r.get("index"))
    bs, asz = fl(r.get("bid_size")), fl(r.get("ask_size"))
    hour = int(r["time"][:2]) if r.get("time") else None
    why = []
    if mfe is not None and mfe < 0.5:
        why.append(f"Alımdan sonra neredeyse hiç yükselmedi (en yüksek %{mfe:+.1f}); yanlış alarm gibi görünüyor.")
    elif mfe is not None and mfe >= 2:
        why.append(f"Önce %{mfe:+.1f} yükseldi, sonra verdi; çıkış uyarısı ya da kâr alma zamanlaması önemliydi.")
    if spread is not None and spread >= 0.6:
        why.append(f"Alış-satış farkı yüksekti (%{spread:.1f}); girişte maliyet büyüktü.")
    if burst is not None and burst < 4:
        why.append(f"Hacim artışı sınırdaydı (normalin {burst:.1f} katı).")
    if day is not None and day >= 5:
        why.append(f"Sinyal anında hisse zaten %{day:+.1f} yükselmişti; geç kalınmış olabilir.")
    if index is not None and index < 0:
        why.append(f"Sinyal anında BIST 100 düşüşteydi (%{index:+.1f}).")
    elif idx_day is not None and idx_day <= -1.0:
        why.append(f"Bugün BIST 100 genel olarak sert düştü (%{idx_day:+.1f}).")
    if hour is not None and hour >= 16:
        why.append("Sinyal kapanışa yakın geldi, düşüş toparlanacak zaman bulamamış olabilir.")
    if asz is not None and bs is not None and bs > 0 and asz > 2 * bs:
        why.append("Sinyalde satış tarafındaki emirler alış tarafının 2 katından fazlaydı (satış baskısı).")
    if not why:
        why.append("Veriden belirgin bir neden çıkmadı.")
    return why


# ---------- haftalık liste ----------
def daily_frames(tickers):
    out = {}
    if not tickers:
        return out
    try:
        data = yf.download([t + ".IS" for t in tickers], period="3mo", interval="1d", group_by="ticker",
                           auto_adjust=True, progress=False, threads=True)
        for t in tickers:
            try:
                try:
                    df = data[t + ".IS"]
                except Exception:
                    df = data
                df = df.dropna(subset=["Close"]).copy()
                idx = pd.to_datetime(df.index)
                df.index = (idx.tz_localize(None) if idx.tz is not None else idx).normalize()
                if len(df):
                    out[t] = df
            except Exception:
                continue
    except Exception as e:
        print("günlük veri alınamadı:", repr(e))
    return out


def index_frame():
    try:
        raw = yf.download("XU100.IS", period="3mo", interval="1d", auto_adjust=True, progress=False)
        if raw is None or raw.empty:
            return None
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = raw.columns.get_level_values(0)
        raw = raw.dropna(subset=["Close"]).copy()
        idx = pd.to_datetime(raw.index)
        raw.index = (idx.tz_localize(None) if idx.tz is not None else idx).normalize()
        return raw
    except Exception as e:
        print("endeks alınamadı:", repr(e))
        return None


def pick_reasons(df, ref, start, idx_df):
    why = []
    w = df[df.index >= start]
    if w.empty:
        return why
    last = float(w["Close"].iloc[-1])
    ret = (last / ref - 1) * 100
    if idx_df is not None:
        iw = idx_df[idx_df.index >= start]
        ib = idx_df[idx_df.index < start]
        if len(iw) and len(ib):
            ir = (float(iw["Close"].iloc[-1]) / float(ib["Close"].iloc[-1]) - 1) * 100
            if ir <= -1.0 and ret < ir + 1:
                why.append(f"BIST 100 aynı dönemde %{ir:+.1f} düştü; hisse piyasayla birlikte geriledi.")
            elif ret < ir - 2:
                why.append(f"Hisse endeksten zayıf kaldı (hisse %{ret:+.1f}, endeks %{ir:+.1f}); hisseye özgü bir neden olabilir.")
    e20 = df["Close"].ewm(span=20, adjust=False).mean()
    if last < float(e20.iloc[-1]):
        why.append("Fiyat 20 günlük ortalamanın altına indi, kısa vadeli trend bozuldu.")
    hi_wk = float(w["High"].max())
    if (hi_wk / ref - 1) * 100 >= 3 and ret <= 0:
        why.append(f"Hafta içinde %{(hi_wk / ref - 1) * 100:+.1f}'e çıktı, sonra verdi (kâr satışı).")
    avg_v = float(df["Volume"].iloc[-25:-5].mean()) if len(df) > 25 else None
    down = w[w["Close"] < w["Open"]]
    if avg_v and len(down) and float(down["Volume"].max()) > 1.8 * avg_v:
        why.append("Düşüş günlerinden birinde hacim normalin 1,8 katından fazlaydı (satış baskısı).")
    gap = [(float(w["Open"].iloc[i]) / float(w["Close"].iloc[i - 1]) - 1) * 100 for i in range(1, len(w))]
    if gap and min(gap) <= -2:
        why.append(f"Bir gün %{min(gap):+.1f} aşağıdan açıldı (gece haberi ya da genel piyasa sebebi olabilir).")
    if not why:
        why.append("Veriden belirgin bir neden çıkmadı.")
    return why


# ---------- ders çıkarma ----------
def lessons(rows):
    done = [r for r in rows if result(r) in ("win", "loss")]
    if len(rows) < MIN_SAMPLES or len(done) < MIN_SAMPLES // 2:
        return [f"Ders çıkarmak için yeterli kayıt yok ({len(rows)} uyarı, en az {MIN_SAMPLES} gerekiyor). "
                "Şimdilik sadece gözlem yapıyorum."]
    base = sum(1 for r in done if result(r) == "win") / len(done) * 100
    out = [f"Genel: {len(done)} sonuçlanmış uyarının %{base:.0f}'i tuttu."]

    def rate(sub):
        d = [r for r in sub if result(r) in ("win", "loss")]
        if len(d) < MIN_BUCKET:
            return None
        return sum(1 for r in d if result(r) == "win") / len(d) * 100, len(d)

    buckets = []
    for key, name in PATTERN_NAMES.items():
        buckets.append((f"Kalıp: {name}", [r for r in done if (r.get("pattern") or "ivme") == key]))
    for label, fn in [
        ("Alış-satış farkı %0,6 ve üstü", lambda r: (fl(r.get("spread")) or 0) >= 0.6),
        ("Hacim artışı 4 katın altı", lambda r: fl(r.get("burst")) is not None and fl(r.get("burst")) < 4),
        ("Sinyalde gün içi %5 ve üstü yükselmiş", lambda r: (fl(r.get("day")) or 0) >= 5),
        ("Sinyalde BIST 100 düşüşte", lambda r: fl(r.get("index")) is not None and fl(r.get("index")) < 0),
        ("Sinyal saat 15:30'dan sonra", lambda r: (r.get("time") or "00")[:5] >= "15:30"),
        ("Sinyal saat 10:15-11:30 arası", lambda r: "10:15" <= (r.get("time") or "")[:5] <= "11:30"),
    ]:
        buckets.append((label, [r for r in done if fn(r)]))
    found = False
    for name, sub in buckets:
        rr = rate(sub)
        if rr is None:
            continue
        pr, n = rr
        if pr <= base - GAP_POINTS:
            out.append(f"⚠️ {name}: tutma oranı %{pr:.0f} ({n} uyarı), genelden belirgin kötü. Bu koşulu sıkılaştırmayı düşünebiliriz.")
            found = True
        elif pr >= base + GAP_POINTS:
            out.append(f"✅ {name}: tutma oranı %{pr:.0f} ({n} uyarı), genelden iyi.")
            found = True
    if not found:
        out.append("Hiçbir koşul genelden belirgin farklı değil; şimdilik filtreleri değiştirmek için neden yok.")
    out.append("Bunlar öneri. Örnek sayısı azken rastlantı olabilir, ayarları kendim değiştirmiyorum.")
    return out


# ---------- isteğe bağlı yapay zeka yorumu ----------
def ai_comment(text):
    key = os.environ.get("ANTHROPIC_API_KEY")
    if not key:
        return None
    try:
        r = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={"x-api-key": key, "anthropic-version": "2023-06-01", "content-type": "application/json"},
            json={"model": os.environ.get("GUNSONU_MODEL", "claude-sonnet-5-5"), "max_tokens": 700,
                  "messages": [{"role": "user", "content":
                                "Bir BIST hisse sinyal sisteminin gün sonu raporunu inceliyorsun. Aşağıdaki veriye göre, "
                                "tutmayan sinyaller için en olası 2-3 ortak nedeni ve sistemin eşiklerinde yapılabilecek "
                                "1-2 somut iyileştirmeyi kısa Türkçe maddelerle yaz. Haber/KAP kontrol edilmedi, nedenin "
                                "haber olabileceğini belirt. Kesin konuşma, yatırım tavsiyesi verme.\n\n" + text}]},
            timeout=60)
        r.raise_for_status()
        return "".join(b.get("text", "") for b in r.json().get("content", []) if b.get("type") == "text").strip()
    except Exception as e:
        print("yapay zeka yorumu alınamadı:", repr(e))
        return None


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
    todays_str = str(today)
    rows_all = read_csv(ALERT_FILE)
    rows = [r for r in rows_all if r.get("date") == todays_str]
    idxf = index_frame()
    idx_day = None
    if idxf is not None and len(idxf) > 1 and idxf.index[-1].date() == today:
        idx_day = (float(idxf["Close"].iloc[-1]) / float(idxf["Close"].iloc[-2]) - 1) * 100

    parts = [f"🧾 Gün sonu raporu ({today:%d.%m.%Y})"]
    if idx_day is not None:
        parts.append(f"BIST 100 bugün: %{idx_day:+.1f}")

    # 1) bugünkü uyarılar
    summary_for_ai = []
    if not rows:
        parts.append("\nBugün kayıtlı yükseliş uyarısı yok.")
    else:
        w = sum(1 for r in rows if result(r) == "win")
        l = sum(1 for r in rows if result(r) == "loss")
        parts.append(f"\nBugünkü uyarılar: {len(rows)} | tutan {w} | tutmayan {l} | belirsiz {len(rows) - w - l}")
        for r in rows:
            res = result(r)
            icon = {"win": "✅", "loss": "❌", "flat": "➖"}[res]
            name = PATTERN_NAMES.get(r.get("pattern") or "ivme", r.get("pattern"))
            line = (f"{icon} {r['sym']} [{name}] {r['time'][:5]}: seans sonu %{r.get('ret_end')} | "
                    f"en yüksek %{r.get('mfe')} | en düşük %{r.get('mae')}")
            parts.append("\n" + line)
            if res == "loss":
                why = alert_reasons(r, idx_day)
                parts.append("Olası nedenler:\n- " + "\n- ".join(why))
                summary_for_ai.append(line + " | nedenler: " + " ".join(why))
        if any(result(r) == "loss" for r in rows):
            parts.append("\nNot: Haber/KAP açıklaması bu raporda kontrol edilmedi, düşüşün asıl sebebi haber olabilir.")

    # 2) haftalık liste
    wk = read_csv(WEEK_FILE)
    weeks = sorted({r["week"] for r in wk if r.get("sym")})
    if weeks:
        cur = weeks[-1]
        start = pd.Timestamp(cur)
        if (pd.Timestamp(today) - start).days <= 6:
            picks = [r for r in wk if r["week"] == cur and r.get("sym")]
            frames = daily_frames([p["sym"] for p in picks])
            lines = []
            for p in picks:
                df = frames.get(p["sym"])
                if df is None:
                    continue
                w = df[df.index >= start]
                if w.empty:
                    continue
                ref = float(p["ref_close"])
                ret = (float(w["Close"].iloc[-1]) / ref - 1) * 100
                icon = "✅" if ret >= 1 else ("❌" if ret <= -2 else "➖")
                lines.append(f"{icon} {p['sym']}: hafta başından %{ret:+.1f}")
                if ret <= -2:
                    why = pick_reasons(df, ref, start, idxf)
                    lines.append("Olası nedenler:\n- " + "\n- ".join(why))
                    summary_for_ai.append(f"Haftalık liste {p['sym']} %{ret:+.1f}: " + " ".join(why))
            if lines:
                parts.append(f"\n📅 Haftalık liste ({cur} haftası), şu ana kadar:\n" + "\n".join(lines))

    # 3) dersler
    parts.append("\n🎓 Öğrenilenler:\n" + "\n".join(lessons(rows_all)))

    # 4) yapay zeka yorumu
    if summary_for_ai:
        c = ai_comment("\n".join(summary_for_ai))
        if c:
            parts.append("\n🤖 Yapay zeka yorumu:\n" + c)
    parts.append("\nBu rapor bilgilendirmedir, yatırım tavsiyesi değildir.")
    send_long("\n".join(parts))


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("HATA:", repr(e))
        try:
            send(f"⚠️ Gün sonu raporu hata verdi: {type(e).__name__}: {e}")
        except Exception:
            pass
        raise
