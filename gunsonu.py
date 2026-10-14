"""Gün sonu raporu ve ders çıkarma.
1) Bugünkü uyarılar ne yaptı; tutmayanların olası nedenleri (veriden hesaplanır).
2) Haftalık listedeki hisseler bu hafta ne yaptı; düşenlerin olası nedenleri.
3) Yeterli kayıt birikince: hangi kalıp / koşul daha çok tutmuş-tutmamış, hangi filtreyi sıkılaştırmak mantıklı.
4) İsteğe bağlı: ANTHROPIC_API_KEY secret'ı varsa kısa yapay zeka yorumu.
5) Yeterli kayıt (en az 30 sonuçlanmış uyarı) birikince hatalı koşulları ogrenilen.json dosyasına yazar;
   canlı takip ertesi sabah bu filtreleri okur. Filtreler yalnızca SIKILAŞTIRIR ve sınırlıdır; filtrelenen uyarılar
   gönderilmez ama izlenir ("gölge kayıt"), böylece filtre yanlışsa ertesi gün veri bunu gösterir ve filtre kalkar.
Kullanım: python gunsonu.py"""
import csv
import json
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
LEARN_FILE = "ogrenilen.json"
MIN_LEARN = 30            # filtre yazmak için en az sonuçlanmış uyarı
LEARN_BUCKET = 8          # bir filtrenin gerekçesi için koşuldaki en az uyarı
PATTERN_NAMES = {"ivme": "Hızlı yükseliş", "kirilim": "Dar aralık kırılımı",
                 "geri_donus": "Düşüşten dönüş", "tahta": "Alıcı baskısı", "hacim_oncu": "Hacim öncüsü"}


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
    upshare, b2, dshare = fl(r.get("upshare")), fl(r.get("b2")), fl(r.get("day_share"))
    if upshare is not None and upshare < 0.55:
        why.append(f"İşlemlerin yalnızca %{upshare * 100:.0f}'i alıcı yönlüydü; hacim artışı alımdan çok satış kaynaklı olabilir.")
    if b2 is not None and burst is not None and b2 < burst * 0.7:
        why.append("Sinyalde hacim zaten yavaşlıyordu (son 2 dk, son 5 dk hızından düşük); hareket sönmeye başlamıştı.")
    if dshare is not None and dshare < 0.05:
        why.append("Bugünkü hacim normal günün %5'inden azdı; hisse henüz ilgi görmüyordu.")
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
        ("Alıcı yönlü işlem oranı %55'in altında", lambda r: fl(r.get("upshare")) is not None and fl(r.get("upshare")) < 0.55),
        ("Bugünkü hacim normal günün %10'undan az", lambda r: fl(r.get("day_share")) is not None and fl(r.get("day_share")) < 0.10),
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
    out.append("Bunlar öneri. Örnek sayısı azken rastlantı olabilir.")
    return out


# ---------- filtre öğrenme ----------
def wr(sub):
    d = [r for r in sub if result(r) in ("win", "loss")]
    if not d:
        return None, 0
    return sum(1 for r in d if result(r) == "win") / len(d) * 100, len(d)


def learn(rows_all):
    """Hatalı koşulları bulur: (filtre sözlüğü ya da None, açıklama satırları).
    Filtreler yalnızca sıkılaştırır. Gölge (gönderilmeyen) kayıtlar da sayılır, böylece filtre kendini doğrular."""
    done = [r for r in rows_all if result(r) in ("win", "loss")]
    if len(done) < MIN_LEARN:
        return None, [f"Filtre öğrenmek için yeterli sonuçlanmış uyarı yok ({len(done)}/{MIN_LEARN}). Mevcut filtreler aynen kalıyor."]
    base, _ = wr(done)
    notes = []

    def bad(sub):
        p, n = wr(sub)
        return p is not None and n >= LEARN_BUCKET and p <= base - GAP_POINTS, p, n

    out = {}
    closed = []
    for key, name in PATTERN_NAMES.items():
        sub = [r for r in done if (r.get("pattern") or "ivme") == key]
        ok, p, n = bad(sub)
        if ok and n >= 10 and p <= 25:
            closed.append(key)
            notes.append(f"{name} kalıbı kapatıldı: {n} uyarının %{p:.0f}'i tutmuş (genel %{base:.0f}). Yine de izlenmeye devam edecek.")
    if closed and len(closed) < len(PATTERN_NAMES):
        out["kapali_kaliplar"] = closed

    best = None
    for T in (4.0, 5.0, 6.0):          # en hafif sıkılaştırma yeterliyse onu seç
        ok, p, n = bad([r for r in done if fl(r.get("burst")) is not None and fl(r.get("burst")) < T])
        if ok:
            best = (T, p, n)
            break
    if best:
        out["min_burst"] = best[0]
        notes.append(f"Hacim artışı {best[0]:g} katın altındaki uyarıların %{best[1]:.0f}'i tutmuş ({best[2]} uyarı, genel %{base:.0f}): en az {best[0]:g} kat şartı.")

    best = None
    for S in (0.8, 0.6, 0.5):
        ok, p, n = bad([r for r in done if fl(r.get("spread")) is not None and fl(r.get("spread")) >= S])
        if ok:
            best = (S, p, n)
            break
    if best:
        out["max_spread"] = best[0]
        notes.append(f"Alış-satış farkı %{best[0]:g} ve üstü uyarıların %{best[1]:.0f}'i tutmuş ({best[2]} uyarı): fark en fazla %{best[0]:g} olsun.")

    best = None
    for U in (0.55, 0.60, 0.65):
        ok, p, n = bad([r for r in done if fl(r.get("upshare")) is not None and fl(r.get("upshare")) < U])
        if ok:
            best = (U, p, n)
            break
    if best:
        out["min_upshare"] = best[0]
        notes.append(f"Alıcı yönlü işlem oranı %{best[0] * 100:.0f}'in altındaki uyarıların %{best[1]:.0f}'i tutmuş ({best[2]} uyarı): en az %{best[0] * 100:.0f} şartı.")
    if not out:
        notes.append("Belirgin kötü bir koşul bulunamadı; ek filtre yok.")
    return out, notes


def filters_text(f):
    if not f:
        return "yok"
    parts = []
    if f.get("kapali_kaliplar"):
        parts.append("kapalı kalıp: " + ", ".join(PATTERN_NAMES.get(k, k) for k in f["kapali_kaliplar"]))
    if f.get("min_burst"):
        parts.append(f"hacim artışı en az {f['min_burst']:g} kat")
    if f.get("max_spread"):
        parts.append(f"alış-satış farkı en fazla %{f['max_spread']:g}")
    if f.get("min_upshare"):
        parts.append(f"alıcı yönlü oran en az %{f['min_upshare'] * 100:.0f}")
    return "; ".join(parts) if parts else "yok"


def read_learn():
    try:
        with open(LEARN_FILE, encoding="utf-8") as fh:
            d = json.load(fh)
        return {k: d[k] for k in ("kapali_kaliplar", "min_burst", "max_spread", "min_upshare") if d.get(k)}
    except (OSError, ValueError):
        return {}


def apply_learning(rows_all, today_str):
    """Filtreleri hesaplar, dosyaya yazar, rapor satırlarını döner."""
    old = read_learn()
    new, notes = learn(rows_all)
    lines = ["Filtre öğrenme:"] + notes
    if new is None:
        return lines
    try:
        with open(LEARN_FILE, "w", encoding="utf-8") as fh:
            json.dump({"updated": today_str, "n": len([r for r in rows_all if result(r) in ("win", "loss")]),
                       **new}, fh, ensure_ascii=False, indent=1)
    except OSError as e:
        lines.append(f"Filtre dosyası yazılamadı: {e!r}")
        return lines
    if new == old:
        lines.append(f"Yarın uygulanacak filtreler (değişmedi): {filters_text(new)}")
    else:
        lines.append(f"Dün: {filters_text(old)}")
        lines.append(f"Yarın uygulanacak filtreler: {filters_text(new)}")
    lines.append("Filtreler yalnızca sıkılaştırır. Filtrelenen uyarılar gönderilmez ama izlenir; yanlış çıkarsa kalkar.")
    return lines


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
    day_rows = [r for r in rows_all if r.get("date") == todays_str]
    rows = [r for r in day_rows if not r.get("shadow")]
    shadow_today = [r for r in day_rows if r.get("shadow")]
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

    if shadow_today:
        sw = sum(1 for r in shadow_today if result(r) == "win")
        sl = sum(1 for r in shadow_today if result(r) == "loss")
        parts.append(f"\n🚫 Filtrelerin engellediği (gönderilmeyen) uyarılar: {len(shadow_today)} | "
                     f"tutacak olan {sw} | tutmayacak olan {sl}")
        parts.append("(Filtre doğruysa tutmayacak olanlar çoğunlukta olmalı.)")

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
    try:
        parts.append("\n" + "\n".join(apply_learning(rows_all, todays_str)))
    except Exception as e:
        print("filtre öğrenme hatası:", repr(e))

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
