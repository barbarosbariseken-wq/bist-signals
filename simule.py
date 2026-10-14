"""Kural deneme (geçmiş uyarıları farklı stop / hedef / bekleme süreleriyle yeniden oynatır).
izler.csv: canlı takipte her uyarının ve rastgele kontrol hisselerinin dakikalık fiyat izi (alt, üst, kapanış; baz puan).
Bu script şunları söyler:
 1) Uyarılar rastgele (kontrol) hisselerden daha mı iyi? Sadece beklemeyle, stop/hedef olmadan.
 2) Şu anki kural (stop %2, hedef %4, en çok 120 dk) uyarılarda ve kontrolde ne getirmiş?
 3) Başka kurallar uyarılarda daha iyi olur muydu? Seçim ilk yarıda yapılır, ikinci yarıda sınanır (aşırı uyum kontrolü).
Maliyet: kayıttaki alış-satış farkı + varsayılan komisyon (COMMISSION_BP). Stop'ta ek kayma varsayılır (SLIP_BP).
Dakikalık veri olduğundan gerçek işlem daha kötü çıkabilir (dakika içi sıra bilinmez: stop ve hedef aynı dakikadaysa stop sayılır)."""
import csv
import math
import os
import sys

from sinyal import now_tr, send

TRACE_FILE = "izler.csv"
COMMISSION_BP = 20       # alış+satış komisyonu tahmini (%0,20), kendi komisyonuna göre değiştir
SLIP_BP = 20             # stop'ta ek kayma (%0,20)
MIN_ALERTS = 30          # kural karşılaştırması için en az uyarı
MIN_COMPARE = 10         # uyarı-kontrol kıyası için her gruptan en az kayıt
STOPS = (100, 150, 200, 300, 500, 800)
TARGETS = (200, 300, 400, 600, 1000)
HOLDS = (30, 60, 120)
CURRENT = (200, 400, 120)


def read_traces(path=TRACE_FILE):
    if not os.path.exists(path):
        return []
    out = []
    with open(path, encoding="utf-8") as f:
        for r in csv.DictReader(f):
            try:
                tr = [tuple(int(x) for x in m.split(",")) for m in (r.get("trace") or "").split(";") if m]
                if not tr:
                    continue
                out.append({"date": r["date"], "time": r["time"], "sym": r["sym"], "kind": r["kind"],
                            "spread_bp": float(r["spread"]) * 100 if r.get("spread") else 50.0, "trace": tr})
            except (ValueError, KeyError):
                continue
    out.sort(key=lambda x: (x["date"], x["time"]))
    return out


def sim(rec, stop_bp, target_bp, hold):
    """Tek işlemin net getirisi (baz puan). stop_bp/target_bp None ise o kural yok."""
    cost = rec["spread_bp"] + COMMISSION_BP
    tr = rec["trace"][:hold]
    for lo, hi, cl in tr:
        if stop_bp is not None and lo <= -stop_bp:
            return -stop_bp - SLIP_BP - cost
        if target_bp is not None and hi >= target_bp:
            return target_bp - cost
    return tr[-1][2] - cost


def stats(vals):
    n = len(vals)
    if n == 0:
        return None
    m = sum(vals) / n
    sd = math.sqrt(sum((v - m) ** 2 for v in vals) / (n - 1)) if n > 1 else 0.0
    return {"n": n, "mean": m, "se": sd / math.sqrt(n) if n > 1 else float("inf"),
            "win": sum(1 for v in vals if v > 0) / n * 100}


def fmt(s):
    if s is None:
        return "veri yok"
    return f"{s['n']} işlem | ortalama %{s['mean'] / 100:+.2f} | kazançlı olan %{s['win']:.0f}"


def compare_line(a, c):
    if a is None or c is None or a["n"] < MIN_COMPARE or c["n"] < MIN_COMPARE:
        return None
    diff = a["mean"] - c["mean"]
    se = math.sqrt(a["se"] ** 2 + c["se"] ** 2)
    z = diff / se if se and se != float("inf") else 0.0
    if abs(z) < 2:
        verdict = "fark rastlantı olabilir"
    elif z > 0:
        verdict = "uyarılar belirgin daha iyi"
    else:
        verdict = "uyarılar belirgin daha KÖTÜ"
    return f"fark %{diff / 100:+.2f} (hata payı ±%{se / 100:.2f}) → {verdict}"


def build_report(recs):
    alerts = [r for r in recs if r["kind"] in ("uyari", "gölge")]
    ctrl = [r for r in recs if r["kind"] == "kontrol"]
    L = [f"🧪 Kural deneme raporu ({now_tr():%d.%m.%Y})",
         f"Kayıt: {len(alerts)} uyarı (gölge dahil), {len(ctrl)} kontrol hissesi",
         f"Maliyet: alış-satış farkı + %{COMMISSION_BP / 100:.2f} komisyon varsayıldı"]
    if len(alerts) < MIN_COMPARE:
        L.append(f"\nHenüz yeterli kayıt yok (uyarı {len(alerts)}, en az {MIN_COMPARE}). Birkaç gün sonra tekrar bak.")
        return "\n".join(L)

    L.append("\n1) Sadece bekleme (stop/hedef yok), uyarı mı kontrol mü?")
    for h in HOLDS:
        a = stats([sim(r, None, None, h) for r in alerts])
        c = stats([sim(r, None, None, h) for r in ctrl])
        L.append(f"- {h} dk sonra çıkış → uyarılar: {fmt(a)}")
        L.append(f"  kontrol: {fmt(c)}")
        cl = compare_line(a, c)
        if cl:
            L.append(f"  {cl}")

    s, t, h = CURRENT
    a = stats([sim(r, s, t, h) for r in alerts])
    c = stats([sim(r, s, t, h) for r in ctrl])
    L.append(f"\n2) Şu anki kural (stop %{s / 100:g}, hedef %{t / 100:g}, en çok {h} dk)")
    L.append(f"- uyarılar: {fmt(a)}")
    L.append(f"- kontrol: {fmt(c)}")
    cl = compare_line(a, c)
    if cl:
        L.append(f"- {cl}")

    if len(alerts) < MIN_ALERTS:
        L.append(f"\n3) Başka kurallar: en az {MIN_ALERTS} uyarı gerekiyor (şimdi {len(alerts)}). Az veriyle en iyi kuralı "
                 "seçmek rastlantıya uyum olur, o yüzden henüz denemiyorum.")
    else:
        half = len(alerts) // 2
        first, second = alerts[:half], alerts[half:]
        grid = []
        for st in STOPS:
            for tg in TARGETS:
                for hd in HOLDS:
                    grid.append(((st, tg, hd), stats([sim(r, st, tg, hd) for r in first])["mean"]))
        grid.sort(key=lambda x: -x[1])
        L.append(f"\n3) Başka kurallar (seçim ilk {len(first)} uyarıda, sınama sonraki {len(second)} uyarıda)")
        for (st, tg, hd), m1 in grid[:5]:
            m2 = stats([sim(r, st, tg, hd) for r in second])
            cc = stats([sim(r, st, tg, hd) for r in ctrl])
            L.append(f"- stop %{st / 100:g} / hedef %{tg / 100:g} / {hd} dk: ilk yarı %{m1 / 100:+.2f} → "
                     f"ikinci yarı %{m2['mean'] / 100:+.2f} | kontrolde %{(cc['mean'] / 100):+.2f}" if cc else
                     f"- stop %{st / 100:g} / hedef %{tg / 100:g} / {hd} dk: ilk yarı %{m1 / 100:+.2f} → "
                     f"ikinci yarı %{m2['mean'] / 100:+.2f}")
        L.append("İkinci yarıda da pozitif kalan kural varsa güvenilir sayılır; düşüyorsa ilk yarıdaki başarı rastlantıydı.")
    L.append("\nBu bir geriye dönük deneme, gelecek sonucu garanti etmez. Yatırım tavsiyesi değildir.")
    return "\n".join(L)


def send_long(text):
    while text:
        if len(text) <= 3900:
            send(text)
            return
        cut = text.rfind("\n", 0, 3900)
        send(text[:cut if cut > 500 else 3900])
        text = text[cut if cut > 500 else 3900:].lstrip()


def run():
    send_long(build_report(read_traces()))


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("HATA:", repr(e))
        try:
            send(f"⚠️ Kural deneme raporu hata verdi: {type(e).__name__}: {e}")
        except Exception:
            pass
        raise
