"""Portföy takibi: her çalışmada portföydeki hisselerin durumunu tek mesajda gönderir;
bozulma (🔻, 🛑) ya da yükseliş başlangıcı (🚀, 🎯) varsa ayrı ve anında bildirir.
Portföy PORTFOY ortam değişkeninden (GitHub secret) okunur. Biçim, hisseler ';' ile ayrılır:
  SEMBOL [maliyet] [adet] [stop_fiyati] [hedef_fiyati]     ('-' = boş bırak)
  örn:  ATAGY 8,20 1000 7,60 9,50; EUYO 4,30 500; SKYLP
Veri Yahoo'dan (15 dk'lık mumlar, ~15 dk gecikmeli); olmazsa TradingView tarayıcısından sadece fiyat."""
import os
import re
import sys

import pandas as pd
import yfinance as yf

from sinyal import TZ, now_tr, send

STOP_PCT = 5.0        # stop fiyatı verilmemişse: maliyetin bu kadar % altı
TARGET_PCT = 10.0     # hedef fiyatı verilmemişse: maliyetin bu kadar % üstü
DROP_HOUR = -2.0      # son 1 saatte bu kadar düşerse bozulma
PULLBACK = 3.0        # günün zirvesinden bu kadar geri çekilirse (gün içi en az +%2 yükselmişse)
DROP_DAY = -4.0       # günlük bu kadar düşerse
LIMIT_BREAK = 1.5     # tavan olup sonra zirveden bu kadar inerse "tavan bozuldu"
RISE_HOUR = 2.0       # son 1 saatte bu kadar yükselirse (hacim de artıyorsa) yükseliş başladı
RISE_BURST = 1.5
START_MIN = 10 * 60
END_MIN = 18 * 60 + 10


def num(x):
    x = str(x).strip()
    if x in ("", "-"):
        return None
    return float(x.replace(",", "."))


def parse_portfolio(text):
    out = []
    for part in re.split(r"[;\n]", text or ""):
        tok = [t for t in re.split(r"[\s:]+", part.strip()) if t]
        if not tok:
            continue
        tok += ["-"] * (5 - len(tok))
        out.append({"sym": tok[0].upper().replace(".IS", ""), "cost": num(tok[1]), "qty": num(tok[2]),
                    "stop": num(tok[3]), "target": num(tok[4])})
    return out


def load_bars(syms):
    data = yf.download([s + ".IS" for s in syms], period="5d", interval="15m", group_by="ticker",
                       auto_adjust=True, progress=False, threads=True)
    out = {}
    for s in syms:
        try:
            try:
                df = data[s + ".IS"]
            except Exception:
                df = data
            df = df.dropna(subset=["Close"]).copy()
            idx = pd.to_datetime(df.index)
            idx = idx.tz_localize(TZ) if idx.tz is None else idx.tz_convert(TZ)
            df.index = idx
            if len(df):
                out[s] = df
        except Exception:
            continue
    return out


def flags_at(df, today, pos):
    """df'nin pos'a kadar olan mumlarıyla durum: {bayrak: açıklama}, metrikler."""
    sl = df.iloc[: pos + 1]
    td = sl[sl.index.date == today]
    if td.empty:
        return {}, None
    before = sl[sl.index.date < today]
    prev_close = float(before["Close"].iloc[-1]) if len(before) else float(td["Open"].iloc[0])
    last = float(td["Close"].iloc[-1])
    high = float(td["High"].max())
    day = (last / prev_close - 1) * 100
    h1 = (last / float(td["Close"].iloc[-5]) - 1) * 100 if len(td) >= 5 else None
    m15 = (last / float(td["Close"].iloc[-2]) - 1) * 100 if len(td) >= 2 else None
    burst = None
    if len(td) >= 4 and len(sl) > 8:
        avg_bar = float(sl["Volume"].mean())
        if avg_bar > 0:
            burst = float(td["Volume"].iloc[-4:].sum()) / (avg_bar * 4)
    pullback = (high / last - 1) * 100
    day_high_gain = (high / prev_close - 1) * 100
    f = {}
    if h1 is not None and h1 <= DROP_HOUR:
        f["boz"] = f"son 1 saatte %{h1:+.1f} düştü"
    if pullback >= PULLBACK and day_high_gain >= 2.0:
        f["boz_zirve"] = f"günün zirvesinden (%{pullback:.1f}) geri çekildi (zirve {high:.2f})"
    if day_high_gain >= 9.5 and last <= high * (1 - LIMIT_BREAK / 100):
        f["tavan_boz"] = f"tavandan döndü (zirve {high:.2f}, şimdi {last:.2f})"
    if day <= DROP_DAY:
        f["boz_gun"] = f"bugün %{day:+.1f} düşüşte"
    if h1 is not None and h1 >= RISE_HOUR and (burst is None or burst >= RISE_BURST):
        f["yuk"] = f"son 1 saatte %{h1:+.1f} yükseldi" + (f", hacim normalin {burst:.1f} katı" if burst else "")
    if m15 is not None and last >= high * 0.9995 and m15 >= 1.0 and day >= 2.0:
        f["yuk_zirve"] = f"günün yeni zirvesinde (%{m15:+.1f} son 15 dk)"
    return f, {"last": last, "day": day, "m15": m15, "h1": h1, "high": high, "prev": prev_close,
               "time": td.index[-1]}


def level_flags(h, last):
    f = {}
    cost = h["cost"]
    stop = h["stop"] if h["stop"] else (cost * (1 - STOP_PCT / 100) if cost else None)
    target = h["target"] if h["target"] else (cost * (1 + TARGET_PCT / 100) if cost else None)
    if stop and last <= stop:
        f["stop"] = f"stop seviyesine ({stop:.2f}) geldi"
    if target and last >= target:
        f["hedef"] = f"hedef seviyesine ({target:.2f}) ulaştı"
    return f


ICON = {"boz": "🔻", "boz_zirve": "🔻", "tavan_boz": "🔻", "boz_gun": "🔻", "stop": "🛑",
        "yuk": "🚀", "yuk_zirve": "🚀", "hedef": "🎯"}
TITLE = {"🔻": "bozulma işareti", "🛑": "stop seviyesi", "🚀": "yükseliş başladı", "🎯": "hedef"}


MILESTONES = (500_000, 1_000_000, 1_500_000)


def tl(x):
    return f"{x:,.0f}".replace(",", ".") + " TL"


def milestone_text(m, value):
    """Her kilometre taşı için bir kez gönderilen kutlama mesajı."""
    if m == 500_000:
        return (f"🎉 YARIM MİLYON! 🎉\n\n"
                f"Portföyünün değeri şu an {tl(value)}.\n\n"
                "Bir zamanlar hayal olan sayı artık hesabında duruyor. İlk yarım milyon, en zor olanıdır; "
                "çünkü sonrasında para da senin için çalışmaya başlar.\n"
                "Ertelediğin hayallerin ilk durağına geldin. Kendine bir teşekkür et: bunu sabırla, plan yaparak, "
                "her hafta raporlara bakarak sen yaptın. 🌱\n\n"
                "Yola devam: sıradaki durak 1 milyon.")
    if m == 1_000_000:
        return (f"🥂 1.000.000 TL! 7 haneli kulübe hoş geldin 🥂\n\n"
                f"Portföyünün değeri şu an {tl(value)}.\n\n"
                "Bir milyon sadece bir sayı değil: sabahları kaygısız uyanabilmek, hayallerini takvime yazabilmek, "
                "'ileride' dediğin şeylerin artık 'yakında'ya dönmesi demek. ✨\n"
                "Bu noktaya gelmek disiplin ister; hem kazandığında hem düştüğünde sakin kalmayı bilmen buraya "
                "getirdi seni. Gurur duy.\n\n"
                "Bir sonraki durak: 1,5 milyon. Ve orada seni bir Scirocco bekliyor olabilir. 😉")
    return (f"🏎️💨 1.500.000 TL! SCIROCCO ZAMANI! 💨🏎️\n\n"
            f"Portföyünün değeri şu an {tl(value)}.\n\n"
            "Scirocco adını Sahra'dan Akdeniz'e esen sıcak rüzgârdan alır; hayalin de tam böyle, çöl sıcağında "
            "başlayıp ufka doğru esen bir rüzgâr gibiydi. Alçak, keskin çizgili o coupé'nin anahtarını eline "
            "aldığın, motoru ilk çalıştırdığın, direksiyonu kavrayıp yola çıktığın anı hayal et. 🔑\n"
            "Bu hayali yıllarca taşıdın. Şimdi artık bir 'keşke' değil, masadaki gerçek bir seçenek.\n\n"
            "Bir not: karar tamamen senin. Araç alırken vergi, sigorta ve bakım masraflarını ve portföyünün ne "
            "kadarını bozacağını bir kez daha hesaplamak isteyebilirsin. Ama bu mesajı okuyorsan, yeterince "
            "yürüdün. Tebrikler, kaptan! 🏁")


def check_milestones(pf, bars, now_value=None):
    """Portföy değeri (lotu yazılı hisseler, son iki 15 dk'lık mum da) bir kilometre taşını aştıysa, ve bu taş
    daha önce kutlanmadıysa mesaj listesi döner. Durum şifreli portfoy_hedef.enc dosyasında tutulur."""
    holds = {}
    for h in pf:
        if h["qty"]:
            holds[h["sym"]] = holds.get(h["sym"], 0) + h["qty"]
    if not holds or not os.environ.get("SNAPSHOT_KEY", "").strip():
        return None
    v_now = v_prev = 0.0
    for sy, q in holds.items():
        df = bars.get(sy)
        if df is None or len(df) < 2:
            return None   # eksik veri: yanlış karar verme
        v_now += q * float(df["Close"].iloc[-1])
        v_prev += q * float(df["Close"].iloc[-2])
    value = v_now
    reached = [m for m in MILESTONES if v_now >= m and v_prev >= m]   # iki mum üst üste: tek hatalı fiyat sayılmaz
    if not reached:
        return None
    import portfoy_defter as dfr
    goals = dfr.load_goals()
    new = [m for m in reached if str(m) not in goals]
    if not new:
        return None
    today = now_tr().strftime("%Y-%m-%d")
    for m in new:
        goals[str(m)] = today
    dfr.save_goals(goals)    # önce kaydet: mesaj iki kez gitmesin
    return [milestone_text(m, value) for m in sorted(new)]


def run():
    pf = parse_portfolio(os.environ.get("PORTFOY", ""))
    if not pf:
        send("⚠️ Portföy boş. GitHub → Settings → Secrets → PORTFOY secret'ına hisselerini yaz.\n"
             "Örnek: ATAGY 8,20 1000 7,60 9,50; EUYO 4,30 500")
        return
    now = now_tr()
    mins = now.hour * 60 + now.minute
    if now.weekday() >= 5 or not (START_MIN <= mins <= END_MIN):
        print("seans dışı")
        return
    bars = load_bars([h["sym"] for h in pf])
    today = now.date()
    lines, alerts, tot_pnl, tot_val, have_qty = [], [], 0.0, 0.0, False
    last_time = None
    for h in pf:
        t = h["sym"]
        df = bars.get(t)
        cur = prev = None
        if df is not None:
            cur, m = flags_at(df, today, len(df) - 1)
            if m is not None:
                prev, _ = flags_at(df, today, len(df) - 2) if len(df) > 1 else ({}, None)
        if df is None or m is None:
            lines.append(f"{t}: bugün veri yok")
            continue
        last = m["last"]
        last_time = m["time"] if last_time is None or m["time"] > last_time else last_time
        lv_now = level_flags(h, last)
        pdf = df.iloc[:-1]
        lv_prev = level_flags(h, float(pdf["Close"].iloc[-1])) if len(pdf) else {}
        cur_all = {**cur, **lv_now}
        prev_all = {**(prev or {}), **lv_prev}
        part = f"{t}: {last:.2f} TL | bugün %{m['day']:+.1f}"
        if m["m15"] is not None:
            part += f" | son 15 dk %{m['m15']:+.1f}"
        if h["cost"]:
            pl = (last / h["cost"] - 1) * 100
            part += f" | maliyete göre %{pl:+.1f}"
            if h["qty"]:
                pnl = (last - h["cost"]) * h["qty"]
                tot_pnl += pnl
                tot_val += last * h["qty"]
                have_qty = True
                part += f" ({pnl:+,.0f} TL)"
        marks = "".join(sorted({ICON[k] for k in cur_all}))
        stop_v = h["stop"] if h["stop"] else (h["cost"] * (1 - STOP_PCT / 100) if h["cost"] else None)
        tgt_v = h["target"] if h["target"] else (h["cost"] * (1 + TARGET_PCT / 100) if h["cost"] else None)
        lv = []
        if stop_v:
            lv.append(f"stop {stop_v:.2f}")
        if tgt_v:
            lv.append(f"hedef {tgt_v:.2f}")
        lines.append(part + (f" {marks}" if marks else "") + (f"\n   ({' | '.join(lv)})" if lv else ""))
        fresh = [k for k in cur_all if k not in prev_all]
        groups = {}
        for k in fresh:
            groups.setdefault(ICON[k], []).append(cur_all[k])
        for icon, why in groups.items():
            body = f"{icon} {t}: {TITLE[icon]}\n- " + "\n- ".join(why) + f"\nFiyat: {last:.2f} TL (bugün %{m['day']:+.1f})"
            if h["cost"]:
                body += f" | maliyete göre %{(last / h['cost'] - 1) * 100:+.1f}"
            body += "\nBu bir bilgilendirmedir, tavsiye değildir."
            alerts.append(body)
    try:
        for ms in check_milestones(pf, bars) or []:
            send(ms)
    except Exception as e:
        print("kilometre taşı kontrolü hatası:", repr(e))
    for a in alerts:
        send(a)
    head = f"📋 Portföy durumu ({now:%H:%M})"
    if last_time is not None:
        head += f" | son mum {last_time:%H:%M}"
    foot = f"\n\nToplam kâr/zarar: {tot_pnl:+,.0f} TL (güncel değer {tot_val:,.0f} TL)" if have_qty else ""
    send(head + "\n\n" + "\n".join(lines) + foot)


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("HATA:", repr(e))
        try:
            send(f"⚠️ Portföy takibi hata verdi: {type(e).__name__}: {e}")
        except Exception:
            pass
        raise
