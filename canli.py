"""Canlı takip: TradingView akışına (borsapy) bağlanır, hızlı yükselen hisseleri anında bildirir
ve her uyarının sonrasını kaydeder (ileri test), böylece stratejinin işe yarayıp yaramadığı ölçülür."""
import csv
import os
import queue
import sys
import threading
import time
from collections import deque

from sinyal import MIN_AVG_TL_VOLUME, TICKERS, get_avg_stats, now_tr, send

# ---- ayarlar (hepsi tahmin, ölçümle ayarlanmalı) ----
WINDOW_SEC = 300        # hareket penceresi (5 dk)
MIN_WINDOW_MOVE = 1.5   # pencerede en az % yükseliş
MIN_ABS_MOVE = 0.03     # ve en az bu kadar TL (düşük fiyatlı hisselerde tick gürültüsünü eler)
MIN_VOL_BURST = 3.0     # pencere hacmi / normal 5 dk hacmi
MIN_DAY_CHANGE = 0.5    # günlük değişim alt sınırı (%)
MAX_DAY_CHANGE = 7.0    # üst sınır (%): tavana (%10) yakınsa geç kalınmış
NEAR_HIGH = 0.99        # fiyat günün zirvesinin en az %99'unda
MAX_SPREAD = 1.0        # alış-satış farkı en fazla %
SPREAD_MOVE_RATIO = 0.5 # fark, pencere hareketinin en fazla yarısı (işlem maliyeti kuralı)
COOLDOWN_MIN = 60       # aynı hisse için iki bildirim arası (dk)
START_MIN = 10 * 60 + 15  # açılış gürültüsü: 10:15'ten önce uyarı yok
END_MIN = 17 * 60 + 40    # kapanışa yakın: 17:40'tan sonra uyarı yok
MAX_ALERTS_PER_SESSION = 6
INDEX_SYMBOL = "XU100"
INDEX_MIN_CHANGE = -1.5 # endeks günlük bu seviyenin altındaysa uyarı yok (piyasa düşüşte)
STOP_PCT = 2.0          # örnek stop (%) - ve ileri testte "stop" ölçütü
TARGET_PCT = 4.0        # örnek hedef (%) - ve ileri testte "hedef" ölçütü
ENTRY_BAND_PCT = 0.5    # uygun alım bölgesi: satış fiyatından en fazla bu kadar yukarısı
WEAK_PCT = 1.0          # alış sinyalinden bu kadar düşerse "güvenim azaldı"
GIVEBACK_ARM_PCT = 2.0  # kâr koruma, zirve sinyalden en az bu kadar yükseldiyse devreye girer
TRAIL_PCT = 1.5         # zirveden bu kadar geri çekilirse "kâr al" uyarısı
TIME_EXIT_MIN = 45      # bu kadar dk sonra hâlâ yukarı gitmediyse "momentum bitti"
STALE_SEC = 180         # bu kadar süre hiç veri gelmezse yeniden bağlan
MAX_RECONNECTS = 5
LOOP_WAIT = 5
LOG_FILE = "alerts.csv"
LOG_FIELDS = ["date", "time", "sym", "price", "move", "day", "burst", "spread", "bid_size",
              "ask_size", "index", "stop", "target", "ret15", "ret30", "ret60", "ret_end",
              "mfe", "mae", "outcome"]

CLOCK = time.time


def pct(a, b):
    if a is None or not b:
        return None
    return (a / b - 1) * 100


def f2(x):
    return "" if x is None else f"{x:.2f}"


class Engine:
    def __init__(self, avg, out, clock=None):
        self.avg = avg
        self.out = out
        self.clock = clock or (lambda: CLOCK())
        self.cache = {t: {} for t in TICKERS}
        self.ticks = {t: deque() for t in TICKERS}
        self.last_alert = {}
        self.last_tick = self.clock()
        self.alert_count = 0
        self.index_change = None
        self.active = []  # uyarı sonrası takip edilen kayıtlar
        self.lock = threading.Lock()

    def on_quote(self, sym, quote):
        t = str(sym).split(":")[-1]
        if not isinstance(quote, dict):
            return
        if t == INDEX_SYMBOL:
            c = quote.get("change_percent")
            if c is not None:
                self.index_change = c
            return
        if t not in self.cache:
            return
        with self.lock:
            try:
                msgs = self._handle(t, quote)
            except Exception as e:
                print("on_quote hatası", t, repr(e))
                return
        for m in msgs:
            self.out.put(m)

    # --- uyarı sonrası takip: ileri test ve çıkış (sat) uyarıları ---
    def _exits(self, t):
        q = self.cache[t]
        last = q.get("last")
        if not last:
            return []
        now_ts = self.clock()
        msgs = []
        for r in self.active:
            if r["sym"] != t:
                continue
            r["last"] = last
            r["hi"] = max(r["hi"], last)
            r["lo"] = min(r["lo"], last)
            el = now_ts - r["ts"]
            for m in (15, 30, 60):
                if r[f"p{m}"] is None and el >= m * 60:
                    r[f"p{m}"] = last
            if r["outcome"] is None:
                if last <= r["stop"]:
                    r["outcome"] = "stop"
                elif last >= r["target"]:
                    r["outcome"] = "hedef"
            msgs.extend(self._exit_msgs(r, last, el))
        return msgs

    def _exit_msgs(self, r, last, el):
        if r["closed"]:
            return []
        ev, p, sym = r["ev"], r["price"], r["sym"]
        chg = pct(last, p)
        if last <= r["stop"]:
            r["closed"] = True
            return [f"🛑 {sym} stop seviyesine geldi: {last:.2f} TL (sinyalden %{chg:+.1f}). "
                    f"Sinyal tutmadı, zarar kesmeyi düşün."]
        if r["hi"] >= p * (1 + GIVEBACK_ARM_PCT / 100) and last <= r["hi"] * (1 - TRAIL_PCT / 100):
            r["closed"] = True
            return [f"💰 {sym} zirveden geri çekiliyor: zirve {r['hi']:.2f}, şimdi {last:.2f} TL "
                    f"(sinyalden %{chg:+.1f}). Kâr almayı düşün."]
        msgs = []
        if last >= r["target"] and "hedef" not in ev:
            ev.add("hedef")
            msgs.append(f"🎯 {sym} hedefe ulaştı: {last:.2f} TL (sinyalden %{chg:+.1f}). Kâr almayı düşün; "
                        f"kalırsan stop'u {p * (1 + TARGET_PCT / 200):.2f} civarına çek.")
        if last <= p * (1 - WEAK_PCT / 100) and "zayif" not in ev:
            ev.add("zayif")
            msgs.append(f"⚠️ {sym} zayıfladı: sinyalden %{chg:+.1f} aşağıda ({last:.2f} TL). "
                        f"Güvenim azaldı, yeni alım yapma; almışsan stop {r['stop']:.2f}.")
        if el >= TIME_EXIT_MIN * 60 and last < p * 1.005 and "zaman" not in ev and "hedef" not in ev:
            ev.add("zaman")
            msgs.append(f"⏱ {sym}: {TIME_EXIT_MIN} dk geçti, yükseliş devam etmedi ({last:.2f} TL, "
                        f"sinyalden %{chg:+.1f}). Momentum bitti, çıkmayı düşün.")
        return msgs

    def _handle(self, t, quote):
        entry = self._entry_check(t, quote)
        msgs = self._exits(t)
        if entry:
            msgs.append(entry)
        return msgs

    def _entry_check(self, t, quote):
        now_ts = self.clock()
        self.last_tick = now_ts
        q = self.cache[t]
        q.update({k: v for k, v in quote.items() if v is not None})
        last, vol = q.get("last"), q.get("volume")
        if not last or vol is None:
            return None

        dq = self.ticks[t]
        dq.append((now_ts, last, vol))
        # pencere başındaki fiyatı taşımak için pencereden eski tek bir örnek tutulur
        while len(dq) > 1 and dq[1][0] <= now_ts - WINDOW_SEC:
            dq.popleft()

        now_dt = now_tr()
        mins = now_dt.hour * 60 + now_dt.minute
        if not (START_MIN <= mins < END_MIN):
            return None
        if self.alert_count >= MAX_ALERTS_PER_SESSION:
            return None
        if self.index_change is not None and self.index_change < INDEX_MIN_CHANGE:
            return None
        prev_alert = self.last_alert.get(t)
        if prev_alert is not None and now_ts - prev_alert < COOLDOWN_MIN * 60:
            return None
        a = self.avg.get(t)
        if not a or a["tl"] < MIN_AVG_TL_VOLUME or a["vol"] <= 0:
            return None

        ref = dq[0]
        elapsed = now_ts - ref[0]
        if elapsed < 0.8 * WINDOW_SEC:
            return None
        move_abs = last - ref[1]
        move = move_abs / ref[1] * 100
        if move < MIN_WINDOW_MOVE or move_abs < MIN_ABS_MOVE:
            return None

        vol_delta = vol - ref[2]
        if vol_delta <= 0:
            return None
        eff = min(elapsed, WINDOW_SEC)
        burst = vol_delta / (a["vol"] / 96 * (eff / 300))
        if burst < MIN_VOL_BURST:
            return None

        day = q.get("change_percent")
        prev = q.get("prev_close")
        if day is None and prev:
            day = (last / prev - 1) * 100
        if day is None or not (MIN_DAY_CHANGE <= day <= MAX_DAY_CHANGE):
            return None
        high = q.get("high") or last
        if last < NEAR_HIGH * high:
            return None

        bid, ask = q.get("bid") or 0, q.get("ask") or 0
        if not (bid and ask and ask >= bid):
            return None
        spread = (ask - bid) / last * 100
        if spread > MAX_SPREAD or spread > SPREAD_MOVE_RATIO * move:
            return None

        stop = last * (1 - STOP_PCT / 100)
        target = last * (1 + TARGET_PCT / 100)
        self.last_alert[t] = now_ts
        self.alert_count += 1
        self.active.append({
            "date": str(now_dt.date()), "time": now_dt.strftime("%H:%M:%S"), "sym": t,
            "price": last, "move": move, "day": day, "burst": burst, "spread": spread,
            "bid_size": q.get("bid_size"), "ask_size": q.get("ask_size"),
            "index": self.index_change, "stop": stop, "target": target, "ts": now_ts,
            "hi": last, "lo": last, "last": last, "p15": None, "p30": None, "p60": None,
            "outcome": None, "closed": False, "ev": set(),
        })
        return (
            f"🚀 {t} hızla yükseliyor\n"
            f"Şu anki fiyat: {last:.2f} TL\n"
            f"Son {eff / 60:.0f} dakikada: %{move:+.1f} yükseldi\n"
            f"Bugün toplam: %{day:+.1f}\n"
            f"İşlem hacmi: normalin {burst:.1f} katı\n"
            f"Alış fiyatı: {bid:.2f} | Satış fiyatı: {ask:.2f} (aradaki fark %{spread:.1f})\n"
            f"Günün en yüksek fiyatı: {high:.2f}\n"
            f"Alım için uygun aralık: {ask:.2f} - {ask * (1 + ENTRY_BAND_PCT / 100):.2f} TL "
            f"(bunun üstüne çıkarsa peşinden koşma)\n"
            f"Örnek zarar durdur: {stop:.2f} TL (-%{STOP_PCT:g})\n"
            f"Örnek hedef: {target:.2f} TL (+%{TARGET_PCT:g})\n"
            f"⏳ Veri yaklaşık 15 dk gecikmeli olabilir, fiyatı kendi uygulamandan kontrol et."
        )

    def finalize(self):
        with self.lock:
            rows = []
            for r in self.active:
                p = r["price"]
                rows.append({
                    "date": r["date"], "time": r["time"], "sym": r["sym"],
                    "price": f"{p:.4f}", "move": f2(r["move"]), "day": f2(r["day"]),
                    "burst": f2(r["burst"]), "spread": f2(r["spread"]),
                    "bid_size": "" if r["bid_size"] is None else r["bid_size"],
                    "ask_size": "" if r["ask_size"] is None else r["ask_size"],
                    "index": f2(r["index"]), "stop": f"{r['stop']:.4f}",
                    "target": f"{r['target']:.4f}",
                    "ret15": f2(pct(r["p15"], p)), "ret30": f2(pct(r["p30"], p)),
                    "ret60": f2(pct(r["p60"], p)), "ret_end": f2(pct(r["last"], p)),
                    "mfe": f2(pct(r["hi"], p)), "mae": f2(pct(r["lo"], p)),
                    "outcome": r["outcome"] or "",
                })
            return rows


def write_log(rows):
    if not rows:
        return
    new = not os.path.exists(LOG_FILE)
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=LOG_FIELDS)
        if new:
            w.writeheader()
        w.writerows(rows)


def summary_text(rows):
    if not rows:
        return "Bu oturumda uyarı yok."
    lines = []
    for r in rows:
        sonuc = {"hedef": "hedefe ulaştı ✅", "stop": "zarar durdurdu ❌"}.get(r["outcome"], "henüz sonuçlanmadı")
        lines.append(
            f"{r['sym']} (uyarı saati {r['time'][:5]}): şu an %{r['ret_end']}"
            f" | en yüksek %{r['mfe']} | en düşük %{r['mae']} | {sonuc}"
        )
    return "Uyarılardan sonra ne oldu:\n" + "\n".join(lines)


def report_text(path=None):
    path = path or LOG_FILE
    if not os.path.exists(path):
        return "📊 Henüz kayıtlı uyarı yok."
    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    n = len(rows)
    if n == 0:
        return "📊 Henüz kayıtlı uyarı yok."

    def vals(k, rs=None):
        out = []
        for r in rs if rs is not None else rows:
            try:
                out.append(float(r[k]))
            except (ValueError, KeyError, TypeError):
                pass
        return out

    def mean(v):
        return sum(v) / len(v) if v else None

    def show(x):
        return "-" if x is None else f"%{x:+.2f}"

    hedef = sum(1 for r in rows if r["outcome"] == "hedef")
    stop = sum(1 for r in rows if r["outcome"] == "stop")
    acik = n - hedef - stop
    net = [float(r["ret60"]) - float(r["spread"]) for r in rows if r["ret60"] and r["spread"]]
    lines = [
        f"📊 Uyarı karnesi (toplam {n} uyarı)",
        f"Önce hedefe ulaşan (+%{TARGET_PCT:g}): {hedef} adet (%{hedef / n * 100:.0f})",
        f"Önce zarar durduran (-%{STOP_PCT:g}): {stop} adet (%{stop / n * 100:.0f})",
        f"İkisine de ulaşmayan: {acik} adet",
        "Uyarıdan sonra ortalama getiri: 15 dk sonra " + show(mean(vals("ret15")))
        + " | 30 dk sonra " + show(mean(vals("ret30")))
        + " | 60 dk sonra " + show(mean(vals("ret60")))
        + " | seans sonunda " + show(mean(vals("ret_end"))),
        "60 dk sonraki getiri, alış-satış farkı düşülünce: " + show(mean(net)),
        "Uyarıdan sonra ortalama en derin düşüş: " + show(mean(vals("mae"))),
    ]
    if n < 30:
        lines.append("Not: 30'dan az uyarıyla sonuçlar rastlantı olabilir, para koymak için erken.")
    return "\n".join(lines)


def apply_auth(bp):
    session = os.environ.get("TV_SESSION")
    sign = os.environ.get("TV_SESSION_SIGN")
    user = os.environ.get("TV_USERNAME")
    pw = os.environ.get("TV_PASSWORD")
    try:
        if session and sign:
            try:
                bp.set_tradingview_auth(session=session, session_sign=sign)
            except TypeError:
                bp.set_tradingview_auth(session=session, signature=sign)
            return "giriş yapıldı (çerez ile)"
        if user and pw:
            bp.set_tradingview_auth(username=user, password=pw)
            return "giriş yapıldı (kullanıcı adı ile)"
    except Exception as e:
        print("TradingView girişi başarısız:", repr(e))
        return "giriş BAŞARISIZ"
    return "giriş yok (veri yaklaşık 15 dk gecikmeli)"


def open_stream(bp, engine):
    stream = bp.TradingViewStream()
    stream.connect()
    stream.on_any_quote(engine.on_quote)
    for t in TICKERS:
        stream.subscribe(t)
    try:
        stream.subscribe(INDEX_SYMBOL)
    except Exception as e:
        print("endeks aboneliği başarısız:", repr(e))
    return stream


def session_end(now):
    h, m = (14, 0) if now.hour < 14 else (18, 5)
    return now.replace(hour=h, minute=m, second=0, microsecond=0)


def safe_send(text):
    try:
        send(text)
    except Exception as e:
        print("Telegram hatası:", repr(e))


def run_live():
    import borsapy as bp

    auth = apply_auth(bp)
    avg = get_avg_stats(TICKERS)
    if not avg:
        raise RuntimeError("Ortalama hacim verisi alınamadı")
    out = queue.Queue()
    engine = Engine(avg, out)
    stream = open_stream(bp, engine)
    end = session_end(now_tr())
    safe_send(
        f"🟢 Canlı takip başladı ({now_tr():%H:%M})\n"
        f"Takip edilen hisse: {len(avg)} / {len(TICKERS)}\n"
        f"TradingView hesabı: {auth}\n"
        f"Bu oturum {end:%H:%M}'de bitecek"
    )

    reconnects = 0
    try:
        while now_tr() < end:
            try:
                safe_send(out.get(timeout=LOOP_WAIT))
            except queue.Empty:
                pass
            now = now_tr()
            mins = now.hour * 60 + now.minute
            stale = engine.clock() - engine.last_tick > STALE_SEC
            if stale and 10 * 60 + 5 <= mins < 18 * 60 and reconnects < MAX_RECONNECTS:
                reconnects += 1
                print("akış durdu, yeniden bağlanılıyor:", reconnects)
                try:
                    stream.disconnect()
                except Exception:
                    pass
                stream = open_stream(bp, engine)
                engine.last_tick = engine.clock()
                if reconnects == 1:
                    safe_send("⚠️ Veri akışı durdu, yeniden bağlanılıyor.")
                if reconnects == MAX_RECONNECTS:
                    safe_send("⚠️ Akış tekrar tekrar durdu, daha fazla denenmeyecek (tatil olabilir).")
    finally:
        try:
            stream.disconnect()
        except Exception:
            pass
        rows = engine.finalize()
        try:
            write_log(rows)
        except Exception as e:
            print("Kayıt yazılamadı:", repr(e))
    safe_send(f"🔴 Canlı takip bitti | {engine.alert_count} uyarı\n" + summary_text(rows))


def run_report():
    safe_send(report_text())


def run_test():
    import borsapy as bp

    lines = [f"TradingView hesabı: {apply_auth(bp)}"]
    seen = {"n": 0}
    quotes = {}
    stream = bp.TradingViewStream()
    try:
        stream.connect()
        stream.on_any_quote(lambda s, q: seen.__setitem__("n", seen["n"] + 1))
        for t in TICKERS:
            try:
                stream.subscribe(t)
            except Exception:
                pass
        try:
            stream.subscribe(INDEX_SYMBOL)
        except Exception as e:
            lines.append(f"Endeks (BIST 100) takibi başlatılamadı: {e}")
        time.sleep(20)
        lines.append(f"Son 20 saniyede gelen fiyat güncellemesi: {seen['n']}")
        for t in TICKERS:
            try:
                q = stream.wait_for_quote(t, timeout=0.5)
            except Exception:
                q = None
            if q:
                quotes[t] = q
        yok = [t for t in TICKERS if t not in quotes]
        derinliksiz = [t for t in quotes if quotes[t].get("bid") is None or quotes[t].get("ask") is None]
        lines.append(f"Veri gelen hisse: {len(quotes)} / {len(TICKERS)}")
        if yok:
            lines.append("Hiç veri gelmeyenler: " + ", ".join(yok))
        if derinliksiz:
            lines.append("Alış/satış fiyatı boş gelenler (bu hisselerde sinyal verilmez): " + ", ".join(derinliksiz))
        try:
            iq = stream.wait_for_quote(INDEX_SYMBOL, timeout=5.0)
        except Exception:
            iq = None
        if iq:
            lines.append(f"BIST 100 endeksi: {iq.get('last')} (bugün %{iq.get('change_percent')})")
        else:
            lines.append("BIST 100 endeksi: veri gelmedi (piyasa düşüş filtresi çalışmaz)")
        lines.append("")
        lines.append("Örnek hisseler:")
        for t in list(quotes)[:3]:
            q = quotes[t]
            lines.append(
                f"{t}: fiyat {q.get('last')} | alış {q.get('bid')} | satış {q.get('ask')}"
                f" | bugünkü hacim {q.get('volume')}"
            )
        lines.append("")
        lines.append("Hacim kontrolü (iki sayı aynı büyüklükte olmalı):")
        for t, a in get_avg_stats(list(quotes)[:3]).items():
            lines.append(
                f"{t}: son 20 günün ortalama günlük hacmi {a['vol']:,.0f}"
                f" | bugünkü hacim {quotes[t].get('volume')}"
            )
    except Exception as e:
        lines.append(f"HATA: {type(e).__name__}: {e}")
    finally:
        try:
            stream.disconnect()
        except Exception:
            pass
    safe_send("🔧 Bağlantı testi sonucu\n" + "\n".join(lines))


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "canli"
    try:
        if mode == "test":
            run_test()
        elif mode == "rapor":
            run_report()
        else:
            run_live()
    except Exception as e:
        print("HATA:", repr(e))
        safe_send(f"⚠️ Canlı takip hata verdi: {type(e).__name__}: {e}")
        raise


if __name__ == "__main__":
    main()
