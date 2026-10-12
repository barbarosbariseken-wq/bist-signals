"""Canlı takip: TradingView akışına (borsapy) bağlanır, hızlı yükselen hisseleri anında bildirir."""
import os
import queue
import sys
import threading
import time
from collections import deque

from sinyal import MIN_AVG_TL_VOLUME, TICKERS, get_avg_stats, now_tr, send

# ---- ayarlar ----
WINDOW_SEC = 300        # hareket penceresi (5 dk)
MIN_WINDOW_MOVE = 1.5   # pencerede en az % yükseliş
MIN_ABS_MOVE = 0.03     # ve en az bu kadar TL (düşük fiyatlı hisselerde tick gürültüsünü eler)
MIN_VOL_BURST = 3.0     # pencere hacmi / normal 5 dk hacmi
MIN_DAY_CHANGE = 0.5    # günlük değişim alt sınırı (%)
MAX_DAY_CHANGE = 7.0    # günlük değişim üst sınırı (%) - tavana yakınsa geç kalınmış
NEAR_HIGH = 0.99        # fiyat günün zirvesinin en az %99'unda
MAX_SPREAD = 1.5        # alış-satış farkı en fazla %
COOLDOWN_MIN = 60       # aynı hisse için iki bildirim arası (dk)
STALE_SEC = 180         # bu kadar süre hiç veri gelmezse yeniden bağlan
MAX_RECONNECTS = 5
LOOP_WAIT = 5

CLOCK = time.time


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
        self.lock = threading.Lock()

    def on_quote(self, sym, quote):
        t = str(sym).split(":")[-1]
        if t not in self.cache or not isinstance(quote, dict):
            return
        with self.lock:
            try:
                msg = self._handle(t, quote)
            except Exception as e:
                print("on_quote hatası", t, repr(e))
                return
        if msg:
            self.out.put(msg)

    def _handle(self, t, quote):
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
        if not (10 * 60 + 5 <= mins < 18 * 60):
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
        spread = (ask - bid) / last * 100 if bid and ask and ask >= bid else None
        if spread is not None and spread > MAX_SPREAD:
            return None

        self.last_alert[t] = now_ts
        self.alert_count += 1
        parts = [
            f"🚀 {t} yükseliyor: {last:.2f} TL",
            f"{eff / 60:.0f} dk'da %{move:+.1f}",
            f"gün %{day:+.1f}",
            f"hacim {burst:.1f}x",
        ]
        if spread is not None:
            parts.append(f"alış {bid:.2f} / satış {ask:.2f}")
        parts.append(f"günün zirvesi {high:.2f}")
        return " | ".join(parts)


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
            return "çerez"
        if user and pw:
            bp.set_tradingview_auth(username=user, password=pw)
            return "kullanıcı adı/şifre"
    except Exception as e:
        print("TradingView girişi başarısız:", repr(e))
        return "BAŞARISIZ"
    return "yok (veri gecikmeli olabilir)"


def open_stream(bp, engine):
    stream = bp.TradingViewStream()
    stream.connect()
    stream.on_any_quote(engine.on_quote)
    for t in TICKERS:
        stream.subscribe(t)
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
        f"🟢 Canlı takip başladı ({now_tr():%H:%M}) | {len(avg)}/{len(TICKERS)} hisse"
        f" | TradingView girişi: {auth} | bitiş {end:%H:%M}"
    )

    reconnects = 0
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

    try:
        stream.disconnect()
    except Exception:
        pass
    safe_send(f"🔴 Canlı takip bitti | bu oturumda {engine.alert_count} uyarı")


def run_test():
    import borsapy as bp

    lines = [f"TradingView girişi: {apply_auth(bp)}"]
    seen = {"n": 0}
    stream = bp.TradingViewStream()
    try:
        stream.connect()
        stream.on_any_quote(lambda s, q: seen.__setitem__("n", seen["n"] + 1))
        sample = TICKERS[:3]
        for t in sample:
            stream.subscribe(t)
        for i, t in enumerate(sample):
            q = stream.wait_for_quote(t, timeout=8.0)
            if not q:
                lines.append(f"{t}: quote gelmedi")
                continue
            lines.append(
                f"{t}: son {q.get('last')} | alış {q.get('bid')} | satış {q.get('ask')}"
                f" | hacim {q.get('volume')} | zaman {q.get('timestamp')}"
            )
            if i == 0:
                lines.append("alanlar: " + ", ".join(sorted(q.keys())))
        n0 = seen["n"]
        time.sleep(20)
        lines.append(f"20 sn'de gelen güncelleme sayısı: {seen['n'] - n0}")
    except Exception as e:
        lines.append(f"HATA: {type(e).__name__}: {e}")
    finally:
        try:
            stream.disconnect()
        except Exception:
            pass
    safe_send("🔧 Canlı test sonucu\n" + "\n".join(lines))


def main():
    mode = sys.argv[1] if len(sys.argv) > 1 else "canli"
    try:
        run_test() if mode == "test" else run_live()
    except Exception as e:
        print("HATA:", repr(e))
        safe_send(f"⚠️ Canlı takip hata verdi: {type(e).__name__}: {e}")
        raise


if __name__ == "__main__":
    main()
