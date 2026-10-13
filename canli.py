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
MAX_ALERTS_PER_SESSION = 8
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
# --- ek kalıplar ---
HIST_SEC = 35 * 60         # geçmiş fiyat hafızası
BRK_LOOKBACK_MIN = 30      # kırılım: son 30 dk'ya bakılır (son 2 dk hariç)
BRK_MIN_WATCH_MIN = 20     # kırılım: en az 20 dk veri toplanmış olmalı
BRK_MAX_RANGE = 2.0        # kırılım: bekleme aralığı en fazla %2 genişlikte
BRK_MARGIN = 0.3           # kırılım: aralığın en az %0,3 üstüne çıkmalı
BRK_MIN_BURST = 2.0        # kırılım: hacim normalin en az 2 katı
PB_MIN_DAY = 2.0           # geri dönüş: gün içinde en az %2 yükselmiş olmalı
PB_MIN_DROP = 1.0          # geri dönüş: zirveden en az %1 geri çekilmiş
PB_MAX_DROP = 4.0          # ... ama %4'ten fazla değil (çöküş olmasın)
PB_MIN_BOUNCE = 0.4        # geri dönüş: dipten en az %0,4 toparlanmış
PB_MIN_BURST = 1.5
BOOK_RATIO = 3.0           # tahta: alış tarafı satış tarafının en az 3 katı
BOOK_HOLD_SEC = 30         # ... ve bu durum en az 30 sn sürmüş
BOOK_MIN_MOVE = 0.5        # tahta: son 5 dk'da en az %0,5 yükselmiş
BOOK_MIN_BURST = 1.5
PATTERN_NAMES = {"ivme": "Hızlı yükseliş", "kirilim": "Dar aralık kırılımı",
                 "geri_donus": "Düşüşten dönüş", "tahta": "Alıcı baskısı"}
# --- tavan takibi ---
LIMIT_PCT = 10.0            # günlük fiyat sınırı (%)
LIMIT_MIN_DAY = 9.5         # en az bu kadar yükselmişse tavan sayılabilir
LIMIT_LEAVE_SEC = 120       # tavandan bu kadar süre uzak kalırsa tavan bozuldu sayılır
LIMIT_START_MIN = 10 * 60   # tavan bildirimleri 10:00-17:50 arası
LIMIT_END_MIN = 17 * 60 + 50
LIMIT_MIN_QUEUE_TL = 200_000  # en yüksek kuyruk bunun altındaysa azalma bildirimi yapılmaz (gürültü)
LIMIT_DROP_1 = 0.50         # kuyruk bu orana (en yüksek kuyruğun %50'sine) inince ilk bildirim
LIMIT_DROP_2 = 0.25         # %75 azalınca ikinci bildirim
MAX_LIMIT_MSGS = 40
BLOCK_MIN_TL = 1_000_000   # tek güncellemede en az bu kadar TL'lik işlem = "blok" sayılır
BLOCK_MIN_PCT_AVG = 8.0    # ve normal günlük hacmin en az bu kadar %'si
BLOCK_COOLDOWN_MIN = 30    # aynı hisse için iki blok bildirimi arası (dk)
MAX_BLOCKS_PER_SESSION = 10
LOG_FILE = "alerts.csv"
LOG_FIELDS = ["date", "time", "sym", "pattern", "price", "move", "day", "burst", "spread", "bid_size",
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
        self.hist = {t: deque() for t in TICKERS}
        self.book_since = {}
        self.last_alert = {}
        self.last_tick = self.clock()
        self.alert_count = 0
        self.block_count = 0
        self.last_block = {}
        self.limit = {}
        self.limit_count = 0
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
        prev_vol = self.cache[t].get("volume")
        prev_last = self.cache[t].get("last")
        entry = self._entry_check(t, quote)
        msgs = self._exits(t)
        if entry:
            msgs.append(entry)
        block = self._block_check(t, prev_vol, prev_last)
        if block:
            msgs.append(block)
        msgs.extend(self._limit_check(t))
        return msgs

    @staticmethod
    def _tick(p):
        for top, step in ((20, 0.01), (50, 0.02), (100, 0.05), (250, 0.10), (500, 0.25),
                          (1000, 0.50), (2500, 1.0)):
            if p < top:
                return step
        return 2.5

    def _limit_check(self, t):
        """Tavana kilitlenen hisseyi bildirir; alış kuyruğu en yüksek değerinin %50'sine ve %25'ine inince uyarır.
        Kuyruk = tavan fiyatındaki bekleyen alış miktarı (bid_size). Veri gecikmeliyse uyarı da gecikir."""
        q = self.cache[t]
        last, prev = q.get("last"), q.get("prev_close")
        if not last or not prev:
            return []
        now_ts = self.clock()
        now_dt = now_tr()
        mins = now_dt.hour * 60 + now_dt.minute
        if not (LIMIT_START_MIN <= mins < LIMIT_END_MIN):
            return []
        tick = self._tick(prev * (1 + LIMIT_PCT / 100))
        ceiling = int(prev * (1 + LIMIT_PCT / 100) / tick + 1e-6) * tick
        day = (last / prev - 1) * 100
        at_limit = last >= ceiling - tick * 0.5 and day >= LIMIT_MIN_DAY
        st = self.limit.get(t)
        bs = q.get("bid_size")

        if not at_limit:
            if st is not None:
                if st["off_since"] is None:
                    st["off_since"] = now_ts
                elif now_ts - st["off_since"] >= LIMIT_LEAVE_SEC:
                    del self.limit[t]  # tavan bozuldu; tekrar kilitlenirse yeni bildirim gelir
            return []

        msgs = []
        if st is None:
            if self.limit_count >= MAX_LIMIT_MSGS:
                return []
            st = self.limit[t] = {"peak": 0.0, "since": now_ts, "off_since": None, "s50": False, "s75": False}
            self.limit_count += 1
            if bs:
                st["peak"] = float(bs)
                qtxt = (f"Tavanda bekleyen alış: {bs:,.0f} adet (yaklaşık {bs * last / 1e6:.1f} milyon TL)\n"
                        f"Bu kuyruk %50 ve %75 eridiğinde ayrıca haber vereceğim.")
            else:
                qtxt = "Tavanda bekleyen alış miktarı verisi gelmiyor, kuyruk uyarısı yapılamaz."
            msgs.append(
                f"🔒 {t} tavan oldu\n"
                f"Fiyat: {last:.2f} TL (bugün %{day:+.1f})\n{qtxt}"
            )
            return msgs
        st["off_since"] = None
        if not bs:
            return []
        bs = float(bs)
        if bs > st["peak"]:
            st["peak"] = bs
            return []
        peak = st["peak"]
        if peak * last < LIMIT_MIN_QUEUE_TL:
            return []
        ratio = bs / peak
        gone = (1 - ratio) * 100
        head = (f"Fiyat: {last:.2f} TL (bugün %{day:+.1f})\n"
                f"Takip ettiğim en yüksek kuyruk: {peak:,.0f} adet → şimdi {bs:,.0f} adet (%{gone:.0f} azaldı)\n"
                f"Alıcılar çekilmiş ya da satışlar karşılanmış olabilir.")
        if ratio <= LIMIT_DROP_2 and not st["s75"]:
            st["s75"] = st["s50"] = True
            msgs.append(f"🚨 {t}: tavandaki alış kuyruğu %75 azaldı, tavan bozulabilir\n" + head)
        elif ratio <= LIMIT_DROP_1 and not st["s50"]:
            st["s50"] = True
            msgs.append(f"⚠️ {t}: tavandaki alış kuyruğu %50 azaldı\n" + head)
        return msgs

    def _block_check(self, t, prev_vol, prev_last):
        """Tek güncellemede çok büyük hacim gelip fiyat düşmediyse 'blok alım olabilir' der.
        Veride kimin aldığı/sattığı yok; yön, fiyatın alış-satış ortasının üstünde olmasından tahmin edilir."""
        q = self.cache[t]
        last, vol = q.get("last"), q.get("volume")
        if not last or vol is None or prev_vol is None or prev_last is None:
            return None
        delta = vol - prev_vol
        if delta <= 0:
            return None
        now_ts = self.clock()
        now_dt = now_tr()
        mins = now_dt.hour * 60 + now_dt.minute
        if not (START_MIN <= mins < END_MIN):
            return None
        if self.block_count >= MAX_BLOCKS_PER_SESSION:
            return None
        a = self.avg.get(t)
        if not a or a["tl"] < MIN_AVG_TL_VOLUME or a["vol"] <= 0:
            return None
        delta_tl = delta * last
        if delta_tl < BLOCK_MIN_TL or delta < BLOCK_MIN_PCT_AVG / 100 * a["vol"]:
            return None
        if delta > 1.5 * a["vol"]:
            return None  # veri sıfırlanması/hata olabilir
        if last < prev_last:
            return None
        bid, ask = q.get("bid"), q.get("ask")
        if bid and ask and ask >= bid and last < (bid + ask) / 2:
            return None
        prev_block = self.last_block.get(t)
        if prev_block is not None and now_ts - prev_block < BLOCK_COOLDOWN_MIN * 60:
            return None
        self.last_block[t] = now_ts
        self.block_count += 1
        day = q.get("change_percent")
        day_txt = f"\nBugün toplam: %{day:+.1f}" if day is not None else ""
        return (
            f"🧱 {t}: büyük alım olabilir\n"
            f"Şu anki fiyat: {last:.2f} TL{day_txt}\n"
            f"Tek seferde gelen işlem: yaklaşık {delta:,.0f} adet ({delta_tl / 1e6:.1f} milyon TL)\n"
            f"Bu, hissenin normal günlük hacminin %{delta / a['vol'] * 100:.0f}'i kadar\n"
            f"Fiyat bu işlemde düşmedi, alıcı baskın görünüyor.\n"
            f"Not: Kimin aldığı kesin bilinmez, bu bir tahmindir. Veri yaklaşık 15 dk gecikmeli "
            f"olabilir, kendi uygulamandan kontrol et."
        )

    def _win(self, t, secs, now_ts, last, vol, a):
        """Son 'secs' saniyedeki hareket ve hacim patlaması: (hareket %, hacim katı, süre, hareket TL)."""
        hist = self.hist[t]
        ref = None
        for smp in reversed(hist):
            if smp[0] <= now_ts - secs:
                ref = smp
                break
        if ref is None:
            ref = hist[0]
        elapsed = now_ts - ref[0]
        if elapsed < 0.8 * secs or not ref[1]:
            return None
        move_abs = last - ref[1]
        move = move_abs / ref[1] * 100
        vol_delta = vol - ref[2]
        eff = min(elapsed, secs)
        burst = vol_delta / (a["vol"] / 96 * (eff / 300)) if vol_delta > 0 else 0.0
        return move, burst, eff, move_abs

    # --- kalıplar: her biri uygunsa {"pattern","title","lines",...} döner ---
    def _p_momentum(self, c):
        w = c["w5"]
        if not w:
            return None
        move, burst, eff, move_abs = w
        if move < MIN_WINDOW_MOVE or move_abs < MIN_ABS_MOVE or burst < MIN_VOL_BURST:
            return None
        if c["last"] < NEAR_HIGH * c["high"]:
            return None
        if c["spread"] > SPREAD_MOVE_RATIO * move:
            return None
        return {"pattern": "ivme", "title": f"🚀 {c['t']} hızla yükseliyor",
                "lines": [f"Son {eff / 60:.0f} dakikada: %{move:+.1f} yükseldi"]}

    def _p_breakout(self, c):
        t, now, last = c["t"], c["now"], c["last"]
        hist = self.hist[t]
        if hist[0][0] > now - BRK_MIN_WATCH_MIN * 60:
            return None
        segt = [s for s in hist if now - BRK_LOOKBACK_MIN * 60 <= s[0] <= now - 120]
        if len(segt) < 8:
            return None
        seg = [s[1] for s in segt]
        span_min = (segt[-1][0] - segt[0][0]) / 60
        hi, lo = max(seg), min(seg)
        if not lo:
            return None
        width = (hi / lo - 1) * 100
        if width > BRK_MAX_RANGE or last < hi * (1 + BRK_MARGIN / 100):
            return None
        if last < NEAR_HIGH * c["high"]:
            return None
        w = c["w5"]
        if not w or w[1] < BRK_MIN_BURST:
            return None
        return {"pattern": "kirilim", "title": f"📈 {t} dar aralığı yukarı kırdı",
                "lines": [f"Yaklaşık {span_min:.0f} dakikadır {lo:.2f} - {hi:.2f} TL arasında bekliyordu "
                          f"(aralık %{width:.1f})",
                          f"Şimdi {last:.2f} TL ile bu aralığın üstüne çıktı"],
                "stop": max(lo, last * (1 - STOP_PCT / 100))}

    def _p_pullback(self, c):
        t, now, last, high = c["t"], c["now"], c["last"], c["high"]
        if c["day"] < PB_MIN_DAY or last > high * 0.995:
            return None
        hist = self.hist[t]
        if hist[0][0] > now - 10 * 60:
            return None
        low = min(s[1] for s in hist if s[0] >= now - 20 * 60)
        if not low:
            return None
        drop = (high / low - 1) * 100
        bounce = (last / low - 1) * 100
        if not (PB_MIN_DROP <= drop <= PB_MAX_DROP) or bounce < PB_MIN_BOUNCE:
            return None
        w = c["w5"]
        if not w or w[1] < PB_MIN_BURST:
            return None
        stop = max(min(low * 0.997, last * 0.99), last * 0.95)
        return {"pattern": "geri_donus", "title": f"↩️ {t} düşüşten dönüyor",
                "lines": [f"Bugün %{c['day']:+.1f} yükselmişti, zirveden (%{drop:.1f}) geri çekildi",
                          f"Dipten (%{bounce:.1f}) toparlandı, hacim artıyor"],
                "stop": stop}

    def _p_book(self, c):
        t, now = c["t"], c["now"]
        since = self.book_since.get(t)
        if since is None or now - since < BOOK_HOLD_SEC:
            return None
        w = c["w5"]
        if not w or w[0] < BOOK_MIN_MOVE or w[1] < BOOK_MIN_BURST:
            return None
        q = self.cache[t]
        ratio = q["bid_size"] / q["ask_size"]
        return {"pattern": "tahta", "title": f"⚖️ {t} alıcılar baskın",
                "lines": [f"Alış tarafında bekleyen miktar, satış tarafının {ratio:.1f} katı "
                          f"(en az {BOOK_HOLD_SEC} sn'dir)",
                          f"Son 5 dakikada: %{w[0]:+.1f}"]}

    def _entry_check(self, t, quote):
        now_ts = self.clock()
        self.last_tick = now_ts
        q = self.cache[t]
        q.update({k: v for k, v in quote.items() if v is not None})
        last, vol = q.get("last"), q.get("volume")
        if not last or vol is None:
            return None

        hist = self.hist[t]
        hist.append((now_ts, last, vol))
        while len(hist) > 1 and hist[1][0] <= now_ts - HIST_SEC:
            hist.popleft()

        bid, ask = q.get("bid") or 0, q.get("ask") or 0
        bs, asz = q.get("bid_size"), q.get("ask_size")
        if bid and ask and bs and asz and bs / asz >= BOOK_RATIO:
            self.book_since.setdefault(t, now_ts)
        else:
            self.book_since.pop(t, None)

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
        if not (bid and ask and ask >= bid):
            return None
        spread = (ask - bid) / last * 100
        if spread > MAX_SPREAD:
            return None
        day = q.get("change_percent")
        prev = q.get("prev_close")
        if day is None and prev:
            day = (last / prev - 1) * 100
        if day is None or not (MIN_DAY_CHANGE <= day <= MAX_DAY_CHANGE):
            return None
        high = q.get("high") or last

        c = {"t": t, "now": now_ts, "last": last, "vol": vol, "day": day, "high": high,
             "spread": spread, "w5": self._win(t, WINDOW_SEC, now_ts, last, vol, a)}
        res = None
        for fn in (self._p_momentum, self._p_breakout, self._p_pullback, self._p_book):
            res = fn(c)
            if res:
                break
        if not res:
            return None

        w5 = c["w5"]
        move = w5[0] if w5 else None
        burst = w5[1] if w5 else None
        stop = res.get("stop") or last * (1 - STOP_PCT / 100)
        target = last * (1 + TARGET_PCT / 100)
        self.last_alert[t] = now_ts
        self.alert_count += 1
        self.active.append({
            "date": str(now_dt.date()), "time": now_dt.strftime("%H:%M:%S"), "sym": t,
            "pattern": res["pattern"],
            "price": last, "move": move, "day": day, "burst": burst, "spread": spread,
            "bid_size": q.get("bid_size"), "ask_size": q.get("ask_size"),
            "index": self.index_change, "stop": stop, "target": target, "ts": now_ts,
            "hi": last, "lo": last, "last": last, "p15": None, "p30": None, "p60": None,
            "outcome": None, "closed": False, "ev": set(),
        })
        stop_pct = (1 - stop / last) * 100
        burst_txt = f"İşlem hacmi: normalin {burst:.1f} katı\n" if burst is not None else ""
        return (
            f"{res['title']}\n"
            + "\n".join(res["lines"]) + "\n"
            f"Şu anki fiyat: {last:.2f} TL\n"
            f"Bugün toplam: %{day:+.1f}\n"
            f"{burst_txt}"
            f"Alış fiyatı: {bid:.2f} | Satış fiyatı: {ask:.2f} (aradaki fark %{spread:.1f})\n"
            f"Günün en yüksek fiyatı: {high:.2f}\n"
            f"Alım için uygun aralık: {ask:.2f} - {ask * (1 + ENTRY_BAND_PCT / 100):.2f} TL "
            f"(bunun üstüne çıkarsa peşinden koşma)\n"
            f"Örnek zarar durdur: {stop:.2f} TL (-%{stop_pct:.1f})\n"
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
                    "pattern": r["pattern"],
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
            f"{r['sym']} [{PATTERN_NAMES.get(r['pattern'], r['pattern'])}] (uyarı saati {r['time'][:5]}): şu an %{r['ret_end']}"
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
    lines.append("")
    lines.append("Kalıplara göre (hangisi işe yarıyor):")
    for key, name in PATTERN_NAMES.items():
        rs = [r for r in rows if (r.get("pattern") or "ivme") == key]
        if not rs:
            continue
        h = sum(1 for r in rs if r["outcome"] == "hedef")
        st = sum(1 for r in rs if r["outcome"] == "stop")
        lines.append(f"{name}: {len(rs)} uyarı | hedef {h} | zarar durdur {st} | 60 dk sonra ort. "
                     + show(mean(vals("ret60", rs))))
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
        send(text if len(text) <= 4000 else text[:3990] + "\n…(kısaltıldı)")
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
    safe_send(f"🔴 Canlı takip bitti\nYükseliş uyarısı: {engine.alert_count} | Büyük alım bildirimi: {engine.block_count} | Tavan olan hisse: {engine.limit_count}\n" + summary_text(rows))


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
            lines.append("Hiç veri gelmeyenler: " + ", ".join(yok[:25]) + (f" … (+{len(yok) - 25})" if len(yok) > 25 else ""))
        if derinliksiz:
            lines.append("Alış/satış fiyatı boş gelenler (bu hisselerde sinyal verilmez): " + ", ".join(derinliksiz[:25])
                     + (f" … (+{len(derinliksiz) - 25})" if len(derinliksiz) > 25 else ""))
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
