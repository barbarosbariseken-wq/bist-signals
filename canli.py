"""Canlı takip: TradingView akışına (borsapy) bağlanır, hızlı yükselen hisseleri anında bildirir
ve her uyarının sonrasını kaydeder (ileri test), böylece stratejinin işe yarayıp yaramadığı ölçülür."""
import csv
import math
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
MAX_ALERTS_PER_SESSION = 10
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
                 "geri_donus": "Düşüşten dönüş", "tahta": "Alıcı baskısı",
                 "hacim_oncu": "Hacim öncüsü"}
# --- hacim öncüsü: fiyat henüz az oynamışken hacmin hızlandığını ve alıcıların baskın olduğunu yakalar ---
VOL_PRE_BURST5 = 3.0       # son 5 dk hacmi normalin en az 3 katı
VOL_PRE_BURST2 = 3.0       # son 2 dk hacmi de en az 3 kat ve 5 dk'lık hızdan düşük değil (hızlanıyor)
VOL_PRE_UPSHARE = 0.60     # işlem hacminin en az %60'ı yükselen fiyatta (alıcı yönlü) gerçekleşmiş
VOL_PRE_MIN_MOVE = 0.2     # son 5 dk'da en az %0,2 yükselmiş (hareket başlıyor) ...
                           # ... ama MIN_WINDOW_MOVE'dan az: büyük hareket zaten "Hızlı yükseliş" uyarısı olur
VOL_PRE_NEAR_HIGH = 0.985  # fiyat günün zirvesinin en az %98,5'inde
VOL_PRE_MAX_SPREAD = 0.6   # alış-satış farkı en fazla %0,6 (hareket küçükken maliyet yemesin)
UPSHARE_WIN = 300          # alıcı yönlü hacim oranı için pencere (sn)
LEARN_FILE = "ogrenilen.json"  # gün sonu raporunun çıkardığı filtreler (gunsonu.py yazar)
MAX_SHADOW_PER_SESSION = 30    # filtrelenen (gönderilmeyen) ama izlenen uyarıların üst sınırı
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
              "mfe", "mae", "outcome", "b2", "upshare", "day_share", "shadow"]

CLOCK = time.time


def pct(a, b):
    if a is None or not b:
        return None
    return (a / b - 1) * 100


def f2(x):
    return "" if x is None else f"{x:.2f}"


class Engine:
    def __init__(self, avg, out, clock=None, portfolio=(), learned=None):
        self.portfolio = set(portfolio)
        self.learned = learned or {}
        self.shadow_count = 0
        names = list(TICKERS) + [p for p in self.portfolio if p not in TICKERS]
        self.avg = avg
        self.out = out
        self.clock = clock or (lambda: CLOCK())
        self.cache = {t: {} for t in names}
        self.hist = {t: deque() for t in names}
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
            if not r.get("shadow"):
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

    @staticmethod
    def _tick(p):
        for top, step in ((20, 0.01), (50, 0.02), (100, 0.05), (250, 0.10), (500, 0.25),
                          (1000, 0.50), (2500, 1.0)):
            if p < top:
                return step
        return 2.5

    def _limit_check(self, t):
        """Tavan (tüm liste) ve taban (sadece portföy) takibi. Kuyruk: tavanda bekleyen alış (bid_size),
        tabanda bekleyen satış (ask_size). Kuyruk, görülen en yüksek değerinin %50'sine ve %25'ine inince uyarır.
        Veri gecikmeliyse uyarı da gecikir."""
        q = self.cache[t]
        last, prev = q.get("last"), q.get("prev_close")
        if not last or not prev:
            return []
        now_dt = now_tr()
        mins = now_dt.hour * 60 + now_dt.minute
        if not (LIMIT_START_MIN <= mins < LIMIT_END_MIN):
            return []
        mine = t in self.portfolio
        day = (last / prev - 1) * 100
        msgs = []
        sides = [("tavan", 1)] + ([("taban", -1)] if mine else [])
        for side, sgn in sides:
            tick = self._tick(prev * (1 + sgn * LIMIT_PCT / 100))
            if sgn > 0:
                level = int(prev * (1 + LIMIT_PCT / 100) / tick + 1e-6) * tick
                at = last >= level - tick * 0.5 and day >= LIMIT_MIN_DAY
                qty = q.get("bid_size")
            else:
                level = math.ceil(prev * (1 - LIMIT_PCT / 100) / tick - 1e-6) * tick  # yukarı yuvarla
                at = last <= level + tick * 0.5 and day <= -LIMIT_MIN_DAY
                qty = q.get("ask_size")
            msgs.extend(self._limit_side(t, side, at, qty, last, day, mine))
        return msgs

    def _limit_side(self, t, side, at, qty, last, day, mine):
        now_ts = self.clock()
        key = (t, side)
        st = self.limit.get(key)
        who = " (portföyünde)" if mine else ""
        if not at:
            if st is not None:
                if st["off_since"] is None:
                    st["off_since"] = now_ts
                elif now_ts - st["off_since"] >= LIMIT_LEAVE_SEC:
                    del self.limit[key]  # bozuldu; tekrar kilitlenirse yeni bildirim gelir
            return []
        what = "alış" if side == "tavan" else "satış"
        if st is None:
            if not mine and self.limit_count >= MAX_LIMIT_MSGS:
                return []
            st = self.limit[key] = {"peak": 0.0, "since": now_ts, "off_since": None, "s50": False, "s75": False}
            if not mine:
                self.limit_count += 1
            icon = "🔒" if side == "tavan" else "🧊"
            if qty:
                st["peak"] = float(qty)
                qtxt = (f"{side.capitalize()}da bekleyen {what}: {qty:,.0f} adet (yaklaşık {qty * last / 1e6:.1f} milyon TL)\n"
                        f"Bu kuyruk %50 ve %75 eridiğinde ayrıca haber vereceğim.")
            else:
                qtxt = f"{side.capitalize()}da bekleyen {what} miktarı verisi gelmiyor, kuyruk uyarısı yapılamaz."
            return [f"{icon} {t}{who} {side} oldu\nFiyat: {last:.2f} TL (bugün %{day:+.1f})\n{qtxt}"]
        st["off_since"] = None
        if not qty:
            return []
        qty = float(qty)
        if qty > st["peak"]:
            st["peak"] = qty
            return []
        peak = st["peak"]
        if peak * last < LIMIT_MIN_QUEUE_TL:
            return []
        ratio = qty / peak
        if side == "tavan":
            note = "Alıcılar çekilmiş ya da satışlar karşılanmış olabilir."
            t50 = f"⚠️ {t}{who}: tavandaki alış kuyruğu %50 azaldı"
            t75 = f"🚨 {t}{who}: tavandaki alış kuyruğu %75 azaldı, tavan bozulabilir"
        else:
            note = "Satıcılar çekilmiş ya da alımlar karşılamış olabilir."
            t50 = f"⚠️ {t}{who}: tabandaki satış kuyruğu %50 azaldı"
            t75 = f"🚨 {t}{who}: tabandaki satış kuyruğu %75 azaldı, taban bozulabilir"
        head = (f"Fiyat: {last:.2f} TL (bugün %{day:+.1f})\n"
                f"Takip ettiğim en yüksek kuyruk: {peak:,.0f} adet → şimdi {qty:,.0f} adet "
                f"(%{(1 - ratio) * 100:.0f} azaldı)\n{note}")
        if ratio <= LIMIT_DROP_2 and not st["s75"]:
            st["s75"] = st["s50"] = True
            return [t75 + "\n" + head]
        if ratio <= LIMIT_DROP_1 and not st["s50"]:
            st["s50"] = True
            return [t50 + "\n" + head]
        return []

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

    def _upshare(self, t, now_ts, secs=UPSHARE_WIN):
        """Son 'secs' sn'deki hacmin ne kadarı fiyat yükselirken (alıcı yönlü) gerçekleşti (0-1).
        Yöntem: hacim artışı, fiyat bir önceki güncellemeye göre arttıysa alıcıya, azaldıysa satıcıya yazılır;
        fiyat değişmediyse son yöne yazılır (yön yoksa yarı yarıya). Kesin veri değil, tahmindir."""
        samples = [s for s in self.hist[t] if s[0] >= now_ts - secs]
        if len(samples) < 6:
            return None
        up = down = 0.0
        sign = 0
        for a, b in zip(samples, samples[1:]):
            dv = b[2] - a[2]
            if dv <= 0:
                continue
            dp = b[1] - a[1]
            if dp > 0:
                sign = 1
            elif dp < 0:
                sign = -1
            if sign > 0:
                up += dv
            elif sign < 0:
                down += dv
            else:
                up += dv / 2
                down += dv / 2
        tot = up + down
        return up / tot if tot > 0 else None

    def _learn_block(self, res, c):
        """Gün sonu raporunun öğrendiği filtreler. Engelliyorsa nedeni döner (uyarı gönderilmez ama izlenir)."""
        L = self.learned
        if not L:
            return None
        if res["pattern"] in L.get("kapali_kaliplar", []):
            return "kalıp kapalı"
        w5 = c["w5"]
        burst = w5[1] if w5 else None
        if L.get("min_burst") and burst is not None and burst < L["min_burst"]:
            return f"hacim artışı {burst:.1f} < {L['min_burst']}"
        if L.get("max_spread") and c["spread"] > L["max_spread"]:
            return f"alış-satış farkı {c['spread']:.1f} > {L['max_spread']}"
        if L.get("min_upshare") and c["upshare"] is not None and c["upshare"] < L["min_upshare"]:
            return f"alıcı yönlü oran {c['upshare']:.2f} < {L['min_upshare']}"
        return None

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

    def _p_volume(self, c):
        w5, w2 = c["w5"], c["w2"]
        if not w5 or not w2 or c["upshare"] is None:
            return None
        move5, b5 = w5[0], w5[1]
        move2, b2 = w2[0], w2[1]
        if b5 < VOL_PRE_BURST5 or b2 < VOL_PRE_BURST2 or b2 < b5:
            return None
        if c["w15"] and b5 < c["w15"][1]:
            return None           # hacim son 15 dk'ya göre de artıyor olmalı
        if c["upshare"] < VOL_PRE_UPSHARE:
            return None
        if not (VOL_PRE_MIN_MOVE <= move5 < MIN_WINDOW_MOVE) or move2 < 0:
            return None
        if c["last"] < VOL_PRE_NEAR_HIGH * c["high"]:
            return None
        if c["spread"] > VOL_PRE_MAX_SPREAD:
            return None
        return {"pattern": "hacim_oncu", "title": f"🔊 {c['t']}: hacim hızla artıyor, yükseliş başlıyor olabilir",
                "lines": [f"Son 2 dakikada hacim normalin {b2:.1f} katı, son 5 dakikada {b5:.1f} katı (hızlanıyor)",
                          f"İşlemlerin yaklaşık %{c['upshare'] * 100:.0f}'i alıcı yönlü",
                          f"Fiyat henüz yalnızca %{move5:+.1f} oynadı (hareket yeni başlıyor)"]}

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
             "spread": spread, "w5": self._win(t, WINDOW_SEC, now_ts, last, vol, a),
             "w2": self._win(t, 120, now_ts, last, vol, a), "w15": self._win(t, 900, now_ts, last, vol, a),
             "upshare": self._upshare(t, now_ts), "day_share": vol / a["vol"] if a["vol"] else None}
        res = None
        for fn in (self._p_momentum, self._p_breakout, self._p_pullback, self._p_book, self._p_volume):
            res = fn(c)
            if res:
                break
        if not res:
            return None
        shadow = self._learn_block(res, c) is not None
        if shadow:
            if self.shadow_count >= MAX_SHADOW_PER_SESSION:
                return None
            self.shadow_count += 1

        w5 = c["w5"]
        move = w5[0] if w5 else None
        burst = w5[1] if w5 else None
        stop = res.get("stop") or last * (1 - STOP_PCT / 100)
        target = last * (1 + TARGET_PCT / 100)
        self.last_alert[t] = now_ts
        if not shadow:
            self.alert_count += 1
        self.active.append({
            "date": str(now_dt.date()), "time": now_dt.strftime("%H:%M:%S"), "sym": t,
            "pattern": res["pattern"],
            "price": last, "move": move, "day": day, "burst": burst, "spread": spread,
            "bid_size": q.get("bid_size"), "ask_size": q.get("ask_size"),
            "index": self.index_change, "stop": stop, "target": target, "ts": now_ts,
            "hi": last, "lo": last, "last": last, "p15": None, "p30": None, "p60": None,
            "outcome": None, "closed": False, "ev": set(),
            "b2": c["w2"][1] if c["w2"] else None, "upshare": c["upshare"], "day_share": c["day_share"],
            "shadow": shadow,
        })
        if shadow:
            return None
        stop_pct = (1 - stop / last) * 100
        burst_txt = f"İşlem hacmi: normalin {burst:.1f} katı\n" if burst is not None else ""
        det = []
        if c["w2"]:
            det.append(f"son 2 dk {c['w2'][1]:.1f} kat")
        if burst is not None:
            det.append(f"son 5 dk {burst:.1f} kat")
        if c["w15"]:
            det.append(f"son 15 dk {c['w15'][1]:.1f} kat")
        if c["upshare"] is not None:
            det.append(f"işlemlerin %{c['upshare'] * 100:.0f}'i alıcı yönlü")
        if c["day_share"] is not None:
            det.append(f"bugünkü hacim normal günün %{c['day_share'] * 100:.0f}'i")
        if det:
            burst_txt += "Hacim ayrıntısı: " + " | ".join(det) + "\n"
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
                    "b2": f2(r.get("b2")), "upshare": f2(r.get("upshare")), "day_share": f2(r.get("day_share")),
                    "shadow": "1" if r.get("shadow") else "",
                })
            return rows


def write_log(rows):
    if not rows:
        return
    new = not os.path.exists(LOG_FILE)
    if not new:
        with open(LOG_FILE, encoding="utf-8", newline="") as f:
            rd = csv.DictReader(f)
            header = rd.fieldnames or []
            old = list(rd) if header != LOG_FIELDS else None
        if old is not None:   # eski sütun düzeni: yeni alanlarla baştan yaz (eski satırlar boş alan alır)
            with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=LOG_FIELDS, extrasaction="ignore", restval="")
                w.writeheader()
                w.writerows(old)
    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=LOG_FIELDS, extrasaction="ignore", restval="")
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
        allrows = list(csv.DictReader(f))
    rows = [r for r in allrows if not r.get("shadow")]
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
    if len(allrows) > n:
        lines.append(f"Öğrenilen filtreler yüzünden gönderilmeyen ama izlenen uyarı: {len(allrows) - n} adet.")
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


def load_learned(path=None):
    """ogrenilen.json'u okur ve sınırlar içine çeker (filtreler sadece sıkılaştırabilir, sınırlıdır)."""
    path = path or LEARN_FILE
    try:
        import json
        with open(path, encoding="utf-8") as f:
            d = json.load(f)
    except (OSError, ValueError):
        return {}
    out = {}
    try:
        if d.get("min_burst"):
            out["min_burst"] = min(max(float(d["min_burst"]), MIN_VOL_BURST), 6.0)
        if d.get("max_spread"):
            out["max_spread"] = min(max(float(d["max_spread"]), 0.4), MAX_SPREAD)
        if d.get("min_upshare"):
            out["min_upshare"] = min(max(float(d["min_upshare"]), 0.0), 0.65)
        kp = [k for k in d.get("kapali_kaliplar", []) if k in PATTERN_NAMES]
        if kp:
            out["kapali_kaliplar"] = kp
    except (TypeError, ValueError):
        return {}
    return out


def learned_text(L):
    if not L:
        return ""
    parts = []
    if L.get("kapali_kaliplar"):
        parts.append("kapalı kalıp: " + ", ".join(PATTERN_NAMES[k] for k in L["kapali_kaliplar"]))
    if L.get("min_burst"):
        parts.append(f"hacim artışı en az {L['min_burst']:g} kat")
    if L.get("max_spread"):
        parts.append(f"alış-satış farkı en fazla %{L['max_spread']:g}")
    if L.get("min_upshare"):
        parts.append(f"alıcı yönlü oran en az %{L['min_upshare'] * 100:.0f}")
    return "Öğrenilen filtreler: " + "; ".join(parts) + "\n"


def portfolio_symbols():
    """PORTFOY secret'ından sembolleri okur (yoksa boş)."""
    try:
        from portfoy import parse_portfolio
        return [h["sym"] for h in parse_portfolio(os.environ.get("PORTFOY", ""))]
    except Exception as e:
        print("portföy okunamadı:", repr(e))
        return []


def open_stream(bp, engine):
    stream = bp.TradingViewStream()
    stream.connect()
    stream.on_any_quote(engine.on_quote)
    for t in list(TICKERS) + [p for p in engine.portfolio if p not in TICKERS]:
        try:
            stream.subscribe(t)
        except Exception as e:
            print("abonelik başarısız:", t, repr(e))
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
    pf = portfolio_symbols()
    learned = load_learned()
    engine = Engine(avg, out, portfolio=pf, learned=learned)
    stream = open_stream(bp, engine)
    end = session_end(now_tr())
    safe_send(
        f"🟢 Canlı takip başladı ({now_tr():%H:%M})\n"
        f"Takip edilen hisse: {len(avg)} / {len(TICKERS)}"
        + (f" + portföy: {', '.join(pf)}" if pf else "") + "\n"
        f"TradingView hesabı: {auth}\n"
        + learned_text(learned) +
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
    real = [r for r in rows if not r.get("shadow")]
    extra = (f"\nÖğrenilen filtreler yüzünden gönderilmeyen ama izlenen: {len(rows) - len(real)} uyarı"
             if len(real) != len(rows) else "")
    safe_send(f"🔴 Canlı takip bitti\nYükseliş uyarısı: {engine.alert_count} | Büyük alım bildirimi: {engine.block_count} | Tavan olan hisse: {engine.limit_count}\n" + summary_text(real) + extra)


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
