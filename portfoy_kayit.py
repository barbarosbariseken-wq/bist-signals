"""Her iş günü kapanıştan sonra çalışır: PORTFOY secret'ındaki portföyü defterle karşılaştırır.
Değişiklik varsa (hisse eklendi/çıkarıldı/lot değişti) Telegram'a haber verir ve şifreli defteri günceller.
Değişiklik yoksa hiçbir şey yazmaz."""
import os

from sinyal import now_tr, send
from portfoy import parse_portfolio
import portfoy_defter as dfr


def run():
    if not os.environ.get("SNAPSHOT_KEY", "").strip():
        send("⚠️ Portföy defteri için SNAPSHOT_KEY secret'ı yok.\n"
             "GitHub → Settings → Secrets → New secret → adı SNAPSHOT_KEY, değeri uzun bir parola (en az 12 karakter).")
        return
    pf = parse_portfolio(os.environ.get("PORTFOY", ""))
    cur = dfr.holdings(pf)
    if not cur:
        print("portföyde lotu yazılı hisse yok")
        return
    today = now_tr().strftime("%Y-%m-%d")
    ledger = dfr.load()
    last = dfr.latest_before(ledger, today)
    if last is None:
        ledger[today] = cur
        dfr.save(ledger)
        body = "\n".join(f"- {s}: {q:,.0f} lot" for s, q in sorted(cur.items()))
        send(f"📒 Portföy defteri başladı ({today}). Kayıtlı portföy:\n{body}\n\n"
             "Bundan sonra hisse ekler/çıkarır ya da lot değiştirirsen haber veririm.")
        return
    if last[1] == cur:
        print("değişiklik yok")
        return
    ledger[today] = cur
    dfr.save(ledger)
    send("📒 Portföy değişikliği kaydedildi (" + today + "):\n" + "\n".join(dfr.diff_text(last[1], cur)) +
         "\n\nHaftalık rapor bu tarihten itibaren yeni portföye göre hesaplanır.")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        print("HATA:", repr(e))
        try:
            send(f"⚠️ Portföy defteri hata verdi: {type(e).__name__}: {e}")
        except Exception:
            pass
        raise
