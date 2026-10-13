"""Portföy defteri: portföyündeki değişiklikleri (hangi gün hangi hisseden kaç lot) ŞİFRELİ bir dosyada tutar.
Repo herkese açık olduğu için dosya düz yazı değildir; SNAPSHOT_KEY secret'ındaki parola olmadan okunamaz.
Defterde yalnızca değişikliğin olduğu günler yazılır: "bu günün sonunda portföy şuydu"."""
import base64
import hashlib
import json
import os

from cryptography.fernet import Fernet, InvalidToken

FILE = "portfoy_defter.enc"
KEEP_DAYS = 200


def _fernet():
    pw = os.environ.get("SNAPSHOT_KEY", "").strip()
    if not pw:
        return None
    key = hashlib.pbkdf2_hmac("sha256", pw.encode(), b"bist-portfoy-defter", 200_000)
    return Fernet(base64.urlsafe_b64encode(key))


def load():
    """{ 'YYYY-MM-DD': {SEMBOL: lot} }. Dosya yoksa boş. Parola yanlışsa ValueError."""
    f = _fernet()
    if f is None:
        raise ValueError("SNAPSHOT_KEY secret'ı yok")
    if not os.path.exists(FILE):
        return {}
    with open(FILE, "rb") as fh:
        raw = fh.read()
    if not raw.strip():
        return {}
    try:
        return json.loads(f.decrypt(raw).decode())
    except InvalidToken:
        raise ValueError("SNAPSHOT_KEY yanlış: defter açılamadı")


def save(ledger):
    f = _fernet()
    days = sorted(ledger)[-KEEP_DAYS:]
    data = json.dumps({d: ledger[d] for d in days}, sort_keys=True).encode()
    with open(FILE, "wb") as fh:
        fh.write(f.encrypt(data))


def holdings(pf):
    """parse_portfolio çıktısından {SEMBOL: lot} (lotu yazılmayanlar atlanır)."""
    out = {}
    for h in pf:
        if h.get("qty"):
            out[h["sym"]] = out.get(h["sym"], 0) + h["qty"]
    return out


def latest_before(ledger, day):
    """day'den (dahil) önceki son kayıt: (tarih, {sembol: lot}) ya da None."""
    ds = [d for d in ledger if d <= day]
    return (max(ds), ledger[max(ds)]) if ds else None


def holdings_at(ledger, day, base=None):
    """'day' günü kapanışındaki portföy. Kayıt yoksa base (hafta başı portföyü), o da yoksa en eski kayıt."""
    r = latest_before(ledger, day)
    if r:
        return r[1]
    if base is not None:
        return base
    if ledger:
        return ledger[min(ledger)]
    return {}


def diff_text(old, new):
    """İki portföy arasındaki farkı okunur satırlara çevirir."""
    lines = []
    for s in sorted(set(old) | set(new)):
        a, b = old.get(s, 0), new.get(s, 0)
        if a == b:
            continue
        if a == 0:
            lines.append(f"➕ {s} eklendi ({b:,.0f} lot)")
        elif b == 0:
            lines.append(f"➖ {s} çıkarıldı (önceki {a:,.0f} lot)")
        else:
            lines.append(f"🔁 {s}: {a:,.0f} → {b:,.0f} lot")
    return lines
