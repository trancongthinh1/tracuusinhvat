# -*- coding: utf-8 -*-
"""
Tạo file sealifebase.json cho trang "Tra cứu các loài sinh vật biển Việt Nam".

Lấy cho mỗi loài trong data.json:
  - Phân bố thế giới  (mục Distribution trên trang FishBase / SeaLifeBase)
  - Hình thái         (mục Morphology / Short description)
  - Đặc điểm sinh học (mục Biology)
  - Kích thước, độ sâu, môi trường sống

Nguồn: bản dữ liệu mở chính thức của FishBase (cá) và SeaLifeBase (sinh vật khác),
do nhóm rOpenSci/rfishbase phát hành công khai trên source.coop (cùng dữ liệu với
các trang fishbase.se và sealifebase.ca). Không cào trang web.

Cách chạy (trên máy có Internet, Python 3.9+):
    pip install duckdb
    python lay_du_lieu_sealifebase.py
Chạy trong thư mục có data.json; kết quả: sealifebase.json đặt cạnh index.html.
Tuỳ chọn:
    --data đường_dẫn/data.json   --out đường_dẫn/sealifebase.json
    --fb v24.07 --slb v24.07     (chọn bản dữ liệu; mặc định: bản mới nhất)
"""
import argparse, json, re, sys, urllib.request, html

S3_LIST = "https://s3.us-west-2.amazonaws.com/us-west-2.opendata.source.coop?list-type=2&prefix=cboettig/fishbase/{sv}/&delimiter=/"
PARQUET = "https://data.source.coop/cboettig/fishbase/{sv}/{ver}/parquet/{table}.parquet"
SITE = {"fb": ("FishBase", "https://www.fishbase.se/summary/{g}-{s}.html"),
        "slb": ("SeaLifeBase", "https://www.sealifebase.ca/summary/{g}-{s}.html")}
FISH = {"actinopterygii", "actinopteri", "elasmobranchii", "chondrichthyes", "holocephali", "osteichthyes",
        "myxini", "petromyzonti", "teleostei", "sarcopterygii", "leptocardii"}
HABITAT = {"reef-associated": "sống ở rạn", "demersal": "sống đáy", "pelagic-neritic": "sống tầng nổi ven bờ",
           "pelagic-oceanic": "sống tầng nổi đại dương", "benthopelagic": "sống tầng đáy – tầng nổi",
           "bathydemersal": "sống đáy biển sâu", "bathypelagic": "sống tầng nổi biển sâu", "pelagic": "sống tầng nổi",
           "sessile": "sống bám", "benthic": "sống đáy"}


def latest(sv):
    with urllib.request.urlopen(S3_LIST.format(sv=sv), timeout=60) as r:
        xml = r.read().decode()
    vers = sorted(set(re.findall(r"/(v\d{2}\.\d{2})/", xml)))
    if not vers:
        sys.exit(f"Không tìm thấy bản dữ liệu nào cho {sv} trên source.coop")
    return vers[-1]


def clean(t):
    if t is None:
        return ""
    t = html.unescape(str(t))
    t = re.sub(r"<[^>]+>", "", t)
    return re.sub(r"\s+", " ", t).strip()


def num(a, b=None):
    def f(x):
        if x is None:
            return None
        x = float(x)
        return str(int(x)) if x.is_integer() else f"{x:g}"
    a, b = f(a), f(b)
    if a and b and a != b:
        return f"{a}–{b}"
    return a or b


class Tables:
    def __init__(self, con, sv, ver, base):
        self.con, self.sv, self.ver, self.base = con, sv, ver, base

    def url(self, table):
        return (self.base.rstrip("/") + f"/{table}.parquet") if self.base else PARQUET.format(sv=self.sv, ver=self.ver, table=table)

    def load(self, table, want):
        """Đọc các cột cần (không phân biệt hoa thường); cột thiếu -> None."""
        u = self.url(table)
        try:
            cols = [r[0] for r in self.con.execute(f"DESCRIBE SELECT * FROM read_parquet('{u}')").fetchall()]
        except Exception as e:
            print(f"  ! không đọc được bảng {table} ({self.sv}): {str(e).splitlines()[0]}")
            return []
        low = {c.lower(): c for c in cols}
        sel = ", ".join(f'"{low[w.lower()]}" AS "{w}"' if w.lower() in low else f'NULL AS "{w}"' for w in want)
        rows = self.con.execute(f"SELECT {sel} FROM read_parquet('{u}')").fetchall()
        miss = [w for w in want if w.lower() not in low]
        if miss:
            print(f"  (bảng {table} của {self.sv} không có cột: {', '.join(miss)})")
        return [dict(zip(want, r)) for r in rows]


def build(con, sv, ver, base):
    print(f"Đang đọc {SITE[sv][0]} {ver} …")
    T = Tables(con, sv, ver, base)
    sp = T.load("species", ["SpecCode", "Genus", "Species", "Comments", "Length", "LTypeMaxM", "CommonLength",
                            "Weight", "DemersPelag", "DepthRangeShallow", "DepthRangeDeep", "Fresh", "Brack", "Saltwater"])
    stocks = T.load("stocks", ["SpecCode", "StockCode", "StockDefs", "Level"])
    morph = T.load("morphdat", ["Speccode", "StockCode", "AddChars", "DorsalSpinesMin", "DorsalSpinesMax",
                                "DorsalSoftRaysMin", "DorsalSoftRaysMax", "AnalSpinesMin", "AnalSpinesMax",
                                "AnalSoftRaysMin", "AnalSoftRaysMax", "VertebraeTotalMin", "VertebraeTotalMax"])
    syn = T.load("synonyms", ["SpecCode", "SynGenus", "SynSpecies"])
    byname, info = {}, {}
    for r in sp:
        if r["Genus"] and r["Species"]:
            byname[f"{r['Genus']} {r['Species']}".lower()] = r["SpecCode"]
            info[r["SpecCode"]] = r
    synname = {}
    for r in syn:
        if r["SynGenus"] and r["SynSpecies"]:
            synname.setdefault(f"{r['SynGenus']} {r['SynSpecies']}".lower(), r["SpecCode"])
    dist = {}
    for r in sorted(stocks, key=lambda r: (str(r.get("Level") or "").lower() != "species in general", r.get("StockCode") or 0)):
        t = clean(r["StockDefs"])
        if t and r["SpecCode"] not in dist:
            dist[r["SpecCode"]] = t
    mor = {}
    for r in morph:
        if r["Speccode"] in mor:
            continue
        parts = []
        for lab, a, b in [("Gai vây lưng", "DorsalSpinesMin", "DorsalSpinesMax"), ("Tia mềm vây lưng", "DorsalSoftRaysMin", "DorsalSoftRaysMax"),
                          ("Gai vây hậu môn", "AnalSpinesMin", "AnalSpinesMax"), ("Tia mềm vây hậu môn", "AnalSoftRaysMin", "AnalSoftRaysMax"),
                          ("Đốt sống", "VertebraeTotalMin", "VertebraeTotalMax")]:
            v = num(r[a], r[b])
            if v:
                parts.append(f"{lab}: {v}")
        txt = "; ".join(parts)
        add = clean(r["AddChars"])
        if add:
            txt = (txt + ". " if txt else "") + add
        if txt:
            mor[r["Speccode"]] = txt
    return byname, synname, info, dist, mor


def facts(r):
    out = []
    L = num(r.get("Length"))
    if L:
        out.append(f"Chiều dài tối đa: {L} cm" + (f" ({r['LTypeMaxM']})" if r.get("LTypeMaxM") else ""))
    C = num(r.get("CommonLength"))
    if C:
        out.append(f"chiều dài thường gặp: {C} cm")
    W = num(r.get("Weight"))
    if W:
        out.append(f"khối lượng tối đa: {W} g")
    D = num(r.get("DepthRangeShallow"), r.get("DepthRangeDeep"))
    if D:
        out.append(f"độ sâu: {D} m")
    env = [n for k, n in (("Saltwater", "biển"), ("Brack", "nước lợ"), ("Fresh", "nước ngọt")) if r.get(k) in (1, -1, True, "-1", "1")]
    if env:
        out.append("môi trường: " + ", ".join(env))
    hp = str(r.get("DemersPelag") or "").strip().lower()
    if hp:
        out.append(HABITAT.get(hp, hp))
    s = "; ".join(out)
    return s[:1].upper() + s[1:] if s else ""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", default="data.json")
    ap.add_argument("--out", default="sealifebase.json")
    ap.add_argument("--fb")
    ap.add_argument("--slb")
    ap.add_argument("--base-fb", help=argparse.SUPPRESS)   # thư mục parquet cục bộ (dùng để thử)
    ap.add_argument("--base-slb", help=argparse.SUPPRESS)
    a = ap.parse_args()
    try:
        import duckdb
    except ImportError:
        sys.exit("Cần cài duckdb trước:  pip install duckdb")
    con = duckdb.connect()
    if not (a.base_fb and a.base_slb):
        con.execute("INSTALL httpfs; LOAD httpfs;")
    db = json.load(open(a.data, encoding="utf-8"))
    src = {}
    for sv, ver, base in (("fb", a.fb, a.base_fb), ("slb", a.slb, a.base_slb)):
        ver = ver or ("local" if base else latest(sv))
        src[sv] = (ver,) + build(con, sv, ver, base)
    out, found = {}, {"fb": 0, "slb": 0}
    for d in db:
        names = [d["k"]] + [" ".join(re.sub(r"\(\w+\)\s*", "", s).split()[:2]).lower() for s in d.get("syn", [])]
        fish = (d.get("cls") or "").lower() in FISH or "dmcb" in d.get("src", {})
        for sv in (("fb", "slb") if fish else ("slb", "fb")):
            ver, byname, synname, info, dist, mor = src[sv]
            code = next((byname[n] for n in names if n in byname), None) or next((synname[n] for n in names if n in synname), None)
            if code is None or code not in info:
                continue
            r = info[code]
            rec = {"db": SITE[sv][0], "v": ver,
                   "u": SITE[sv][1].format(g=r["Genus"], s=r["Species"]),
                   "pb": dist.get(code, ""), "ht": mor.get(code, ""), "sh": clean(r.get("Comments")), "kt": facts(r)}
            rec = {k: v for k, v in rec.items() if v}
            if len(rec) > 3:
                out[d["k"]] = rec
                found[sv] += 1
            break
    json.dump(out, open(a.out, "w", encoding="utf-8"), ensure_ascii=False, separators=(",", ":"))
    print(f"Xong: {len(out)} / {len(db)} loài có dữ liệu (FishBase: {found['fb']}, SeaLifeBase: {found['slb']}).")
    print(f"Đã ghi {a.out}. Đặt file này cạnh index.html và data.json rồi tải lại trang.")


if __name__ == "__main__":
    main()
