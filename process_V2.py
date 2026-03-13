import time
import requests
import pandas as pd
import re

# =========================================================
# CONFIG
# =========================================================
BASE_URL = "https://api-pddikti.kemdiktisaintek.go.id/v2/pt/search/filter"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0",
    "Origin": "https://pddikti.kemdiktisaintek.go.id",
    "Referer": "https://pddikti.kemdiktisaintek.go.id/",
}

# Coba semester dari yang paling baru, stop di yang pertama valid
DEFAULT_SEMESTERS = ["20261", "20251", "20242", "20241"]

# =========================
# LIST PROVINSI PILIHAN
# =========================
provinsi_list = [
    "Prov. Aceh",
    "Prov. Sumatera Utara",
    "Prov. Sumatera Barat",
    "Prov. Riau",
    "Prov. Jambi",
    "Prov. Sumatera Selatan",
    "Prov. Bengkulu",
    "Prov. Lampung",
    "Prov. Kepulauan Bangka Belitung",
    "Prov. Kepulauan Riau",
    "Prov. Banten",
    "Prov. Jawa Barat",
    "Prov. Jawa Tengah",
    "Prov. D.I. Yogyakarta",
]

# =========================================================
# HELPER
# =========================================================
def safe_filename(text: str) -> str:
    text = text.strip().lower()
    text = text.replace("prov. ", "")
    text = text.replace(" ", "_")
    text = re.sub(r"[^a-z0-9_\.]", "", text)
    return text


# =========================================================
# TAHAP 1 - SCRAPE LIST PT
# =========================================================
def fetch_page(session, page=1, provinsi="", retries=3, sleep_retry=3):
    params = {
        "page": page,
        "akreditasi": "",
        "jenis": "",
        "provinsi": provinsi,
        "status": "",
    }

    for attempt in range(1, retries + 1):
        try:
            r = session.get(BASE_URL, headers=HEADERS, params=params, timeout=60)
            print(f"provinsi={provinsi} | page={page} | attempt={attempt} | status={r.status_code}")
            print(r.url)

            if r.status_code == 200:
                return r.json()

            print("response:", r.text[:500])

        except Exception as e:
            print(f"provinsi={provinsi} | page={page} | attempt={attempt} | error={e}")

        time.sleep(sleep_retry)

    return None


def scrape_province_raw_full(provinsi):
    session = requests.Session()
    all_rows = []

    first_payload = fetch_page(session=session, page=1, provinsi=provinsi)

    if first_payload is None:
        print(f"Gagal ambil page pertama untuk {provinsi}")
        return pd.DataFrame()

    total_pages = first_payload.get("totalPages", 0)
    total_items = first_payload.get("totalItems", 0)
    limit = first_payload.get("limit", None)
    first_items = first_payload.get("data") or []

    print("\n" + "=" * 80)
    print(f"PROVINSI   : {provinsi}")
    print(f"TOTAL PAGE : {total_pages}")
    print(f"TOTAL ITEM : {total_items}")
    print(f"LIMIT API  : {limit}")
    print(f"ITEM PAGE1 : {len(first_items)}")
    print("=" * 80)

    all_rows.extend(first_items)

    for page in range(2, total_pages + 1):
        payload = fetch_page(session=session, page=page, provinsi=provinsi)

        if payload is None:
            print(f"Stop: gagal ambil payload di {provinsi} page {page}")
            break

        items = payload.get("data") or []
        print(f"Jumlah item page {page}: {len(items)}")

        all_rows.extend(items)
        time.sleep(1)

    return pd.DataFrame(all_rows)


def build_df_all(provinsi_list):
    hasil_per_provinsi = {}
    gabungan_list = []

    for provinsi in provinsi_list:
        print("\n\n" + "#" * 100)
        print(f"Mulai scraping: {provinsi}")
        print("#" * 100)

        df_prov = scrape_province_raw_full(provinsi)

        hasil_per_provinsi[provinsi] = df_prov
        gabungan_list.append(df_prov)

        filename = f"pddikti_{safe_filename(provinsi)}_raw.csv"
        df_prov.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"File per provinsi tersimpan: {filename}")
        print(f"Total baris {provinsi}: {len(df_prov)}")

    if gabungan_list:
        df_all = pd.concat(gabungan_list, ignore_index=True)
    else:
        df_all = pd.DataFrame()

    return df_all, hasil_per_provinsi


# =========================================================
# TAHAP 2 - SCRAPE DETAIL PRODI
# =========================================================
def fetch_prodi_pt_first_valid_semester(
    id_sp: str,
    semesters=None,
    session=None,
    timeout=60,
    sleep_each_try=1,
):
    if semesters is None:
        semesters = DEFAULT_SEMESTERS

    sess = session or requests.Session()

    for semester in semesters:
        url = f"https://api-pddikti.kemdiktisaintek.go.id/pt/prodi/{id_sp}/{semester}"

        headers = HEADERS.copy()
        headers["Referer"] = f"https://pddikti.kemdiktisaintek.go.id/detail-pt/{id_sp}"

        try:
            r = sess.get(url, headers=headers, timeout=timeout)
            print(f"id_sp={id_sp} | semester={semester} | status={r.status_code}")

            if r.status_code != 200:
                time.sleep(sleep_each_try)
                continue

            data = r.json()

            if not isinstance(data, list) or len(data) == 0:
                print(f"  -> semester {semester} kosong")
                time.sleep(sleep_each_try)
                continue

            df = pd.DataFrame(data)
            df["id_sp"] = id_sp
            df["semester"] = semester
            df["detail_url"] = f"https://pddikti.kemdiktisaintek.go.id/detail-pt/{id_sp}"

            print(f"  -> pakai semester {semester}, jumlah prodi: {len(df)}")
            return df, semester

        except Exception as e:
            print(f"id_sp={id_sp} | semester={semester} | error={e}")
            time.sleep(sleep_each_try)

    return pd.DataFrame(), None


def build_df_detail_from_df_all(
    df_all: pd.DataFrame,
    semesters=None,
    sleep_each_pt=1,
    id_col="id_sp",
    nama_col="nama_pt",
):
    if df_all.empty:
        return pd.DataFrame(), pd.DataFrame()

    df_source = df_all[[id_col, nama_col]].copy()
    df_source = df_source.dropna(subset=[id_col]).drop_duplicates()

    session = requests.Session()
    detail_list = []
    log_list = []

    total = len(df_source)

    for i, row in enumerate(df_source.itertuples(index=False), start=1):
        id_sp = getattr(row, id_col)
        nama_pt = getattr(row, nama_col)

        print("\n" + "-" * 100)
        print(f"[{i}/{total}] Proses id_sp={id_sp} | nama_pt={nama_pt}")

        df_one, semester_valid = fetch_prodi_pt_first_valid_semester(
            id_sp=id_sp,
            semesters=semesters,
            session=session,
            sleep_each_try=1,
        )

        if not df_one.empty:
            df_one[nama_col] = nama_pt

            front_cols = [id_col, nama_col, "semester", "detail_url"]
            other_cols = [c for c in df_one.columns if c not in front_cols]
            df_one = df_one[front_cols + other_cols]

            detail_list.append(df_one)

        log_list.append({
            id_col: id_sp,
            nama_col: nama_pt,
            "semester_valid": semester_valid,
            "jumlah_prodi": len(df_one) if not df_one.empty else 0,
            "status_detail": "OK" if not df_one.empty else "KOSONG/GAGAL",
        })

        time.sleep(sleep_each_pt)

    if detail_list:
        df_detail = pd.concat(detail_list, ignore_index=True)
    else:
        df_detail = pd.DataFrame()

    df_detail_log = pd.DataFrame(log_list)

    return df_detail, df_detail_log


# =========================================================
# MAIN PROCESS
# =========================================================
print("\n" + "#" * 100)
print("TAHAP 1 - BUILD df_all")
print("#" * 100)

df_all, hasil_per_provinsi = build_df_all(provinsi_list)

print("\nSELESAI TAHAP 1")
print("Jumlah baris df_all:", len(df_all))
print("Kolom df_all:")
print(df_all.columns.tolist())

df_all.to_csv("pddikti_all_pt_raw.csv", index=False, encoding="utf-8-sig")
print("File tersimpan: pddikti_all_pt_raw.csv")

print("\n" + "#" * 100)
print("TAHAP 2 - BUILD df_detail dari df_all")
print("#" * 100)

df_detail, df_detail_log = build_df_detail_from_df_all(
    df_all=df_all,
    semesters=DEFAULT_SEMESTERS,
    sleep_each_pt=1,   # bisa dinaikkan kalau mau lebih sopan ke server
    id_col="id_sp",
    nama_col="nama_pt",
)

print("\nSELESAI TAHAP 2")
print("Jumlah baris df_detail:", len(df_detail))
print("Jumlah baris df_detail_log:", len(df_detail_log))

df_detail.to_csv("pddikti_detail_prodi.csv", index=False, encoding="utf-8-sig")
df_detail_log.to_csv("pddikti_detail_prodi_log.csv", index=False, encoding="utf-8-sig")

print("File tersimpan: pddikti_detail_prodi.csv")
print("File tersimpan: pddikti_detail_prodi_log.csv")

print("\nPreview df_all:")
print(df_all.head())

print("\nPreview df_detail:")
print(df_detail.head())

print("\nPreview df_detail_log:")
print(df_detail_log.head())
