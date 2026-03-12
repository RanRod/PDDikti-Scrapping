import time
import requests
import pandas as pd
import re

BASE_URL = "https://api-pddikti.kemdiktisaintek.go.id/v2/pt/search/filter"

HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": "Mozilla/5.0",
    "Origin": "https://pddikti.kemdiktisaintek.go.id",
    "Referer": "https://pddikti.kemdiktisaintek.go.id/",
}

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

def safe_filename(text: str) -> str:
    text = text.strip().lower()
    text = text.replace("prov. ", "")
    text = text.replace(" ", "_")
    text = re.sub(r"[^a-z0-9_\.]", "", text)
    return text

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

    # ambil page pertama untuk tahu totalPages
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

    # lanjut page 2 s.d. total_pages
    for page in range(2, total_pages + 1):
        payload = fetch_page(session=session, page=page, provinsi=provinsi)

        if payload is None:
            print(f"Stop: gagal ambil payload di {provinsi} page {page}")
            break

        items = payload.get("data") or []
        print(f"Jumlah item page {page}: {len(items)}")

        all_rows.extend(items)
        time.sleep(1)

    df = pd.DataFrame(all_rows)
    return df

# =========================
# JALANKAN SCRAPING
# =========================
hasil_per_provinsi = {}
gabungan_list = []

for provinsi in provinsi_list:
    print("\n\n" + "#" * 100)
    print(f"Mulai scraping: {provinsi}")
    print("#" * 100)

    df_prov = scrape_province_raw_full(provinsi)

    hasil_per_provinsi[provinsi] = df_prov
    gabungan_list.append(df_prov)

    # simpan CSV per provinsi
    filename = f"pddikti_{safe_filename(provinsi)}_raw.csv"
    df_prov.to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"File per provinsi tersimpan: {filename}")
    print(f"Total baris {provinsi}: {len(df_prov)}")

# gabungkan semua provinsi
if gabungan_list:
    df_all = pd.concat(gabungan_list, ignore_index=True)
else:
    df_all = pd.DataFrame()


df_all
