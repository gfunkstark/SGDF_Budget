import os
import time
import logging
import pandas as pd
from dotenv import load_dotenv
from io import StringIO
from bs4 import BeautifulSoup

# Selenium
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

# Google
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# ======================
# CONFIG
# ======================
load_dotenv()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_DIR = os.path.join(BASE_DIR, "downloads")
LOG_DIR = os.path.join(BASE_DIR, "logs")

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

LOGIN_URL = "https://sgdf.production.sirom.net/"
TARGET_URL = "https://sgdf.production.sirom.net/recettedepense?m=1"
EXERCISE_URL = "https://sgdf.production.sirom.net/exercice?m=1"
STATS_URL = "https://sgdf.production.sirom.net/statistiqueslibres"

USERNAME = os.getenv("SGDF_USERNAME")
PASSWORD = os.getenv("SGDF_PASSWORD")
SERVICE_ACCOUNT_FILE = os.getenv("SGDF_SERVICE_ACCOUNT_FILE")
SPREADSHEET_URL = os.getenv("SPREADSHEET_URL")
EXPECTED_PERIOD = os.getenv("EXPECTED_PERIOD")
PAUSE_SECONDS = int(os.getenv("PAUSE_SECONDS", "5"))

# ======================
# LOGGING
# ======================
LOG_FILE = os.path.join(LOG_DIR, f"pipeline_{time.strftime('%Y-%m-%d_%H-%M-%S')}.log")
logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def log(msg):
    print(msg)
    logging.info(msg)

# ======================
# DRIVER
# ======================
def create_driver(headless=True):
    options = Options()
    if headless:
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-blink-features=AutomationControlled")

    prefs = {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
    }
    options.add_experimental_option("prefs", prefs)
    options.add_argument("--window-size=1920,1080")

    return webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# ======================
# GOOGLE CLIENTS
# ======================
def get_gspread():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return gspread.authorize(creds)


def get_drive():
    creds = Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=[
            "https://www.googleapis.com/auth/drive.readonly",
            "https://www.googleapis.com/auth/spreadsheets"
        ]
    )
    return build("drive", "v3", credentials=creds)

# ======================
# COMMON ACTIONS
# ======================
def login(driver):
    log("Logging in...")
    driver.get(LOGIN_URL)
    wait = WebDriverWait(driver, 20)

    wait.until(EC.presence_of_element_located((By.ID, "username"))).send_keys(USERNAME)
    driver.find_element(By.ID, "password").send_keys(PASSWORD)
    driver.find_element(By.ID, "kc-login").click()


def ensure_period(driver):
    wait = WebDriverWait(driver, 20)
    period_div = wait.until(EC.presence_of_element_located(
        (By.XPATH, "//div[@title='Structure et exercice actuellement sélectionnés']")
    ))

    if EXPECTED_PERIOD not in period_div.text:
        log("Switching period...")
        driver.get(EXERCISE_URL)

        select = Select(wait.until(EC.presence_of_element_located(
            (By.ID, "portal_bundle_frontbundleexercicechange_id")
        )))
        select.select_by_visible_text(EXPECTED_PERIOD)
        driver.find_element(By.ID, "portal_bundle_frontbundleexercicechange_submit").click()
    else:
        log("Period already correct.")

# ======================
# STEP 1: CSV EXPORT
# ======================
def download_csv(driver):
    log("Downloading CSV...")
    driver.get(TARGET_URL)
    wait = WebDriverWait(driver, 20)

    wait.until(EC.presence_of_element_located((By.ID, "exportCSV"))).click()
    wait.until(EC.element_to_be_clickable((By.ID, "depenserecette_export_generer"))).click()

    file_path = None
    timeout = 60
    elapsed = 0
    while not file_path and elapsed < timeout:
        for f in os.listdir(DOWNLOAD_DIR):
            if f.endswith(".csv") and not f.endswith(".crdownload"):
                file_path = os.path.join(DOWNLOAD_DIR, f)
                break
        time.sleep(1)
        elapsed += 1

    if not file_path:
        raise TimeoutError("CSV download did not complete within 60 seconds.")

    return file_path


def upload_csv_to_sheet(file_path):
    log("Uploading CSV to Sheets...")

    try:
        df = pd.read_csv(file_path, sep=";", encoding="latin1")
    except Exception:
        df = pd.read_csv(file_path)

    client = get_gspread()
    sheet = client.open_by_url(SPREADSHEET_URL)
    ws = sheet.worksheet(os.getenv("SHEET_COMPTA"))

    # Numeric conversion
    numeric_cols = ["Dépense", "Recette", "Dépense ventilation", "Recette ventilation"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".", regex=False),
                errors="coerce"
            ).fillna(0)

    df = df.replace([float("inf"), float("-inf")], 0)
    df = df.fillna("")

    ws.update(values=df.values.tolist(), range_name="A3")
    log(f"CSV uploaded: {len(df)} rows.")

    os.remove(file_path)

# ======================
# STEP 2: STATS SCRAPING
# ======================
def scrape_stats(driver):
    log("Scraping stats...")
    driver.get(STATS_URL)
    wait = WebDriverWait(driver, 20)

    select = Select(wait.until(EC.presence_of_element_located((By.NAME, "requete_a_choisir"))))
    select.select_by_visible_text("GS - Check Export Trésoriers")

    driver.find_element(By.CSS_SELECTOR, "input[value='Afficher les paramètres de la requête sélectionnée']").click()

    # Wait for DOM to settle after partial reload, then re-locate _submit
    time.sleep(2)
    submit_btn = wait.until(EC.element_to_be_clickable((By.ID, "_submit")))
    driver.execute_script("arguments[0].click();", submit_btn)

    time.sleep(PAUSE_SECONDS)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = soup.find("table")

    if not table:
        raise ValueError("Stats table not found in page source.")

    rows = []
    for tr in table.find_all("tr"):
        cells = []
        for td in tr.find_all(["td", "th"]):
            btn = td.find("button")
            inp = td.find("input")
            if btn:
                cells.append(btn.get_text(strip=True))
            elif inp:
                cells.append(inp.get("value", "").strip())
            else:
                cells.append(td.get_text(strip=True))
        if cells:
            rows.append(cells)

    if not rows:
        raise ValueError("No rows found in stats table.")

    headers = rows[0]
    data = rows[1:]
    df = pd.DataFrame(data, columns=headers)
    log(f"Scraped stats: {df.shape[0]} rows x {df.shape[1]} cols")
    return df


def upload_stats(df):
    log("Uploading stats to Sheets...")
    client = get_gspread()
    sheet = client.open_by_url(SPREADSHEET_URL)
    ws = sheet.worksheet(os.getenv("SHEET_STATS"))

    df = df.replace([float("inf"), float("-inf")], 0)
    df = df.fillna("")

    ws.update(values=[df.columns.tolist()] + df.values.tolist(), range_name="Q1")
    log("Stats uploaded.")

# ======================
# STEP 3: DRIVE FILE LIST
# ======================
def list_drive_files(folder_id):
    log("Listing Drive files...")
    service = get_drive()

    files = []
    page_token = None
    while True:
        response = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(name)",
            pageToken=page_token
        ).execute()
        for f in response.get("files", []):
            files.append([f["name"]])
        page_token = response.get("nextPageToken")
        if not page_token:
            break

    log(f"Found {len(files)} files in Drive folder.")
    return files


def upload_drive_files(data):
    log("Uploading Drive file list to Sheets...")
    client = get_gspread()
    sheet = client.open_by_url(SPREADSHEET_URL)
    ws = sheet.worksheet(os.getenv("SHEET_FILES"))

    ws.update(values=[["File Name"]] + data, range_name="A1")
    log("Drive file list uploaded.")

# ======================
# MAIN PIPELINE
# ======================
def main():
    log("=== START PIPELINE ===")

    driver = create_driver(headless=True)

    try:
        login(driver)
        ensure_period(driver)

        # Step 1 — CSV export
        csv_file = download_csv(driver)
        upload_csv_to_sheet(csv_file)

        # Step 2 — Stats scraping
        stats_df = scrape_stats(driver)
        upload_stats(stats_df)

        # Step 3 — Drive file list
        folder_id = os.getenv("DRIVE_FOLDER_ID")
        files = list_drive_files(folder_id)
        upload_drive_files(files)

        log("=== SUCCESS ===")

    except Exception as e:
        log(f"ERROR: {e}")
        driver.save_screenshot(os.path.join(BASE_DIR, "error.png"))
        raise

    finally:
        driver.quit()
        log("Driver closed.")


if __name__ == "__main__":
    main()