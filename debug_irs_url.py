
import requests
from datetime import date
import sys

# Try to check if 2026-01-05 is a holiday
try:
    import jpholiday
    d = date(2026, 1, 5)
    print(f"Is {d} a holiday? {jpholiday.is_holiday(d)}")
    if jpholiday.is_holiday(d):
        print(f"Holiday name: {jpholiday.is_holiday_name(d)}")
except ImportError:
    print("jpholiday not installed")

# Check URL
target_date = date(2026, 1, 5)
date_str = target_date.strftime("%Y%m%d")
pdf_filename = f"SettlementRates_{date_str}.pdf"
base_url = "https://www.jpx.co.jp/jscc/cimhll0000000umu-att"
url = f"{base_url}/{pdf_filename}"

print(f"Checking URL: {url}")
try:
    response = requests.head(url, timeout=10)
    print(f"Status Code: {response.status_code}")
except Exception as e:
    print(f"Request failed: {e}")
