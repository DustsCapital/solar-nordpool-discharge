#!/usr/bin/env python3
import requests
import pandas as pd
import os
import glob
import time
from datetime import datetime, timedelta
import pytz
import argparse
from config import CONFIG

# Set EET timezone
EET = pytz.timezone('Europe/Riga')

def get_daily_log_file(prefix):
    date_str = datetime.now(EET).strftime('%Y-%m-%d')
    return os.path.join(CONFIG['saves_folder'], f"{prefix}_{date_str}.txt")

def log_message(msg, prefix):
    log_file = get_daily_log_file(prefix)
    timestamp = datetime.now(EET).strftime('%Y-%m-%d %H:%M:%S EET')
    full_msg = f"[{timestamp}] {msg}"
    print(full_msg)
    os.makedirs(CONFIG['saves_folder'], exist_ok=True)
    with open(log_file, 'a') as f:
        f.write(full_msg + '\n')

def cleanup_old_logs(prefix):
    pattern = os.path.join(CONFIG['saves_folder'], f"{prefix}_*.txt")
    files = glob.glob(pattern)
    if len(files) <= CONFIG['max_files']:
        return
    files.sort(key=lambda f: os.path.basename(f).split('_')[-1].replace('.txt', ''), reverse=True)
    for old_file in files[CONFIG['max_files']:]:
        os.remove(old_file)
        log_message(f"Deleted old log: {old_file}", prefix)

def create_saves_folder():
    os.makedirs(CONFIG['saves_folder'], exist_ok=True)

def fetch_prices(target_date):
    url = f"https://dataportal-api.nordpoolgroup.com/api/DayAheadPrices?date={target_date}&market=DayAhead&deliveryArea={CONFIG['delivery_area']}&currency=EUR"
    try:
        response = requests.get(url, timeout=10)
        log_message(f"Fetch for {target_date}: status {response.status_code}", CONFIG['fetch_log_prefix'])
        if response.status_code == 200:
            data = response.json()
            if 'multiAreaEntries' in data and data['multiAreaEntries']:
                df = pd.DataFrame(data['multiAreaEntries'])
                df['deliveryStart'] = pd.to_datetime(df['deliveryStart'])
                df['deliveryEnd'] = pd.to_datetime(df['deliveryEnd'])
                df['local_start'] = df['deliveryStart'].dt.tz_convert(EET)
                df['local_end'] = df['deliveryEnd'].dt.tz_convert(EET)
                df['StartTime'] = df['local_start'].dt.strftime('%H:%M')
                df['EndTime'] = df['local_end'].dt.strftime('%H:%M')
                df['Price'] = df['entryPerArea'].apply(lambda x: x.get('LV', 0))
                df['Price'] = df['Price'].round(2)
                log_message(f"Success: Fetched {len(df)} 15-min slots for {target_date} (EET times)", CONFIG['fetch_log_prefix'])
                return df[['StartTime', 'EndTime', 'Price']]
            else:
                log_message(f"No multiAreaEntries for {target_date}", CONFIG['fetch_log_prefix'])
                return None
        elif response.status_code == 204:
            log_message(f"204 No Content for {target_date} (pre-auction)", CONFIG['fetch_log_prefix'])
            return None
        else:
            log_message(f"API error: {response.text[:200]}", CONFIG['fetch_log_prefix'])
            return None
    except Exception as e:
        log_message(f"Exception fetching {target_date}: {e}", CONFIG['fetch_log_prefix'])
        return None

def save_to_csv(df, filename):
    filepath = os.path.join(CONFIG['saves_folder'], filename)
    df.to_csv(filepath, index=False)
    log_message(f"Saved: {filepath}", CONFIG['fetch_log_prefix'])

def cleanup_old_files():
    pattern = os.path.join(CONFIG['saves_folder'], 'lv_prices_*.csv')
    files = glob.glob(pattern)
    if len(files) <= CONFIG['max_files']:
        return
    files.sort(key=lambda f: os.path.basename(f).split('_')[-1].replace('.csv', ''), reverse=True)
    for old_file in files[CONFIG['max_files']:]:
        os.remove(old_file)
        log_message(f"Deleted old CSV: {old_file}", CONFIG['fetch_log_prefix'])

def run_today_discharge():
    today = datetime.now(EET).strftime('%Y-%m-%d')
    log_message(f"Running discharge for TODAY ({today})", CONFIG['fetch_log_prefix'])
    df = fetch_prices(today)
    if df is not None:
        filename = f"lv_prices_{today}.csv"
        save_to_csv(df, filename)
        cleanup_old_files()
        log_message("Today's data ready → launching solar_discharge.py", CONFIG['fetch_log_prefix'])
        os.system("python3 solar_discharge.py")
    else:
        log_message("Failed to fetch today's data", CONFIG['fetch_log_prefix'])

def monitor_tomorrow():
    tomorrow = (datetime.now(EET) + timedelta(days=1)).strftime('%Y-%m-%d')
    log_message(f"Starting monitoring for tomorrow's data ({tomorrow})", CONFIG['fetch_log_prefix'])
    
    while True:
        now_eet = datetime.now(EET)
        if now_eet.hour >= CONFIG['retry_end_hour'] and now_eet.minute >= 30:
            log_message("Reached 18:30 EET — stopping tomorrow fetch attempts", CONFIG['fetch_log_prefix'])
            break
        
        df = fetch_prices(tomorrow)
        if df is not None:
            filename = f"lv_prices_{tomorrow}.csv"
            save_to_csv(df, filename)
            cleanup_old_files()
            log_message(f"Tomorrow's data ({tomorrow}) fetched and saved!", CONFIG['fetch_log_prefix'])
            break
        else:
            wait_min = 30 - now_eet.minute if now_eet.minute < 30 else 60 - now_eet.minute
            log_message(f"Tomorrow's data not ready → retry in {wait_min} min", CONFIG['fetch_log_prefix'])
            time.sleep(wait_min * 60)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-today', action='store_true', help="Test fetch today only")
    args = parser.parse_args()
    
    log_message("=== Solar Nord Pool Automation Started ===", CONFIG['fetch_log_prefix'])
    create_saves_folder()
    cleanup_old_logs(CONFIG['fetch_log_prefix'])
    
    if args.test_today:
        run_today_discharge()
        return
    
    # 1. Always run today's discharge first
    run_today_discharge()
    
    # 2. Then start monitoring for tomorrow
    monitor_tomorrow()
    
    log_message("All tasks completed — exiting. Restart daily.", CONFIG['fetch_log_prefix'])

if __name__ == "__main__":
    main()
