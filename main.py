#!/usr/bin/env python3
import requests
import pandas as pd
import os
import glob
import time
from datetime import datetime, timedelta
import pytz
import argparse
from config import CONFIG  # Import unified config

# Set EET timezone
EET = pytz.timezone('Europe/Riga')

def get_daily_log_file(prefix):
    """Get dated log file in saves/ (e.g., saves/fetch_log_2025-12-01.txt)."""
    date_str = datetime.now(EET).strftime('%Y-%m-%d')
    return os.path.join(CONFIG['saves_folder'], f"{prefix}_{date_str}.txt")

def log_message(msg, prefix):
    """Append to dated log in saves/."""
    log_file = get_daily_log_file(prefix)
    timestamp = datetime.now(EET).strftime('%Y-%m-%d %H:%M:%S EET')
    full_msg = f"[{timestamp}] {msg}"
    print(full_msg)
    os.makedirs(CONFIG['saves_folder'], exist_ok=True)
    with open(log_file, 'a') as f:
        f.write(full_msg + '\n')

def cleanup_old_logs(prefix):
    """Keep only the latest 10 log files for this prefix."""
    pattern = os.path.join(CONFIG['saves_folder'], f"{prefix}_*.txt")
    files = glob.glob(pattern)
    if len(files) <= CONFIG['max_files']:
        return
    # Sort by filename date (desc: newest first)
    files.sort(key=lambda f: os.path.basename(f).split('_')[-1].replace('.txt', ''), reverse=True)
    # Delete oldest (beyond 10)
    for old_file in files[CONFIG['max_files']:]:
        os.remove(old_file)
        log_message(f"Deleted old log: {old_file}", prefix)

def create_saves_folder():
    os.makedirs(CONFIG['saves_folder'], exist_ok=True)

def fetch_prices(target_date):
    """Fetch 15-min prices for given date via free public JSON API (96 slots/day, in EET local time)."""
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
                # Convert UTC to EET (API timestamps are tz-aware UTC)
                df['local_start'] = df['deliveryStart'].dt.tz_convert(EET)
                df['local_end'] = df['deliveryEnd'].dt.tz_convert(EET)
                df['StartTime'] = df['local_start'].dt.strftime('%H:%M')
                df['EndTime'] = df['local_end'].dt.strftime('%H:%M')
                df['Price'] = df['entryPerArea'].apply(lambda x: x.get('LV', 0))
                df['Price'] = df['Price'].round(2)
                log_message(f"Success: Fetched {len(df)} 15-min slots for {target_date} (EET times)", CONFIG['fetch_log_prefix'])
                return df[['StartTime', 'EndTime', 'Price']]
            else:
                log_message(f"No multiAreaEntries in response for {target_date}", CONFIG['fetch_log_prefix'])
                return None
        elif response.status_code == 204:
            log_message(f"204 No Content for {target_date} (pre-auction)", CONFIG['fetch_log_prefix'])
            return None
        else:
            log_message(f"API error for {target_date}: {response.text[:200]}", CONFIG['fetch_log_prefix'])
            return None
    except Exception as e:
        log_message(f"Exception fetching {target_date}: {e}", CONFIG['fetch_log_prefix'])
        return None

def save_to_csv(df, filename):
    """Save DF to CSV in saves folder."""
    filepath = os.path.join(CONFIG['saves_folder'], filename)
    df.to_csv(filepath, index=False)
    log_message(f"Saved: {filepath}", CONFIG['fetch_log_prefix'])

def cleanup_old_files():
    """Keep only the latest 10 CSV files."""
    pattern = os.path.join(CONFIG['saves_folder'], 'lv_prices_*.csv')
    files = glob.glob(pattern)
    if len(files) <= CONFIG['max_files']:
        return
    # Sort by filename date (desc: newest first)
    files.sort(key=lambda f: os.path.basename(f).split('_')[-1].replace('.csv', ''), reverse=True)
    # Delete oldest (beyond 10)
    for old_file in files[CONFIG['max_files']:]:
        os.remove(old_file)
        log_message(f"Deleted old CSV: {old_file}", CONFIG['fetch_log_prefix'])

def should_attempt_fetch(now_eet):
    """Check if it's time to attempt fetch (after 13:00, at :30 min)."""
    hour = now_eet.hour
    minute = now_eet.minute
    if hour < CONFIG['retry_start_hour']:
        return False
    if hour > CONFIG['retry_end_hour']:
        return False  # Stop after 18:00
    return minute >= CONFIG['retry_minutes']  # e.g., 13:30, 14:30, etc.

def main():
    parser = argparse.ArgumentParser(description="24/7 Nord Pool Fetcher")
    parser.add_argument('--test-today', action='store_true', help="Test fetch/save today's data immediately")
    args = parser.parse_args()
    
    log_message("Starting 24/7 Nord Pool fetcher...", CONFIG['fetch_log_prefix'])
    create_saves_folder()
    cleanup_old_logs(CONFIG['fetch_log_prefix'])  # Clean old logs on start
    
    if args.test_today:
        # One-shot test: Fetch/save today, cleanup, exit
        today = datetime.now(EET).strftime('%Y-%m-%d')
        df = fetch_prices(today)
        if df is not None:
            filename = f"lv_prices_{today}.csv"
            save_to_csv(df, filename)
            cleanup_old_files()
            log_message("Test complete: Check saves/ for CSV (96 15-min slots in EET)", CONFIG['fetch_log_prefix'])
        else:
            log_message("Test failed: No data (unlikely for today)", CONFIG['fetch_log_prefix'])
        cleanup_old_logs(CONFIG['fetch_log_prefix'])  # Clean after test
        return  # Exit after test
    
    # Original 24/7 loop (if no flag)
    while True:  # Infinite loop
        now_utc = datetime.now(pytz.utc)
        now_eet = now_utc.astimezone(EET)
        tomorrow = (now_eet + timedelta(days=1)).strftime('%Y-%m-%d')
        
        if should_attempt_fetch(now_eet):
            df = fetch_prices(tomorrow)
            if df is not None:
                filename = f"lv_prices_{tomorrow}.csv"
                save_to_csv(df, filename)
                cleanup_old_files()
                cleanup_old_logs(CONFIG['fetch_log_prefix'])  # Clean after success
                # Success: Sleep full hour before next check
                time.sleep(3600)
            else:
                log_message(f"Retry failed for {tomorrow}; next attempt in 1 hour", CONFIG['fetch_log_prefix'])
                time.sleep(3600)
        else:
            # Wait till next possible slot (e.g., if before 13:30, sleep to 13:30)
            next_attempt = now_eet.replace(hour=CONFIG['retry_start_hour'], minute=CONFIG['retry_minutes'], second=0, microsecond=0)
            if now_eet > next_attempt:
                next_attempt += timedelta(hours=1)
            sleep_sec = (next_attempt - now_eet).total_seconds()
            log_message(f"Waiting {int(sleep_sec/60)} min until next attempt window", CONFIG['fetch_log_prefix'])
            time.sleep(max(sleep_sec, 60))  # Min 1 min

if __name__ == "__main__":
    main()
