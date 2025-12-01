#!/usr/bin/env python3
import pandas as pd
import sys
import os
import glob
from datetime import datetime, timedelta
import pytz  # For EET
from pymodbus.exceptions import ModbusException
from pymodbus.client import ModbusTcpClient  # Or ModbusSerialClient for RS485
from config import CONFIG  # Import unified config

# Set EET timezone
EET = pytz.timezone('Europe/Riga')

def get_daily_log_file(prefix):
    """Get dated log file in saves/ (e.g., saves/discharge_log_2025-12-01.txt)."""
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

def find_peak_slot():
    """Read latest CSV from saves/, find max Price 15-min slot above threshold."""
    saves_folder = CONFIG['saves_folder']
    if not os.path.exists(saves_folder):
        log_message("No saves folder—run fetcher first!", CONFIG['discharge_log_prefix'])
        return None
    
    # Get tomorrow's CSV
    tomorrow = (datetime.now(EET) + timedelta(days=1)).strftime('%Y-%m-%d')
    filename = f"lv_prices_{tomorrow}.csv"
    filepath = os.path.join(saves_folder, filename)
    
    if not os.path.exists(filepath):
        log_message(f"No data for tomorrow ({filename})—falling back to today for test", CONFIG['discharge_log_prefix'])
        today = datetime.now(EET).strftime('%Y-%m-%d')
        filename = f"lv_prices_{today}.csv"
        filepath = os.path.join(saves_folder, filename)
        if not os.path.exists(filepath):
            log_message("No today's data either—run fetcher with --test-today first", CONFIG['discharge_log_prefix'])
            return None
    
    df = pd.read_csv(filepath)
    df['Price'] = df['Price'].astype(float)
    df_filtered = df[df['Price'] >= CONFIG['min_price_threshold']]
    if df_filtered.empty:
        log_message(f"No slots above €{CONFIG['min_price_threshold']} threshold", CONFIG['discharge_log_prefix'])
        return None
    
    peak_idx = df_filtered['Price'].idxmax()
    peak_row = df_filtered.loc[peak_idx]
    log_message(f"Peak slot: {peak_row['StartTime']}–{peak_row['EndTime']} at €{peak_row['Price']:.2f}/MWh", CONFIG['discharge_log_prefix'])
    return {
        'start_time': peak_row['StartTime'],
        'end_time': peak_row['EndTime'],
        'price': peak_row['Price']
    }

def discharge_command(start_time, duration_min):
    """Real Modbus write for 15-min discharge at start_time."""
    try:
        if CONFIG['use_tcp']:
            client = ModbusTcpClient(CONFIG['modbus_host'], port=CONFIG['modbus_port'])
        else:
            from pymodbus.client import ModbusSerialClient
            client = ModbusSerialClient(method='rtu', port=CONFIG['modbus_host'], baudrate=CONFIG['modbus_port'], bytesize=8, parity='N', stopbits=1)
        
        client.connect()
        if not client.connected:
            raise ModbusException("Connection failed")
        
        # Parse start_time to hour/min for SolaX registers (adapt per manual, e.g., 0x011A hour, 0x011B min)
        h, m = map(int, start_time.split(':'))
        client.write_register(0x011A, h, unit=CONFIG['modbus_unit'])  # Start hour
        client.write_register(0x011B, m, unit=CONFIG['modbus_unit'])  # Start min
        client.write_register(0x011C, duration_min, unit=CONFIG['modbus_unit'])  # Duration min
        client.write_register(0x0100, 35, unit=CONFIG['modbus_unit'])  # Timed discharge mode
        client.close()
        log_message(f"Discharge scheduled: {start_time} for {duration_min} min via Modbus", CONFIG['discharge_log_prefix'])
        return True
    except Exception as e:
        log_message(f"Modbus error: {e}", CONFIG['discharge_log_prefix'])
        return False

def test_connection():
    try:
        if CONFIG['use_tcp']:
            client = ModbusTcpClient(CONFIG['modbus_host'], port=CONFIG['modbus_port'])
        else:
            from pymodbus.client import ModbusSerialClient
            client = ModbusSerialClient(method='rtu', port=CONFIG['modbus_host'], baudrate=CONFIG['modbus_port'], bytesize=8, parity='N', stopbits=1)
        client.connect()
        if client.connected:
            result = client.read_holding_registers(0, 1, unit=CONFIG['modbus_unit'])
            log_message(f"Connection SUCCESS: Register 0 = {result.registers}", CONFIG['discharge_log_prefix'])
            client.close()
            return True
        else:
            raise ModbusException("Not connected")
    except Exception as e:
        log_message(f"Connection test FAILED:")

def main(test_mode=False):
    log_message("Solar discharge optimizer started", CONFIG['discharge_log_prefix'])
    cleanup_old_logs(CONFIG['discharge_log_prefix'])  # Clean old logs on start
    
    peak = find_peak_slot()
    if not peak:
        log_message("No viable peak—skipping discharge", CONFIG['discharge_log_prefix'])
        return
    
    start_time = peak['start_time']
    price = peak['price']
    if test_mode:
        log_message(f"TEST MODE: Would discharge at {start_time} for €{price:.2f}/MWh", CONFIG['discharge_log_prefix'])
        return
    
    if discharge_command(start_time, CONFIG['discharge_duration_min']):
        log_message(f"LIVE: Discharged at {start_time}—est. gain €{price * (CONFIG['discharge_duration_min']/60) * 100 / 1000:.2f} (at 100kW)", CONFIG['discharge_log_prefix'])
    else:
        log_message("Discharge failed—check wiring/config", CONFIG['discharge_log_prefix'])
    cleanup_old_logs(CONFIG['discharge_log_prefix'])  # Clean after run

if __name__ == "__main__":
    test = '--test' in sys.argv
    main(test_mode=test)
