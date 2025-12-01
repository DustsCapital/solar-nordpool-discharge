"""
Unified Config for Nord Pool Fetcher & Solar Discharge Scripts.
Edit here for all settings—no dupes!
"""

CONFIG = {
    # Fetcher settings
    'delivery_area': 'LV',  # Latvia zone
    'saves_folder': 'saves',
    'max_files': 10,  # Keep 10 latest CSVs/logs
    'retry_start_hour': 13,  # EET hour to start retries
    'retry_end_hour': 18,  # Stop retries after
    'retry_minutes': 30,  # Attempt at :30 past hour
    
    # Discharge settings
    'min_price_threshold': 20,  # €/MWh to trigger discharge
    'discharge_duration_min': 15,  # 15-min slots
    
    # Modbus for SolaX EMS1000 (edit these!)
    'modbus_host': '192.168.1.100',  # Real IP or '/dev/ttyUSB0' for RS485
    'modbus_port': 502,  # TCP port or baud (e.g., 9600)
    'modbus_unit': 1,  # Slave address
    'use_tcp': True,  # True for Ethernet; False for RS485
    
    # Logs (now dated in saves/)
    'fetch_log_prefix': 'fetch_log',
    'discharge_log_prefix': 'discharge_log',
}
