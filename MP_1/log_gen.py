import argparse
import random
import string
from datetime import datetime
import os

parser = argparse.ArgumentParser(description='Generate test log for current machine')
parser.add_argument('machine', help='Current machine number')
parser.add_argument('--patterns', action='store_true', help='Include patterns in the log entries')
args = parser.parse_args()

LOG_FILE = f"unittest.{args.machine}.log"
NUM_ENTRIES = 50000
ENTRY_LEN = 50
TARGET_FILESIZE_MB = 60  # Desired file size in MB
TARGET_FILESIZE_BYTES = TARGET_FILESIZE_MB * 1024 * 1024  # 60 MB in bytes

# Predefined patterns
FREQUENT_PATTERN = "FREQ_PATTERN"
INFREQUENT_PATTERN = "PATTERN_INFREQ"
RARE_PATTERN = "RARE_PATTERN"

# Fixed number of each pattern in file, easier for testing
FREQUENT_COUNT = 100000
INFREQUENT_COUNT = 10000
RARE_COUNT = 1000

def random_ascii_string(length):
    return ''.join(random.choices(string.ascii_letters + string.digits + string.punctuation + ' ', k=length))

def insert_pattern(line, pattern):
    pattern_len = len(pattern)
    start_pos = random.randint(0, ENTRY_LEN - pattern_len)
    return line[:start_pos] + pattern + line[start_pos + pattern_len:]

def generate_base_logs():
    logs = []
    for _ in range(NUM_ENTRIES - (FREQUENT_COUNT + INFREQUENT_COUNT + RARE_COUNT)):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = random_ascii_string(ENTRY_LEN)
        logs.append(f"{timestamp} {log_entry}")
    return logs

def insert_patterns(logs):
    pattern_logs = []

    for _ in range(FREQUENT_COUNT):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = random_ascii_string(ENTRY_LEN)
        log_entry = insert_pattern(log_entry, FREQUENT_PATTERN)
        pattern_logs.append(f"{timestamp} {log_entry}")

    for _ in range(INFREQUENT_COUNT):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = random_ascii_string(ENTRY_LEN)
        log_entry = insert_pattern(log_entry, INFREQUENT_PATTERN)
        pattern_logs.append(f"{timestamp} {log_entry}")

    for _ in range(RARE_COUNT):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = random_ascii_string(ENTRY_LEN)
        log_entry = insert_pattern(log_entry, RARE_PATTERN)
        pattern_logs.append(f"{timestamp} {log_entry}")
    
    logs.extend(pattern_logs)
    return logs

def write_logs_to_file(logs):
    with open(LOG_FILE, 'w') as f:
        for log in logs:
            f.write(log + "\n")

def fill_to_target_size():
    while os.path.getsize(LOG_FILE) < TARGET_FILESIZE_BYTES:
        with open(LOG_FILE, 'a') as f:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            log_entry = random_ascii_string(ENTRY_LEN)
            f.write(f"{timestamp} {log_entry}\n")

def generate_logs():
    logs = generate_base_logs()

    if args.patterns:
        logs = insert_patterns(logs)

    random.shuffle(logs)

    write_logs_to_file(logs)

    fill_to_target_size()

generate_logs()
