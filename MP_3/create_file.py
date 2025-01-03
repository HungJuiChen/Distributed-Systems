import argparse
import os
import random
import json

# example: 
# python create_file.py myfile.bin 10MB
# python create_file.py sample.dat 500KB --seed 12345


def parse_filesize(size_str):
    """Parse a human-readable file size string into bytes."""
    size_str = size_str.strip().upper()
    if size_str.endswith('KB'):
        return int(float(size_str[:-2]) * 1024)
    elif size_str.endswith('MB'):
        return int(float(size_str[:-2]) * 1024 ** 2)
    elif size_str.endswith('GB'):
        return int(float(size_str[:-2]) * 1024 ** 3)
    elif size_str.endswith('B'):
        return int(size_str[:-1])
    else:
        return int(size_str)

def create_file(filename, filesize, seed=None):
    """Create a file with the specified size. Content is random but reproducible with a seed."""
    if seed is not None:
        random.seed(seed)
    else:
        random.seed(0) 

    data_size = 0
    json_lines = []  

    while data_size < filesize:
        json_line = {"key": f"key_{random.randint(1, 1e6)}", "value": random.randint(1, 1e6)}

        json_str = json.dumps(json_line)

        new_data_size = data_size + len(json_str) + 2  # +2 for commas/newlines
        if new_data_size >= filesize:
            break

        json_lines.append(json_str)
        data_size += len(json_str) + 2

    with open(filename, 'w') as f:
        f.write("[\n" + ",\n".join(json_lines) + "\n]")  

    print(f"Created JSON file '{filename}' of size approximately {data_size} bytes.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Create a file with specified size.")
    parser.add_argument("filename", type=str, help="Name of the file to create.")
    parser.add_argument("filesize", type=str, help="Size of the file (e.g., 10MB, 500KB, 1GB).")
    parser.add_argument("--seed", type=int, default=None, help="Seed for random data generation (optional).")

    args = parser.parse_args()

    filesize_bytes = parse_filesize(args.filesize)
    create_file(args.filename, filesize_bytes, seed=args.seed)
