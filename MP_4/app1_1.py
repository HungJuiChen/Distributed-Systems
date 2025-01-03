# stateless

import argparse

def filter_line_by_pattern(key, line, pattern):
    if pattern in line:
        print(f"KEY: {key}, VALUE: {line}")

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Filter a line for a specified pattern.")
    parser.add_argument("key", type=str, help="The key to parse.")
    parser.add_argument("line", type=str, help="The line of text to filter.")
    
    # Parse arguments
    args = parser.parse_args()
    
    pattern = "Parking" # modify during demo
    
    # Filter the line based on the pattern
    filter_line_by_pattern(args.key, args.line, pattern)

if __name__ == "__main__":
    main()
