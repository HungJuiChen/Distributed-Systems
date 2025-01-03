# stateless

import argparse
import json

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Parse a CSV row to extract OBJECTID and Sign_Type.")
    parser.add_argument("key", type=str, help="The CSV key to parse.")
    parser.add_argument("line", type=str, help="The CSV row to parse.")

    # Parse the command-line arguments
    args = parser.parse_args()

    print(f"KEY: {args.key}, VALUE: {args.line}")

if __name__ == "__main__":
    main()