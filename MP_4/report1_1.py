# stateless

import argparse
import re

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Parse a CSV row to extract OBJECTID and Sign_Type.")
    parser.add_argument("key", type=str, help="The CSV key to parse.")
    parser.add_argument("line", type=str, help="The CSV row to parse.")

    # Parse the command-line arguments
    args = parser.parse_args()


    # Split into key and value
    key, value = args.key, args.line

    # Check if "review/score:" is in the value
    if "review/score:" in value:
        # Use regex to extract the score
        match = re.search(r"review/score:\s*(\d+\.\d+)", value)
        if match:
            score = match.group(1)
            print(f"KEY: {score}, VALUE: 1")

if __name__ == "__main__":
    main()
