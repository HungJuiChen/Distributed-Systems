# stateless

import argparse
import csv

def parse_csv_line(line):
    columns = [
        "X", "Y", "OBJECTID", "Sign_Type", "Size_", "Supplement", "Sign_Post", 
        "Year_Insta", "Category", "Notes", "MUTCD", "Ownership", "FACILITYID", 
        "Schools", "Location_Adjusted", "Replacement_Zone", "Sign_Text", 
        "Set_ID", "FieldVerifiedDate", "GlobalID"
    ]
    
    # Parse the CSV line
    reader = csv.DictReader([line], fieldnames=columns)
    for row in reader:
        # Extract OBJECTID and Sign_Type
        objectid = row.get("OBJECTID", "")
        sign_type = row.get("Sign_Type", "")
        print(f"KEY: _, VALUE: ({objectid}, {sign_type})")

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Parse a CSV row to extract OBJECTID and Sign_Type.")
    parser.add_argument("key", type=str, help="The CSV key to parse.")
    parser.add_argument("line", type=str, help="The CSV row to parse.")

    # Parse the command-line arguments
    args = parser.parse_args()

    # Call the parse function
    parse_csv_line(args.line)

if __name__ == "__main__":
    main()
