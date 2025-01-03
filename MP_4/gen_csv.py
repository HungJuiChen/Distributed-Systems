import csv

def generate_new_csv(input_csv, number_of_records, output_csv):
    """
    Generate a new CSV file with the specified number of records.

    Parameters:
        input_csv (str): The input CSV file path.
        number_of_records (int): The number of records to include in the output file.
        output_csv (str): The output CSV file path.
    """
    try:
        with open(input_csv, 'r', newline='', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            # Read the header
            header = next(reader)
            
            # Collect specified number of records
            rows = [header]
            for i, row in enumerate(reader):
                if i >= number_of_records:
                    break
                rows.append(row)
        
        # Write to the output CSV
        with open(output_csv, 'w', newline='', encoding='utf-8') as outfile:
            writer = csv.writer(outfile)
            writer.writerows(rows)
        
        print(f"Successfully created {output_csv} with {len(rows)-1} records.")
    except Exception as e:
        print(f"An error occurred: {e}")

# Example usage
input_csv = "Traffic_Signs.csv"  # Input file uploaded
output_csv = "TrafficSigns_5000"  # Replace with desired output file name
number_of_records = 5000  # Replace with desired number of records

generate_new_csv(input_csv, number_of_records, output_csv)
