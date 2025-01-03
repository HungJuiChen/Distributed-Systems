from pyspark import SparkContext

# Initialize SparkContext
sc = SparkContext("local", "StaticFileProcessingApp")
sc.setLogLevel("WARN")  # Suppress INFO logs
file_path = "foods_1000.txt" 
lines = sc.textFile(file_path)

# Filter lines containing the pattern "review/score: 5.0"
filtered_lines = lines.filter(lambda line: "review/score: 5.0" in line)

# Count the number of lines with "review/score: 5.0"
review_count = filtered_lines.count()

# Collect and print the filtered lines
print("Reviews with score 5.0:")
result = filtered_lines.collect()
for line in result:
    print(line)

# Print the count of reviews with "review/score: 5.0"
print(f"\nTotal number of reviews with score 5.0: {review_count}")