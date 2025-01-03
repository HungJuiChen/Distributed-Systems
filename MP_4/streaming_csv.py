from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, array, array_contains

# Create SparkSession
spark = SparkSession.builder \
    .appName("Traffic Signs Streaming") \
    .getOrCreate()

# Set the log level to WARN to reduce log noise
spark.sparkContext.setLogLevel("WARN")

# Define the schema based on the header of Traffic_Signs.csv
schema = """
    X DOUBLE,
    Y DOUBLE,
    OBJECTID INT,
    Sign_Type STRING,
    Size_ STRING,
    Supplement STRING,
    Sign_Post STRING,
    Year_Insta INT,
    Category STRING,
    Notes STRING,
    MUTCD STRING,
    Ownership STRING,
    FACILITYID STRING,
    Schools STRING,
    Location_Adjusted STRING,
    Replacement_Zone STRING,
    Sign_Text STRING,
    Set_ID STRING,
    FieldVerifiedDate STRING,
    GlobalID STRING
"""

# Define the file path for the specific file
file_path = "TrafficSigns_10000.csv"
csv_data = spark.read \
    .schema(schema) \
    .csv(file_path)

# Filter rows containing the pattern in any string column
string_columns = [col_name for col_name, dtype in csv_data.dtypes if dtype == "string"]
condition = lit(False)
for col_name in string_columns:
    condition = condition | col(col_name).contains("Streetname")

# Apply the filter and select specific columns (OBJECTID, Sign_Type)
filtered_data = csv_data.filter(condition).select(col("OBJECTID"), col("Sign_Type"))
print("Filtered rows containing the pattern 'X' in any string column:")
filtered_data.show()