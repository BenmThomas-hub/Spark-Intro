import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

def main():
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("DataEngineeringIntro") \
        .getOrCreate()

    # Sample data
    samp_data = [
        ("Alice", 34, "F", 146, 85),
        ("Bob", 45, "M", 178, 100),
        ("Catherine", 29, "F", 159, 55),
        ("David", 35, "M", 200, 110),
        ("Ella", 40, "F", 160, 65),
        ("Frank", 28, "M", 168, 75),
        ("Gina", 33, "F", 155, 50),
        ("Harry", 39, "M", 170, 80),
        ("Irene", 31, "F", 150, 60),
        ("John", 36, "M", 175, 90)
    ]
    
    # Read matrices from CSV files
    data = pd.read_csv(sys.argv[1], header=None).values.tolist()

    # Define schema
    columns = ["Name", "Age", "Gender", "Height", "Weight"]

    # Create DataFrame
    df = spark.createDataFrame(data, columns)

    # Show the DataFrame
    print("Original DataFrame:")
    df.show()
    
    print("\nSummary Statistics For Whole:")
    # Calculate the average age
    average_age = df.select(avg("Age")).collect()[0][0]
    print(f"   Average Age: {average_age:.2f}")
    
    # Calculate the average height
    average_height = df.select(avg("Height")).collect()[0][0]
    print(f"   Average Height: {average_height:.2f}")
    
    # Calculate the average weight
    average_weight = df.select(avg("Weight")).collect()[0][0]
    print(f"   Average Weight: {average_weight:.2f}\n")

    # Filter the DataFrame
    filtered_age_df = df.filter((df.Age > 32) & (df.Age <= 40))

    # Show the filtered DataFrame
    print("Filtered DataFrame (Age > 32):")
    filtered_age_df.show()
    
    print("\nSummary Statistics For Filter:")
    # Calculate the average age
    average_age = filtered_age_df.select(avg("Age")).collect()[0][0]
    print(f"   Average Age: {average_age:.2f}")
    
    # Calculate the average height
    average_height = filtered_age_df.select(avg("Height")).collect()[0][0]
    print(f"   Average Height: {average_height:.2f}")
    
    # Calculate the average weight
    average_weight = filtered_age_df.select(avg("Weight")).collect()[0][0]
    print(f"   Average Weight: {average_weight:.2f}\n")

    # Write DataFrame to CSV
    output_path = "output_data.csv"
    df.write.mode("overwrite").csv(output_path)
    print(f"Data written to {output_path}")

    # Stop the SparkSession
    spark.stop()

if __name__ == "__main__":
    main()
