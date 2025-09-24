from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

def process_yelp_data_spark(business_file_path: str, review_file_path: str, output_dir: str) -> None:
    """
    Process Yelp data to extract user-business pairs for Nevada businesses.
    Output is written as a single CSV file to the specified directory.
    """
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Full path to the output file within the directory
    output_file_path = os.path.join(output_dir, "user_business.csv")
    
    # Initialize Spark session
    spark = SparkSession.builder.appName("Yelp Data Processor").getOrCreate()
    
    try:
        print("Processing business data...")
        businesses_df = spark.read.json(business_file_path)
        nevada_businesses_df = businesses_df.filter(col("state") == "NV").select("business_id")
        
        # Cache the DataFrame since we'll use it for joining
        nevada_businesses_df.cache()
        
        # Read reviews and join with Nevada businesses
        print("Processing reviews...")
        reviews_df = spark.read.json(review_file_path)
        
        # Join to get only reviews for Nevada businesses
        result_df = reviews_df.join(
            nevada_businesses_df,
            on="business_id",
            how="inner"
        ).select("user_id", "business_id")
        
        # Convert to pandas and write directly to a single CSV file
        print(f"Writing output to {output_file_path}...")
        result_pd = result_df.toPandas()
        result_pd.to_csv(output_file_path, index=False)
        
        print(f"Output successfully written to {output_file_path}")
        print(f"CSV file location: {os.path.abspath(output_file_path)}")
        
    finally:
        # Stop the Spark session
        spark.stop()

def main():
    # File paths
    business_file = './data/business.json'  # Path to business data
    review_file = './data/review.json'      # Path to review data
    
    output_dir = './data/'  
    
    try:
        process_yelp_data_spark(business_file, review_file, output_dir)
    except Exception as e:
        print(f"Error: An unexpected error occurred - {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()