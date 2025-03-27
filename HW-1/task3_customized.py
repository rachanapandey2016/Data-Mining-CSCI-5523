import pyspark
import json
import argparse
import time

if __name__ == '__main__':
    sc_conf = pyspark.SparkConf() \
                     .setAppName('task3_customized') \
                     .setMaster('local[*]') \
                     .set('spark.driver.memory', '8g') \
                     .set('spark.executor.memory', '4g')

    sc = pyspark.SparkContext(conf = sc_conf)
    sc.setLogLevel('OFF')

    parser = argparse.ArgumentParser(description='A1T3')
    parser.add_argument('--input_file', type=str, default = '/home/pande250/CSCI-5523/HW-task-1/review.json', help ='input review file')
    parser.add_argument('--output_file', type=str, default = 'task3_custom.json', help = 'output file')
    parser.add_argument('--n_partitions', type=int, default = 20, help = 'number of partitions')
    parser.add_argument('--n', type=int, default = 10, help = 'threshold number of reviews')
    args = parser.parse_args()

    '''
    YOUR CODE HERE
    '''

#Starting timing for the customized partition
start_time = time.time()

#Defining the custom partition function
def my_hash(user_id):
    return hash(user_id) % args.n_partitions

# Load Yelp Review Data
json_rdd = sc.textFile(args.input_file)

# Parsing review JSON object in the RDD 
review_rdd = json_rdd.map(lambda x: json.loads(x))
print(review_rdd.take(3))

#First we will extract the user_id and review_counts from review.json
user_business_rdd = review_rdd.map(lambda x: (x['user_id'], x['business_id'])).distinct()

#applying the custom partition hash function
user_business_rdd = user_business_rdd.partitionBy(args.n_partitions, my_hash)

# Counting how many businesses each user reviewed
user_review_counts = user_business_rdd.mapValues(lambda x: 1).reduceByKey(lambda x, y: x + y)

# Filtering users who reviewed more than `n` businesses
filtered_users = user_review_counts.filter(lambda x: x[1] > args.n)

#Checking the customized partitions
num_partitions = filtered_users.getNumPartitions()
print("Number of partitions:", num_partitions) 

# identifying number of items per partition.
partition_items = filtered_users.glom().collect()
partition_counts = [len(partition) for partition in partition_items]

print(f"Items per partition: {partition_counts}")  

# saving output in required format
output = {
    "n_partitions": num_partitions,  
    "n_items": partition_counts,  
    "result": filtered_users.collect()  
}

with open(args.output_file, 'w') as f:
    json.dump(output, f, indent=4)

print("Results saved to:", args.output_file)

# Stoping timing
end_time = time.time()
execution_time = end_time - start_time
print(f"Customized Partition Execution time: {execution_time:.4f} seconds")

sc.stop()
    