import pyspark
import json
import argparse
import time

if __name__ == '__main__':
    sc_conf = pyspark.SparkConf() \
                     .setAppName('task3') \
                     .setMaster('local[*]') \
                     .set('spark.driver.memory', '8g') \
                     .set('spark.executor.memory', '4g')

    sc = pyspark.SparkContext(conf = sc_conf)
    sc.setLogLevel('OFF')

    parser = argparse.ArgumentParser(description='A1T3')
    parser.add_argument('--input_file', type=str, default = '/home/pande250/CSCI-5523/HW-task-1/review.json', help ='input review file')
    parser.add_argument('--output_file', type=str, default = 'task3_default.json', help = 'outputfile')
    parser.add_argument('--n', type=int, default = 10, help = 'threshold number of reviews')
    args = parser.parse_args()

    '''
    YOUR CODE HERE
    '''
#we need to use the review dataset and compute the reviewers that reviewed more than n businesses in the review file

#Starting timing for the default partition
start_time = time.time()

# Loading the input review Data
json_rdd = sc.textFile(args.input_file)

# Parsing into RDD
review_rdd = json_rdd.map(lambda x: json.loads(x))
print(review_rdd.take(3))

#First we will extract the user_id and review_counts from review.json
user_business_rdd = review_rdd.map(lambda x: (x['user_id'], x['business_id'])).distinct()

# Counting how many businesses each user reviewed
user_review_counts = user_business_rdd.mapValues(lambda x: 1).reduceByKey(lambda x, y: x + y)

# Filtering users who reviewed more than `n` businesses
filtered_users = user_review_counts.filter(lambda x: x[1] > args.n)
print(f"users who reviewed more than `n` businesses: {filtered_users.take(10)}")

#Checking the default partitions
num_partitions = filtered_users.getNumPartitions()
print("Number of partitions:", num_partitions)  #The default partion is 27

# identifying number of items per partition.
partition_items = filtered_users.glom().collect()
partition_counts = [len(partition) for partition in partition_items]

print(f"Items per partition: {partition_counts}")  

#Saving the output in the required format
output = {
    "n_partitions": num_partitions,  
    "n_items": partition_counts,  
    "result": filtered_users.collect()  
}

#Saving the output
with open(args.output_file, 'w') as f:
    json.dump(output, f, indent=4)

print("Results saved to", args.output_file)

# Stoping timing
end_time = time.time()
execution_time = end_time - start_time
print(f"Execution time: {execution_time:.4f} seconds")

sc.stop()