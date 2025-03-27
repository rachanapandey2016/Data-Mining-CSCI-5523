import pyspark
import json
import argparse
import re


if __name__ == '__main__':
    sc_conf = pyspark.SparkConf() \
                     .setAppName('task1') \
                     .setMaster('local[*]') \
                     .set('spark.driver.memory', '8g') \
                     .set('spark.executor.memory', '4g')

    sc = pyspark.SparkContext(conf = sc_conf)
    sc.setLogLevel('OFF')

    parser = argparse.ArgumentParser(description='A1T1')
    parser.add_argument('--input_file', type=str, default = '/home/pande250/CSCI-5523/HW-task-1/review.json', help ='input file')
    parser.add_argument('--output_file', type=str, default = 'hw1t1.json', help = 'output file')
    parser.add_argument('--stopwords', type=str, default = '/home/pande250/CSCI-5523/HW-task-1/stopwords', help = 'stopwords file')
    parser.add_argument('--m', type=int, default = '10', help = 'review threshold m')
    parser.add_argument('--s', type=int, default = '2', help = 'star rating')
    parser.add_argument('--i', type=int, default = '10', help = 'top i frequent words')
    parser.add_argument('--y', type=int, default = '2018', help = 'year')
    args = parser.parse_args()

    '''
    YOUR CODE HERE
    '''
    
# Load stopwords 
with open(args.stopwords, 'r') as f:
      stopwords = set(f.read().splitlines())

# Loading Review Data
json_rdd = sc.textFile(args.input_file)

# Parsing the RDD
review_rdd = json_rdd.map(lambda x: json.loads(x))
print(review_rdd.take(3))
                                        

# Number of distinct businesses
distinct_businesses = review_rdd.map(lambda x: x['business_id']).distinct().count()
print(f"Number of distinct businesses = {distinct_businesses}")

#Number of distinct users who have written more than m reviews
user_review_counts = review_rdd.map(lambda x: (x['user_id'], 1)).reduceByKey(lambda a, b: a + b)
distinct_users = user_review_counts.filter(lambda x: x[1] > args.m).count()
print(f"Number of users with more than {args.m} reviews: {distinct_users}")

#Total Review Count for Each Year (Sorted by Count Descending, Year Descending)
review_counts_per_year = (review_rdd.map(lambda x: (int(x['date'][:4]), 1))  
                  .reduceByKey(lambda a, b: a + b)  
                  .sortBy(lambda x: (-x[1], -x[0]))  
                  .collect())

review_counts_per_year = [[year, count] for year, count in review_counts_per_year]

print(f"Review counts per year: {review_counts_per_year}")

# Number of reviews with star rating s
num_reviews_with_star_s = review_rdd.filter(lambda x: x['stars'] == args.s).count()
print(f"Number of reviews with {args.s} stars = {num_reviews_with_star_s}")

# Preprocessing text function- removing punctutation, lowercase, and split
def preprocess_text(text, stopwords):
    text = re.sub(r'[\(\),.!?:;\[\]]', '', text).lower()
    words = [word for word in text.split(" ") if word not in stopwords and word.strip() != ""]
    return words

# Filtering reviews for the given year
reviews_year_y = review_rdd.filter(lambda x: x['date'].startswith(str(args.y)))

# Preprocess review text and count word frequencies
frequent_words = (reviews_year_y.flatMap(lambda x: preprocess_text(x['text'], stopwords))  
                                  .map(lambda word: (word, 1))  
                                  .reduceByKey(lambda a, b: a + b)  
                                  .sortBy(lambda x: (-x[1], x[0])) 
                                  .map(lambda x: x[0])  
                                  .take(args.i))  


print(f"Top {args.i} frequent words in {args.y} = {frequent_words}")

# # Preparing the output
output = {"A": distinct_businesses, "B": distinct_users, "C": review_counts_per_year,"D": num_reviews_with_star_s,"E": frequent_words}

# Save the output to a JSON file
with open(args.output_file, 'w') as f:
        json.dump(output, f, indent=4)

print("Results saved to", args.output_file)

sc.stop()







