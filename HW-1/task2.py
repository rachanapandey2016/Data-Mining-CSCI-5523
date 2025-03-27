import pyspark
import json
import argparse

if __name__ == '__main__':
    sc_conf = pyspark.SparkConf() \
                     .setAppName('task2') \
                     .setMaster('local[*]') \
                     .set('spark.driver.memory', '8g') \
                     .set('spark.executor.memory', '4g')

    sc = pyspark.SparkContext(conf = sc_conf)
    sc.setLogLevel('OFF')

    parser = argparse.ArgumentParser(description='A1T2')
    parser.add_argument('--review_file', type=str, default = '/home/pande250/CSCI-5523/HW-task-2/review.json', help ='input review file')
    parser.add_argument('--business_file', type=str, default = '/home/pande250/CSCI-5523/HW-task-2/business.json', help = 'input business file')
    parser.add_argument('--output_file', type=str, default = 'hw1t2.json', help = 'outputfile')
    parser.add_argument('--n', type=int, default = '50', help = 'top n categories with highest average stars')
    args = parser.parse_args()

    '''
    YOUR CODE HERE
    '''

# Loading Yelp Review Data
json_rdd = sc.textFile(args.review_file)

# Parsing review JSON object in the RDD
review_rdd = json_rdd.map(lambda x: json.loads(x))
print(review_rdd.take(3))

# Load Yelp business Data
business_rdd = sc.textFile(args.business_file)

# Parsing JSON object in the RDD
bus_parsed_rdd = business_rdd.map(lambda x: json.loads(x))

#Filtering business which have None in its category section
def process_rdd(rdd):
    return rdd.filter(lambda x: x.get('categories') is not None)

business_result = process_rdd(bus_parsed_rdd)

#tranforming the categories into list
business_categories = (business_result).map(lambda x: (x['business_id'], x['categories'].split(', ')))  # Split categories

#Extracting business and their corresponding stars from the review.json file and later we need to link this to the business and categories of business.json file
business_ratings = review_rdd.map(lambda x: (x['business_id'], x['stars']))

# #Now we need to join ratings with categories on business_id using join transformation(business_id, categories, stars)
category_ratings = (business_categories                        #business categories is from business file
    .join(business_ratings)  # (business_id, ([category1, category2, ...], stars))        #business rating is from the review file
    .flatMap(lambda x: [(category, x[1][1]) for category in x[1][0]])  # Expand categories
)

# #Computing the average ratings for the business with multiple categories
#First we will sum up all the counts
category_ratings = (category_ratings
                    .mapValues(lambda x: (x, 1))
                    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])))

print(f"first_3_category_ratings: {category_ratings.take(3)}")

#Computing average
average_category_ratings = (
    category_ratings.map(lambda x: (x[0], x[1][0] / x[1][1])))  # Divide total stars by count
print(f"average_category_ratings: {average_category_ratings.take(3)}")

#Now sorting in descending order and then lexicographically
sorted_categories = (average_category_ratings
                     .sortBy (lambda x: (-x[1], x[0]))
                     .take(args.n))       #it takes the top 50, which is our default value

#Converting to required output format
output = {"result": sorted_categories}

#Saving the output to a JSON file
with open(args.output_file, 'w') as f:
    json.dump(output, f, indent=4)

print("Results saved to", args.output_file)

# Stoping Spark Context
sc.stop()
