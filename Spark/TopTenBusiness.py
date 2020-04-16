from pyspark import SparkContext

sc = SparkContext("local", "Top Ten Business Reviews")

src_path = "/Users/ashikashrivathsa/Documents/MS/semesters/spring2020/BigData/homeworks/HW2/"
business = sc.textFile(src_path + "business.csv").map(lambda x: x.split("::"))
review = sc.textFile(src_path + "review.csv").map(lambda x: x.split("::"))

business = business.map(lambda x: (x[0], (x[1], x[2])))
review = review.map(lambda x: (x[2], (float(x[3])))).reduceByKey(lambda x, y: (x + y)/2)

top_ten_review = sc.parallelize(review.top(10, key=lambda x: x[1]))
top_ten_review = top_ten_review.join(business).distinct()
top_ten_review = top_ten_review.map(lambda x: x[0] + "\t" + x[1][1][0] + "\t" + x[1][1][1] + "\t" + str(x[1][0]))
top_ten_review = top_ten_review.map(lambda x: x.replace("(", "[")).map(lambda x: x.replace(")", "]"))

top_ten_review.coalesce(1).saveAsTextFile(src_path + "output_q4")
