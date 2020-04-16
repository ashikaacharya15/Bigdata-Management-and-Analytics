from pyspark import SparkContext

sc = SparkContext("local", "Standford Business Reviews")

src_path = "/Users/ashikashrivathsa/Documents/MS/semesters/spring2020/BigData/homeworks/HW2/"
business = sc.textFile(src_path + "business.csv").map(lambda x: x.split("::"))
review = sc.textFile(src_path + "review.csv").map(lambda x: x.split("::"))

business = business.filter(lambda x: str(x[1]).lower().__contains__("stanford")).map(lambda x: (x[0], (x[1], x[2])))
review = review.map(lambda x: (x[2], (x[1], x[3])))

business_review = review.join(business).distinct()
business_review = business_review.map(lambda x: x[1][0][0] + "\t" + x[1][0][1])

business_review.coalesce(1).saveAsTextFile(src_path + "output_q3")
