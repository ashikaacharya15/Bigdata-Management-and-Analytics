from pyspark import SparkContext
import collections


def make_friends(line):
    all_pairs = []

    friend1 = line[0]
    friends_list = line[1].split(",")

    for friend2 in friends_list:
        if friend2 != '':
            if int(friend1) < int(friend2):
                pair = (friend1 + "," + friend2, friends_list)
            else:
                pair = (friend2 + "," + friend1, friends_list)
            all_pairs.append(pair)
    return all_pairs


def find_common_friends(pairs_list):
    pair = str(pairs_list[0])
    friends_list = pairs_list[1]

    counter = collections.Counter(friends_list)
    common_friends = []
    for key, value in counter.items():
        if value > 1:
            common_friends.append(int(key))
    common_friends.sort()
    if len(common_friends) > 0:
        return str(pair + "\t" + str(common_friends).replace("[","").replace("]",""))
    else:
        return str(pair)


def format_details(user):
    user = user[1]
    count = user[0][1]
    user1 = user[1]
    user2 = user[0][2]
    details = count + "\t" + user1[0] + "\t" + user1[1] + "\t" + user1[2] + "\t" + user2[0] + "\t" + user2[1] + "\t" + user2[2]
    return details


sc = SparkContext("local", "Top ten Mutual Friends")

src_path = "/Users/ashikashrivathsa/Documents/MS/semesters/spring2020/BigData/homeworks/HW2/"

user_data = sc.textFile(src_path + "userdata.txt").map(lambda x: x.split(","))
user_data = user_data.map(lambda x: (x[0], (x[1], x[2], x[3])))

file = sc.textFile(src_path + "soc-LiveJournal1Adj.txt")
friend_pairs = file.map(lambda line: line.split("\t")).filter(lambda line: len(line) == 2).flatMap(make_friends)

mutual_friends = friend_pairs.reduceByKey(lambda x, y: x + y).map(find_common_friends).filter(lambda x: x is not None)

top_ten = mutual_friends.filter(lambda x: len(x.split("\t")) == 2).map(lambda x: (x.split("\t")[0], len(x.split("\t")[1].split(","))))

top_ten = top_ten.sortBy(keyfunc=lambda x: x[0]).top(10, key=lambda x: x[1])

top_ten = sc.parallelize(top_ten).map(lambda x: (x[0].split(",")[0], (x[0].split(",")[1], str(x[1]))))

top_ten_details = top_ten.join(user_data).map(lambda x: (x[1][0][0], (x[0], x[1][0][1], x[1][1])))

top_ten_details = top_ten_details.join(user_data).map(format_details)

top_ten_details.coalesce(1).saveAsTextFile(src_path + "output_q2")
