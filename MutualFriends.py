from pyspark import SparkContext
import collections


def make_friends(line):
    all_pairs = []

    friend1 = line[0]
    friends_list = line[1].split(",")

    for friend2 in friends_list:
        if friend2 != '':
            if int(friend1) < int(friend2):
                pair = (friend1 + ", " + friend2, friends_list)
            else:
                pair = (friend2 + ", " + friend1, friends_list)
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
    return str(pair + "\t" + str(len(common_friends)))


sc = SparkContext("local", "Mutual Friends")

src_path = "/Users/ashikashrivathsa/Documents/MS/semesters/spring2020/BigData/homeworks/HW2/"
file = sc.textFile(src_path + "soc-LiveJournal1Adj.txt")

friend_pairs = file.map(lambda line: line.split("\t")).filter(lambda line: len(line) == 2).flatMap(make_friends)

mutual_friends = friend_pairs.reduceByKey(lambda x, y: x + y).map(find_common_friends).sortBy(lambda x: x.split("\t")[1], False)

mutual_friends.saveAsTextFile(src_path + "output_q1")

