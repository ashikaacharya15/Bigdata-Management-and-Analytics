hdfs dfs -rm -r tmp/q1*
hdfs dfs -rm -r tmp/q2*
hdfs dfs -rm -r tmp/q3*
hdfs dfs -rm -r tmp/q4*

rm -rf q1*
rm -rf q2*
rm -rf q3*
rm -rf q4*

zip -d MutualFriends_Java7.jar META-INF/LICENSE
echo "-------------------- Question1 --------------------"
hadoop jar MutualFriends_Java7.jar MutualFriends /tmp/soc-LiveJournal1Adj.txt tmp/q1 > out.txt
echo "-------------------- Question2 --------------------"
hadoop jar MutualFriends_Java7.jar TopTen /tmp/soc-LiveJournal1Adj.txt tmp/q2_1 tmp/q2_2 > out2.txt
echo "-------------------- Question3 --------------------"
hadoop jar MutualFriends_Java7.jar InMemory /tmp/soc-LiveJournal1Adj.txt /tmp/userdata.txt 9999,44160 tmp/q3 > out3.txt
echo "-------------------- Question4 --------------------"
hadoop jar MutualFriends_Java7.jar JobChaining /tmp/soc-LiveJournal1Adj.txt /tmp/userdata.txt tmp/q4_1 tmp/q4_2 tmp/q4_3 > out4.txt


echo "-------------------- Retrieving all output files --------------------"
hdfs dfs -get tmp/q*
