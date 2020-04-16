import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class TopTen {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Text user_id = new Text();
        private Text friends_list = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\\t");

            String friend1 = data[0];
            if (data.length > 1) {
                List<String> friends = new LinkedList<>(Arrays.asList(data[1].split(",")));

                for (String friend2 : friends) {
                    if (Integer.parseInt(friend1) < Integer.parseInt(friend2)) {
                        user_id.set(friend1 + ", " + friend2);
                    } else {
                        user_id.set(friend2 + ", " + friend1);
                    }
                    friends_list.set(data[1]);
                    context.write(user_id, friends_list);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        private Text friends_count = new Text();
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<Integer> friends = new ArrayList<>();
            for (Text item : values) {
                String[] data = item.toString().split(",");
                for (String i : data) {
                    friends.add(Integer.parseInt(i));
                }
            }
            Collections.sort(friends);

            Integer count = 0;
            for (int i = 0; i < friends.size(); i++) {
                if (i + 1 < friends.size() && friends.get(i).equals(friends.get(i + 1))) {
                    count += 1;
                }
            }
            if (count > 0) {
                friends_count.set(count.toString());
                context.write(key, friends_count);
            }
        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, LongWritable, Text> {
        private LongWritable friends_count = new LongWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] data = value.toString().split("\\t");
            friends_count.set(Integer.parseInt(data[1]));
            context.write(friends_count, new Text(data[0]));
        }
    }

    public static class Reduce2 extends Reducer<LongWritable, Text, Text, Integer> {

        static int top_ten = 1;
        public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
            for (Text item : value) {
                if (top_ten <= 10) {
                    int friends = (int) key.get();
                    context.write(item, friends);
                    top_ten += 1;
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 3) {
            System.err.println("Usage: TopTen <soc-LiveJournal1Adj.txt> <out1> <out2>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf);
        job.setJobName("TopTen_part1");
        job.setJarByClass(TopTen.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        Boolean isFirstJobDone = job.waitForCompletion(true);
        if (isFirstJobDone) {
            Configuration config = new Configuration();

            Job job2 = Job.getInstance(config);
            job2.setJobName("TopTen_part2");
            job2.setJarByClass(TopTen.class);
            job2.setMapperClass(Map2.class);
            job2.setReducerClass(Reduce2.class);

            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Integer.class);

            String file = otherArgs[1] + "/part-r-00000";
            FileInputFormat.addInputPath(job2, new Path(file));
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}

