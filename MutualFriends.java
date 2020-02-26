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

public class MutualFriends {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Text pair = new Text();
        private Text friends_list = new Text();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\\t");

            String friend1 = data[0];
            if (data.length > 1) {
                List<String> friends = new LinkedList<String>(Arrays.asList(data[1].split(",")));

                for (String friend2 : friends) {
                    if (Integer.parseInt(friend1) < Integer.parseInt(friend2)) {
                        pair.set(friend1 + ", " + friend2);
                    } else {
                        pair.set(friend2 + ", " + friend1);
                    }
                    friends_list.set(data[1]);
                    context.write(pair, friends_list);
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        private Text mutual_friends = new Text();
        Text pair = new Text();

        HashMap<String, String> exp_output = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<Integer> friends = new ArrayList<>();
            for (Text item : values) {
                String[] data = item.toString().split(",");
                for (String i : data) {
                    friends.add(Integer.parseInt(i));
                }
            }

            Collections.sort(friends);

            List<Integer> mutualFriends = new ArrayList<>();
            for(int i = 0; i < friends.size(); i++ ) {
                if (i+1 < friends.size() && friends.get(i).equals(friends.get(i+1))) {
                    mutualFriends.add(friends.get(i));
                    i += 1;
                }
            }

            String friend1 = key.toString().split(",")[0];
            String friend2 = key.toString().replace(" ", "").split(",")[1];

            if (exp_output.containsKey(friend1) && exp_output.get(friend1).equals(friend2)) {
                pair.set(key);
                if (mutualFriends.size() > 0) {
                    mutual_friends.set(mutualFriends.toString().replace("[", "").replace("]", ""));
                    context.write(pair, mutual_friends);
                } else {
                    mutual_friends.set("");
                    context.write(pair, mutual_friends);
                }
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // (0,1), (20, 28193), (1, 29826), (6222, 19272), (28041, 28056)
            exp_output.put(String.valueOf(0), String.valueOf(1));
            exp_output.put(String.valueOf(20), String.valueOf(28193));
            exp_output.put(String.valueOf(1), String.valueOf(29826));
            exp_output.put(String.valueOf(6222), String.valueOf(19272));
            exp_output.put(String.valueOf(28041), String.valueOf(28056));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 2) {
            System.err.println("Usage: MutualFriends <soc-LiveJournal1Adj.txt> <out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf);
        job.setJobName("mutualFriends");
        job.setJarByClass(MutualFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
