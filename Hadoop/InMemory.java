import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class InMemory {

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Text pair = new Text();
        private Text friends_list = new Text();
        HashMap<String, String> user_bday = new HashMap<>();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] mydata = value.toString().split("\\t");

            String input = context.getConfiguration().get("INPUT_PAIRS");

            String friend1 = mydata[0];
            if (mydata.length > 1) {
                List<String> friends = new LinkedList<>(Arrays.asList(mydata[1].split(",")));

                for (String friend2 : friends) {
                    if (Integer.parseInt(friend1) < Integer.parseInt(friend2)) {
                        pair.set(friend1 + "," + friend2);
                    } else {
                        pair.set(friend2 + "," + friend1);
                    }

                    if (input.equals(pair.toString())) {
                        friends_list.set(get_bday(friends));
                        context.write(pair, friends_list);
                    }
                }
            }
        }

        private String get_bday(List<String> friends) {
            List<String> friendsData = new ArrayList<>();

            for (String friend : friends) {
                friendsData.add(user_bday.get(friend));
            }
            return friendsData.toString();
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            super.setup(context);
            Configuration configuration = context.getConfiguration();

            Path part = new Path(configuration.get("USERDATA"));

            FileSystem fileSystem = FileSystem.get(configuration);
            FileStatus[] fileStatuses = fileSystem.listStatus(part);

            for (FileStatus status : fileStatuses) {
                Path path = status.getPath();

                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fileSystem.open(path)));
                String line = bufferedReader.readLine();

                while (line != null) {
                    String[] data = line.split(",");
                    user_bday.put(data[0], data[1] +  ":" + data[9]);
                    line = bufferedReader.readLine();
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            Text word = new Text();
            Text result = new Text();

            List<String> friends = new ArrayList<>();
            for (Text value : values) {
                String[] user = value.toString().replace(" ", "").
                        replace("[", "").replace("]", "").split(",");
                for (String i : user){
                    friends.add(i);
                }
            }
            Collections.sort(friends);

            List<String> mutualFriends = new ArrayList<>();
            for(int i = 0; i < friends.size(); i++ ) {
                if (i+1 < friends.size() && friends.get(i).equals(friends.get(i+1))) {
                    mutualFriends.add(friends.get(i));
                    i += 1;
                }
            }
            if (mutualFriends.size() > 0) {
                result.set(mutualFriends.toString());
                word.set(key);

                context.write(word, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();

        if (otherArgs.length != 4) {
            System.err.println("Usage: InMemory <soc-LiveJournal1Adj.txt> <userData.txt> <friend1,friend2> <out>");
            System.exit(2);
        }

        configuration.set("USERDATA", otherArgs[1]);
        configuration.set("INPUT_PAIRS", otherArgs[2]);

        Job job = Job.getInstance(configuration);
        job.setJobName("InMemory");
        job.setJarByClass(InMemory.class);
        job.setMapperClass(InMemory.Map.class);
        job.setReducerClass(InMemory.Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
