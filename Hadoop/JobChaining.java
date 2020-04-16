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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class JobChaining {
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {

        private Text user_id = new Text();
        private Text friends_list = new Text();


        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] myData = value.toString().split("\\t");
            if (myData.length == 2) {
                user_id.set(myData[0]);
                friends_list.set(myData[1]);
                context.write(user_id, friends_list);
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Integer> {

        HashMap<String, Integer> user_age = new HashMap<>();

        public int calculate_age(String date) {

            int age = 0;
            try {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("MM/dd/yyyy");
                Date date1 = new Date();
                Date date2 = simpleDateFormat.parse(date);

                long difference = date1.getTime() - date2.getTime();
                age = (int) TimeUnit.DAYS.convert(difference, TimeUnit.MILLISECONDS);

            } catch (ParseException e) {
                e.printStackTrace();
            }
            return age / 365;
        }

        // maintains  a dict of <user_id, age> which reduce function can utilize late
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
                    user_age.put(data[0], calculate_age(data[9]));
                    line = bufferedReader.readLine();
                }
            }
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            List<Integer> age = new ArrayList<>();
            for (Text friends : values) {
                String[] data = friends.toString().split(",");

                for (String friend : data) {
                    age.add(user_age.get(friend));
                }
            }
            context.write(key, Collections.max(age));
        }
    }

    public static class Map2 extends Mapper<LongWritable, Text, LongWritable, Text> {

        private LongWritable max_age = new LongWritable();

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] data = value.toString().split("\\t");
            max_age.set(Integer.parseInt(data[1]));
            context.write(max_age, new Text(data[0]));
        }
    }

    // purpose of this Reducer is to sort users based on decreasing order of their oldest friends age.
    public static class Reduce2 extends Reducer<LongWritable, Text, Text, Integer> {

        public void reduce(LongWritable key, Iterable<Text> value, Context context) throws IOException, InterruptedException {

            for (Text user_id : value) {
                int max_age = (int) key.get();
                context.write(user_id, max_age);
            }
        }
    }

    public static class Map3 extends Mapper<LongWritable, Text, Text, LongWritable> {

        private LongWritable max_age = new LongWritable();
        int top_ten = 1;

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            if (top_ten <= 10) {
                String[] data = value.toString().split("\\t");
                max_age.set(Integer.parseInt(data[1]));
                context.write(new Text(data[0]), max_age);
                top_ten += 1;
            }
        }
    }

    public static class Reduce3 extends Reducer<Text, LongWritable, Text, Text> {

        HashMap<Integer, String> user_address = new HashMap<>();

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
                    user_address.put(Integer.valueOf(data[0]), data[3] + ", " + data[4] + ", " + data[5]);
                    line = bufferedReader.readLine();
                }
            }
        }

        public void reduce(Text key, Iterable<LongWritable> value, Context context)
                throws IOException, InterruptedException {

            for (LongWritable item : value) {
                String data = user_address.get(Integer.valueOf(key.toString())) + ", " + item;
                context.write(key, new Text(data));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 5) {
            System.err.println("Usage: JobChaining <soc-LiveJournal1Adj.txt> <userData.txt> <out_1> <out_2> <out_3>");
            System.exit(2);
        }

        conf.set("USERDATA" , otherArgs[1]);
        Job job = Job.getInstance(conf);
        job.setJobName("JobChaining_1");
        job.setJarByClass(JobChaining.class);
        job.setMapperClass(JobChaining.Map.class);
        job.setReducerClass(JobChaining.Reduce.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Integer.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

        Boolean isFirstJobDone = job.waitForCompletion(true);

        if (isFirstJobDone) {
            Configuration config = new Configuration();

            Job job2 = Job.getInstance(config);
            job2.setJobName("JobChaining_2");
            job2.setJarByClass(JobChaining.class);
            job2.setMapperClass(JobChaining.Map2.class);
            job2.setReducerClass(JobChaining.Reduce2.class);

            job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);

            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Integer.class);

            String file = otherArgs[2] + "/part-r-00000";
            FileInputFormat.addInputPath(job2, new Path(file));
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3]));

            Boolean isSecondJobDone = job2.waitForCompletion(true);

            if (isSecondJobDone){
                Configuration config3 = new Configuration();

                config3.set("USERDATA" , otherArgs[1]);

                Job job3 = Job.getInstance(config3);
                job3.setJobName("JobChaining_3");
                job3.setJarByClass(JobChaining.class);
                job3.setMapperClass(JobChaining.Map3.class);
                job3.setReducerClass(JobChaining.Reduce3.class);

                job3.setMapOutputKeyClass(Text.class);
                job3.setMapOutputValueClass(LongWritable.class);
                job3.setOutputKeyClass(Text.class);
                job3.setOutputValueClass(Text.class);

                String file2 = otherArgs[3] + "/part-r-00000";
                FileInputFormat.addInputPath(job3, new Path(file2));
                FileOutputFormat.setOutputPath(job3, new Path(otherArgs[4]));
                System.exit(job3.waitForCompletion(true) ? 0 : 1);
            }
        }
    }
}
