import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopPopularLinks.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Link Count");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopPopularLinks.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "League Rank");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(IntWritable.class);
        jobB.setMapOutputValueClass(IntWritable.class);

        jobB.setMapperClass(LeagueRankMap.class);
        jobB.setReducerClass(LeagueRankReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopPopularLinks.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static ArrayList<Integer> readHDFSFile(String path, Configuration conf) throws IOException{
        Path pt=new Path(path);
        FileSystem fs = FileSystem.get(pt.toUri(), conf);
        FSDataInputStream file = fs.open(pt);
        BufferedReader buffIn=new BufferedReader(new InputStreamReader(file));

        ArrayList<Integer> leagueList = new ArrayList<Integer>();
        String line;
        while( (line = buffIn.readLine()) != null) {
        leagueList.add(Integer.parseInt(line));
        }
        return leagueList;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            StringTokenizer st = new StringTokenizer(line, " \t,;.?!-:@[](){}_*/");
            boolean firstElementTaken = true;
            while(st.hasMoreTokens()) {
                Integer currentToken = Integer.parseInt(st.nextToken());
                if(firstElementTaken) {
                    context.write(new IntWritable(currentToken), new IntWritable(0));
                    firstElementTaken = false;
                } else {
                    context.write(new IntWritable(currentToken), new IntWritable(1));
                }
            }
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
       @Override
       public void reduce(IntWritable key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for(IntWritable val : value) {
                sum += val.get();
            }
            LOG.info(key.get() + "\t" + sum);
            context.write(key, new IntWritable(sum));
        }
        }

    public static class LeagueRankMap extends Mapper<Text, Text, IntWritable, IntWritable> {
    ArrayList<Integer> leagueList;

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
        String leagueListPath = conf.get("league");
            this.leagueList = readHDFSFile(leagueListPath, conf);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            Integer mainPage = Integer.parseInt(key.toString());
            Integer linkedTo = Integer.parseInt(value.toString());

            if(this.leagueList.contains(mainPage)) {
                context.write(new IntWritable(mainPage), new IntWritable(linkedTo));
            }
        }

    }

    public static class LeagueRankReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    ArrayList<Integer> page = new ArrayList<Integer>();
    ArrayList<Integer> links = new ArrayList<Integer>();

        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                links.add(val.get());
            }
            page.add(key.get());
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            int rank = 0;
            int size = links.size();
            LOG.info("Logging: size of links - " + size );
            for(int i = 0; i < size; i++) {
                for(int j = 0; j < size; j++) {
                    if( (links.get(i) > links.get(j)) ) {
                        rank += 1;
                    } else {
                        continue;
                    }
                }
        LOG.info("Logging: " + page.get(i) + "\t" + rank);
                context.write(new IntWritable(page.get(i)), new IntWritable(rank));
                rank = 0;
            }
        }
}


}
                                                                                                   176,1         Bot
