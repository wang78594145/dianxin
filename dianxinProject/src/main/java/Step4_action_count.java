package dianxinProject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/*
    4、行为统计输出数据
    pv：page value 访问次数
    uv：unique visitor 访问人数
 */
public class Step4_action_count {
    public static class MyMap extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\||\\s");
            context.write(new Text(splits[0]), new Text(splits[1]+"|"+splits[2]));
        }
    }
    public static class MyReduce extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int pv = 0;
            int uv = 0;
            for(Text value : values){
                String[] splits = value.toString().split("\\|");
                pv++;
                uv += Integer.parseInt(splits[1]);
            }
            // 行为ID | 访问人数uv | 访问次数pv
            context.write(new Text(key.toString()+"|"+pv+"|"+uv), NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step4_action_count.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path("file:/F:\\dx_proj\\server_output\\step3"));
        FileOutputFormat.setOutputPath(job, new Path("file:/F:\\dx_proj\\server_output\\step4"));
        job.waitForCompletion(true);
    }
}
