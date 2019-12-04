package dianxinProject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
   3、PV 用户行为统计输出数据
 */
public class Step3_user_action_count {

    public static class MyMap extends Mapper<LongWritable,Text,Text,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\|",-1);
            // 一级域名000000000000 | 用户ID
            if(splits.length==5){
                context.write(new Text(splits[0].substring(0,6)+"000000000000"+"|"+splits[1]),new IntWritable(1));
            }

        }
    }

    public static class MyReduce extends Reducer<Text,IntWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            for (IntWritable value : values){
                num += value.get();
            }
            context.write(new Text(key.toString()+"|"+num), NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://172.16.11.242:8020");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step3_user_action_count.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
//        FileInputFormat.addInputPath(job, new Path("file:/F:\\dx_proj\\output\\step2"));
//        FileOutputFormat.setOutputPath(job, new Path("file:/F:\\dx_proj\\output\\step3"));
        job.waitForCompletion(true);
    }
}
