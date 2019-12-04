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
    7、产品统计输出数据
 */
public class Step7_product_count {
    public static class MyMap extends Mapper<LongWritable,Text,Text,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\|");
            context.write(new Text(splits[0]+"|"+splits[3]), new IntWritable(Integer.parseInt(splits[2])));
        }
    }
    public static class MyReduce extends Reducer<Text,IntWritable,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int people_num =0;
            int num =0;
            for (IntWritable value : values){
                people_num++;
                num += value.get();
            }
            String[] splits = key.toString().split("\\|");
            context.write(new Text(splits[0]+"|"+people_num+"|"+num+"|"+splits[1]), NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step7_product_count.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path("file:/F:\\dx_proj\\server_output\\step6"));
        FileOutputFormat.setOutputPath(job, new Path("file:/F:\\dx_proj\\server_output\\step7"));
        job.waitForCompletion(true);
    }
}
