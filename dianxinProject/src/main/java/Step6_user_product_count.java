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
    6、用户产品统计输出数据
 */
public class Step6_user_product_count {
    public static class MyMap extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\|");
            context.write(new Text(splits[2]+"|"+splits[0]), new Text(splits[1]));
        }
    }
    public static class MyReduce extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int num = 0;
            String product_type = "";
            for (Text value : values){
                num++;
                product_type = value.toString();
            }
            context.write(new Text(key.toString()+"|"+num+"|"+product_type), NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step6_user_product_count.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.setInputPaths(job, new Path("file:/F:\\dx_proj\\server_output\\step5"));
        FileOutputFormat.setOutputPath(job, new Path("file:/F:\\dx_proj\\server_output\\step6"));
        job.waitForCompletion(true);
    }
}
