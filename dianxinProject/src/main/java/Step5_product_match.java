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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
/*
    5、产品匹配输出数据
 */
public class Step5_product_match {
    public static class MyMap extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fs = (FileSplit) context.getInputSplit();
            Path path = fs.getPath();
            String[] splits = value.toString().split("\\|");
            if(path.toString().contains("product")){
                context.write(new Text(splits[0]), new Text("a|"+splits[1]+"|"+splits[2]+"|"+splits[3]+"|"
                +splits[4]+"|"+splits[5]+"|"+splits[6]+"|"+splits[7]+"|"+splits[8]));
            }else{
                if(Integer.parseInt(splits[4].trim())!=0){
                    context.write(new Text(splits[0]), new Text("b|"+splits[1]));
                }
            }
        }
    }
    public static class MyReduce extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String product = null;
            String action = null;
            for (Text value : values){
                String[] splits = value.toString().split("\\|");
                if(splits[0].equals("a")){
                    product = splits[1]+"|"+splits[2]+"|"+splits[3]+"|"
                            +splits[4]+"|"+splits[5]+"|"+splits[6]+"|"+splits[7]+"|"+splits[8];
                }else{
                    action = splits[1];
                }
            }
            if(product!=null && action!=null){
                // 用户ID | 产品类型 | 产品id | 产品特征值… | 行为id
                context.write(new Text(action+"|"+product+"|"+key.toString()), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step5_product_match.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        FileInputFormat.setInputPaths(job, new Path(args[0]), new Path(args[1]));
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        FileInputFormat.setInputPaths(job, new Path("file:/F:\\dx_proj\\output\\step2"),
                new Path("file:/F:\\dx_proj\\peizhi\\t_dx_product_msg_addr.txt"));
        FileOutputFormat.setOutputPath(job, new Path("file:/F:\\dx_proj\\output\\step5"));
        job.waitForCompletion(true);
    }
}
