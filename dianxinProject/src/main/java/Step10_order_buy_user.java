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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

public class Step10_order_buy_user {
    public static class MyMap extends Mapper<LongWritable,Text,Text,Text> {
        public static HashMap<String,String> typeData = new HashMap<String, String>();
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new FileReader("car_type.txt"));
            String line = null;
            while((line=br.readLine()) != null){
                typeData.put(line.split("\\|")[1], line.split("\\|")[2]);
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\|");
            if(typeData.containsKey(splits[2])){
                context.write(new Text(splits[0]), new Text(typeData.get(splits[2])));
            }
        }
    }
    public static class MyReduce extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int car_num = 0;
            int house_num = 0;

            for (Text value : values){
                int type = Integer.parseInt(value.toString());
                if(type == 1){
                    car_num++;
                }else if(type == 2){
                    house_num++;
                }
            }
            if (car_num!=0 && house_num!=0){
                context.write(new Text(key.toString()+"|"+1+":"+car_num+","+2+":"+house_num), NullWritable.get());
            }else if (car_num!=0){
                context.write(new Text(key.toString()+"|"+1+":"+car_num), NullWritable.get());
            }else if (house_num!=0){
                context.write(new Text(key.toString()+"|"+2+":"+house_num), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://172.16.11.242:8020");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step10_order_buy_user.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        job.addCacheFile(new URI(args[0]));
//        FileInputFormat.addInputPath(job, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.addCacheFile(new URI("hdfs://master:9000/dx_proj/data/peizhi/car_type.txt"));
        FileInputFormat.addInputPath(job, new Path("file:/F:\\dx_proj\\server_output\\step5"));
        FileOutputFormat.setOutputPath(job, new Path("file:/F:\\dx_proj\\server_output\\step10"));
        job.waitForCompletion(true);
    }
}
