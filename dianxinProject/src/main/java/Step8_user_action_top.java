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
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/*
    8、用户行为top提取
 */
public class Step8_user_action_top {

    public static class tm_Comparator implements Comparator<String> {
        public int compare(String o1, String o2) {
            int num1 = Integer.parseInt(o1.split("\\|")[1]);
            int num2 = Integer.parseInt(o2.split("\\|")[1]);
            return num1-num2;
        }
    }
    public static class MyMap extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\|");
            context.write(new Text(splits[1]), new Text(splits[0]+"|"+splits[2]));
        }
    }
    public static class MyReduce extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, String> top = new TreeMap<String, String>(new tm_Comparator());
            for (Text value : values){
                String[] splits = value.toString().split("\\|");
                top.put(splits[0]+"|"+splits[1], null);
            }
            Set<String> set = top.keySet();
            int rank = set.size();
            if(rank <= 5){
                for (String s : set){
                    String[] splits = s.split("\\|");
                    context.write(new Text(splits[0]+"|"+key.toString()+"|"+splits[1]+"|"+rank), NullWritable.get());
                    rank--;
                }
            }else{
                Object[] o = set.toArray();
                int a = 5;
                for (int i=rank-5; i<=rank-1; i++){
                    String[] splits = o[i].toString().split("\\|", -1);
                    context.write(new Text(splits[0]+"|"+key.toString()+"|"+splits[1]+"|"+a), NullWritable.get());
                    a--;
                }
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step8_user_action_top.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path("file:/F:\\dx_proj\\server_output\\step3"));
        FileOutputFormat.setOutputPath(job, new Path("file:/F:\\dx_proj\\server_output\\step8"));
        job.waitForCompletion(true);
    }
}
