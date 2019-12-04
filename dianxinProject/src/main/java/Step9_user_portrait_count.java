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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/*
    9、用户画像统计输出数据
 */
public class Step9_user_portrait_count {
    public static class MyMap extends Mapper<LongWritable,Text,Text,Text>{
        public static Map<String,String> linkData = new HashMap<String, String>();
        protected void setup(Context context) throws IOException, InterruptedException {
            BufferedReader br = new BufferedReader(new FileReader("t_dx_basic_classify_link.txt"));
            String line = null;
            while((line=br.readLine()) != null){
                String[] splits = line.split("\\|",-1);
                linkData.put(splits[1],splits[0]);
            }
        }
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\\|",-1);
            if(linkData.containsKey(splits[0])){
                context.write(new Text(linkData.get(splits[0])), new Text(splits[1]+"|"+splits[2]));
            }
        }
    }
    public static class MyReduce extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<Integer> list = new ArrayList<Integer>();
            ArrayList<String> user_ID = new ArrayList<String>();
            double sum = 0;   // 总次数
            double num = 0;   // 总人数
            for (Text value : values){
                String s = value.toString().split("\\|")[0];
                user_ID.add(s);
                int a = Integer.parseInt(value.toString().split("\\|")[1]);
                list.add(a);
                sum += a;
                num++;
            }
            double stand_dev = Tools.stand_dev(list);
            double avg = sum / num;

            for(int i=0; i<list.size(); i++){
                double biao_zhun_zhi = (list.get(i)-avg)/stand_dev+5;
                if(biao_zhun_zhi<0){
                    biao_zhun_zhi = 0;
                }else if(biao_zhun_zhi>10){
                    biao_zhun_zhi = 10;
                }
                context.write(new Text(user_ID.get(i)+"|"+key.toString()+"|"+list.get(i)+"|"+sum+"|"
                        +biao_zhun_zhi), NullWritable.get());
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://master:9000");
        Job job = Job.getInstance(conf);
        job.setJarByClass(Step9_user_portrait_count.class);
        job.setMapperClass(MyMap.class);
        job.setReducerClass(MyReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        job.addCacheFile(new URI(args[0]));
//        FileInputFormat.addInputPath(job, new Path(args[1]));
//        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.addCacheFile(new URI("hdfs://master:9000/dx_proj/data/peizhi/t_dx_basic_classify_link.txt"));
        FileInputFormat.addInputPath(job, new Path("file:/F:\\dx_proj\\server_output\\step8"));
        FileOutputFormat.setOutputPath(job, new Path("file:/F:\\dx_proj\\server_output\\step9"));
        job.waitForCompletion(true);
    }
}
