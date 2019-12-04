package dianxinProject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.net.DNS;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
/*
    1、清洗后输出数据
 */
public class Step1_clearout {

    public static class MyMap extends Mapper<LongWritable,Text,Text,NullWritable>{
        private String domain;
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\u0001");
            // 去除无效数据
            if(splits.length!=15 || splits[1]==null || splits[14]==null || splits[1].equals("http://") || splits[1].equals("https://"))
                return;
            String user_ID = splits[1];
            String url = splits[14];

            // 去除非用户行为数据
            String pattern = "(jpg|png|bmp|jpeg|tif|gif|psd|ico|pdf|css|tmp|js|gz|rar|gzip|zip|txt|csv|xlsx|xls)$";
            Pattern r1 = Pattern.compile(pattern);
            Matcher m1 = r1.matcher(url);
            if(m1.find())
                return;

            // 保证数据完 整性
            if(!url.startsWith("http://") && !url.startsWith("https://"))
                url = "http://" + url;

            // 获取域名
            domain = url.split("/", -1)[2];
            // 去除端口
            if (domain.indexOf(":") >= 0){
                domain = url.split("\\:", -1)[0];
            }

            // 提取有效一级域名
            String pattern2 = "((\\w*)\\.(com.cn|net.cn|gov.cn|org.nz|org.cn|com|net|org|gov|cc|biz|info|cn|hk|in|am|im|fm|tv|co|me|us|io|mobi|pw|so|gs|top|la))";
            Pattern r2 = Pattern.compile(pattern2);
            Matcher m2 = r2.matcher(domain);
            while (m2.find())
                domain = m2.group(1);

            // 保证一级域名是有效的IP地址
            String ip = "([1-9]|[1-9]|\\d|1\\d{2}|2[0-4]\\d|25[0-5])(\\.([1-9]|[1-9]|\\d|1\\d{2}|2[0-4]\\d|25[0-5])){3}";
            if (domain.matches(ip))
                domain = domain;

            context.write(new Text(user_ID+"|"+domain+"|"+url), NullWritable.get());
        }
    }


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
//        conf.set("fs.defaultFS", "hdfs://master:9000");
        Job job = Job.getInstance(conf);
//        job.setJar("F:\\work_space\\IDEA_Workspace\\hadoop_project\\out\\artifacts\\test_yarn\\test_yarn.jar");
        job.setJarByClass(Step1_clearout.class);
        job.setMapperClass(MyMap.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
//        FileInputFormat.addInputPath(job, new Path(args[0]));
//        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileInputFormat.addInputPath(job, new Path("/dx_proj/data/000002_0"));
        FileOutputFormat.setOutputPath(job, new Path("/dx_proj/output/yarn11"));
        job.waitForCompletion(true);
    }
}
