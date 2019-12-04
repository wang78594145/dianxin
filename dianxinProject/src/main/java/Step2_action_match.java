package dianxinProject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
/*
    2、行为匹配输出数据
 */
public class Step2_action_match {

    // 重写Com555arator方法,使匹配级别为3的都在匹配级别为2的前面...
    public static class tm_Comparator implements Comparator<String> {
        public int compare(String o1, String o2) {
            return o2.compareTo(o1);
        }
    }

    public static class MyMap extends Mapper<LongWritable,Text,Text,NullWritable> {

        // Map<一级域名，TreeMap<匹配级别+匹配地址, 需要的字段>>,
        private static Map<String, TreeMap<String, String>> joinData = new HashMap<String, TreeMap<String, String>>();

        protected void setup(Context context) throws IOException, InterruptedException {

            // 读取匹配文件的数据
//            BufferedReader br = new BufferedReader(new FileReader("t_dx_basic_msg_addr.txt"));

            Path[] paths = DistributedCache.getLocalCacheFiles(context.getConfiguration());
            // 我们这里只缓存了一个文件，所以取第一个即可，创建BufferReader去读取
            BufferedReader br = new BufferedReader(new FileReader(paths[0].toString()));

            String line = "";
            while ((line = br.readLine()) != null) {
                String lines[] = line.split("\\|",-1);

                // 一级域名作为map的key9
                String m_key = lines[1];

                // 如果key不存在,则new TreeMap()后,直接添加到joinData中
                if (!joinData.containsKey(m_key)) {
                    TreeMap<String, String> tm = new TreeMap<String, String>(new tm_Comparator());
                    tm.put(lines[3] + "|" + lines[2], lines[0] + "|" + lines[4] + "|" + lines[5]);
                    joinData.put(m_key, tm);
                } else {
                    // 如果key已存在,则添加到对应的TreeMap中
                    // domain = 匹配级别 | 匹配地址 | 行为ID | 是否产品 | 预购类型
                    joinData.get(m_key).put(lines[3] + "|" + lines[2], lines[0] + "|" + lines[4] + "|" + lines[5]);
                }
            }
            br.close();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String lines[] = value.toString().split("\\|",-1);
            String user_ID = lines[0];
            String domain = lines[1];
            String url = lines[2];
            TreeMap<String, String> tm = joinData.get(domain);

            // 先判断key是否存在,若存在则得到TreeMap
            if (tm != null) {
                Set<String> set = tm.keySet();
                String myData = "";

                // 得到TreeMap,并获取key,对key做迭代
                for (String s : set) {
                    String match = s.split("\\|")[1];
                    if (url.contains(match)) {
                        myData = tm.get(s);
                        break;
                    }
                }
                if (myData.length() != 0) {
                    String[] s = myData.split("\\|");
                    context.write(new Text(s[0] + "|" + user_ID + "|" + s[1] + "|" + url + "|" + s[2]), NullWritable.get());
                }
            }
        }
    }
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
            Configuration conf = new Configuration();
//            conf.set("fs.defaultFS", "hdfs://master:9000");
            Job job = Job.getInstance(conf);
            job.addCacheFile(new URI("/dx_proj/data/peizhi/t_dx_basic_msg_addr.txt"));
//            job.addCacheFile(new URI(args[0]));
            job.setJarByClass(Step2_action_match.class);
            job.setMapperClass(MyMap.class);
            job.setNumReduceTasks(0);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(NullWritable.class);
//            FileInputFormat.addInputPath(job, new Path(args[1]));
//            FileOutputFormat.setOutputPath(job, new Path(args[2]));
            FileInputFormat.addInputPath(job, new Path("file:/F:\\dx_proj\\output\\step11"));
            FileOutputFormat.setOutputPath(job, new Path("file:/F:\\dx_proj\\output\\step22"));
            job.waitForCompletion(true);
        }
}
