package com.feiniu.recommend.usersclustering.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

/**
 * kmeans聚类第一步：随机生成k个聚类中心
 * <p>
 * Created by yijie.pang on 2016/6/7.
 */
public class KmeanMRStep1 {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: Data Deduplication <in> <out> <cluster num>");
            System.exit(2);
        }
        conf.set("ClusterNum", otherArgs[2]);
        FileSystem fs = FileSystem.get(conf);
        Path centerPath = new Path(otherArgs[1]);
        if (fs.exists(centerPath)) {
            fs.delete(centerPath);
        }

        Job job = new Job(conf, "KmeanMRStep1");
        job.setJarByClass(KmeanMRStep1.class);
        job.setMapperClass(KmeanMRStep1Map.class);
        job.setReducerClass(KmeanMRStep1Reduce.class);

        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        //设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.waitForCompletion(true);
    }

    public static class KmeanMRStep1Map extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            if (value.toString() != null) {
                context.write(new Text("kmeans"), value);
            }
        }
    }

    public static class KmeanMRStep1Reduce extends Reducer<Text, Text, Text, Text> {
        int clusterNum;
        private static Set<Integer> indexSet = new HashSet<Integer>();

        @Override
        protected void setup(Context context)
                throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            clusterNum = Integer.parseInt(conf.get("ClusterNum"));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            List<String> dataList = new ArrayList<String>();
            for (Text val : value) {
                dataList.add(val.toString());
            }

            for (int k = 1; k <= clusterNum; k++) {
                int index = getIndex(dataList.size());
                String point = dataList.get(index);
                if (point != null) {
                    StringBuffer sb = new StringBuffer();
                    sb.append("cluster" + k).append(":").append(point);
                    context.write(new Text(sb.toString()), new Text());
                }
            }
        }

        /*
         * 生成不重复的随机数
         */
        public static int getIndex(int size) {

            int res;
            Random random = new Random();
            while (true) {
                int index = random.nextInt(size);
                if (!indexSet.contains(index)) {
                    res = index;
                    indexSet.add(index);
                    break;
                }
            }

            return res;
        }
    }
}
