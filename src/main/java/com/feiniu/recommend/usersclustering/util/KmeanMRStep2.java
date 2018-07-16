package com.feiniu.recommend.usersclustering.util;

import com.feiniu.recommend.usersclustering.entity.ParticleModel;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.*;


/**
 * Step2:迭代计算聚类结果，map计算相似度，reduce更新簇中心
 * <p>
 * Created by yijie.pang on 2016/6/7.
 */
public class KmeanMRStep2 {
    /*
     * main函数中加载簇中心信息
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: Data Deduplication <in> <in> <out>");
            System.exit(2);
        }

        //加载簇中心
        DistributedCache.createSymlink(conf);
        FileSystem fs = FileSystem.get(conf);
        Path clusterCenter = new Path(otherArgs[0]);
        FileStatus[] user_stat = fs.listStatus(clusterCenter);
        for (FileStatus f : user_stat) {
            if (f.getPath().getName().indexOf("_SUCCESS") == -1) {
                DistributedCache.addCacheFile(f.getPath().toUri(), conf);
            }
        }

        if (fs.exists(new Path(otherArgs[2]))) {
            fs.delete(new Path(otherArgs[2]));
        }

        Job job = new Job(conf, "KmeanMRStep2");
        job.setJarByClass(KmeanMRStep2.class);
        job.setMapperClass(KmeanMRStep2Map.class);
        job.setReducerClass(KmeanMRStep2Reduce.class);

        //设置输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        //设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        job.waitForCompletion(true);


    }

    public static class KmeanMRStep2Map extends Mapper<Object, Text, Text, Text> {
        List<ParticleModel> clusterCenter = new ArrayList<ParticleModel>();

        /*
         * 加载簇中心
         */
        protected void setup(Context context)
                throws IOException, InterruptedException {
            System.out.println("========================================================");
            Configuration conf = context.getConfiguration();
            Path[] file = DistributedCache.getLocalCacheFiles(conf);
            FileSystem fs = FileSystem.getLocal(conf);
            String line = null;
            for (Path path : file) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(path)));
                while ((line = reader.readLine()) != null) {
                    String[] tmp = line.split("\t");
                    String[] array = tmp[0].split(":")[2].split(",");
                    String id = line.split(":")[1];
                    double[] x = new double[16];

                    if (array.length != 17) {
                        continue;
                    }
                    for (int i = 0; i < 16; i++) {
                        x[i] = Double.parseDouble(array[i]);
                    }
                    ParticleModel pm = new ParticleModel();

                    pm.setId(id);
                    pm.setX(x);
                    pm.setY(Double.parseDouble(array[16]));
                    clusterCenter.add(pm);
                }
            }
        }

        /*
         * 计算相似度
         */
        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] line = value.toString().split(":")[1].split(",");
            String id = value.toString().split(":")[0];
            double[] x = new double[16];
            if (line.length == 17) {
                ParticleModel pmSample = new ParticleModel();
                for (int i = 0; i < 16; i++) {
                    x[i] = Double.parseDouble(line[i]);
                }
                pmSample.setId(id);
                pmSample.setX(x);
                pmSample.setY(Double.parseDouble(line[16]));

                String nearlyCluster = getNearlyCluster(pmSample);
                String[] temp = nearlyCluster.split(":");
                if (temp.length == 2) {
                    context.write(new Text("cluster" + temp[0]), new Text(temp[1]));
                }
            }
        }

        private String getNearlyCluster(ParticleModel pmSample) {

            HashMap<String, Double> nearlyMap = new HashMap<String, Double>();

            for (int k = 0; k < clusterCenter.size(); k++) {
                StringBuffer sb = new StringBuffer();
                StringBuilder str_x = new StringBuilder();
                for (int i = 0; i < 16; i++) {
                    str_x.append(pmSample.x[i]).append(",");
                }
                double sim = getSimilarity(pmSample, clusterCenter.get(k));

                sb.append(k + 1).append(":").append(pmSample.id).append(",").append(str_x).append(pmSample.y);
                nearlyMap.put(sb.toString(), sim);
            }

            //进行降序
            List<Map.Entry<String, Double>> list_cos = new ArrayList<Map.Entry<String, Double>>(
                    nearlyMap.entrySet());
            Collections.sort(list_cos,
                    new Comparator<Map.Entry<String, Double>>() {
                        // 升序排序
                        public int compare(Map.Entry<String, Double> o1,
                                           Map.Entry<String, Double> o2) {
                            return o1.getValue().compareTo(o2.getValue());
                        }
                    });

            //获取第一个元素，也即最近的一个点
            String nearlyCluster = list_cos.get(0).getKey();

            return nearlyCluster;
        }

        private double getSimilarity(ParticleModel pmSample, ParticleModel pmCluster) {
            double sum_x = 0.0;
            double sum_y = 0.0;
            for (int i = 0; i < 16; i++) {
                sum_x = sum_x + (pmSample.x[i] - pmCluster.x[i]) * (pmSample.x[i] - pmCluster.x[i]);
            }

            sum_y = (pmSample.y - pmCluster.y) * (pmSample.y - pmCluster.y);

            //为特征向量赋权值
            return Math.sqrt(0.6 * sum_x + 0.4 * sum_y);
        }


    }

    /*
     * Reduce更新簇中心
     */
    public static class KmeanMRStep2Reduce extends Reducer<Text, Text, Text, Text> {

        int counter = 0;

        @Override
        protected void reduce(Text key, Iterable<Text> value, Context context)
                throws IOException, InterruptedException {
            //获取簇编号
            String clusterNum = key.toString();

            StringBuilder sbPoint = new StringBuilder();
            List<ParticleModel> clusterInfo = new ArrayList<ParticleModel>();
            for (Text val : value) {
                System.out.println("********************测试1***************************");
                System.out.println(val.toString());
                String[] temp = val.toString().split(",");
                double[] x = new double[16];
                String id;

                if (temp.length != 18) {
                    continue;
                }
                id = temp[0];
                for (int i = 0; i < 16; i++) {
                    x[i] = Double.parseDouble(temp[i+1]);
                }

                ParticleModel pm = new ParticleModel();
                StringBuilder str = new StringBuilder();
                pm.setId(id);
                pm.setX(x);
                pm.setY(Double.parseDouble(temp[17]));
                for (int i = 0; i < 16; i++) {
                    str.append(pm.x[i]).append(",");
                }
                System.out.println("********************测试2***************************");
                System.out.println(pm.id+":"+str);
                clusterInfo.add(pm);
                sbPoint.append(pm.id).append(",").append(str).append(pm.y).append("#");
            }

            ParticleModel newClusterCenter = getNewClusterCenter(clusterInfo);
            StringBuilder sb = new StringBuilder();
            StringBuilder str_x = new StringBuilder();

            if (newClusterCenter != null) {
                for (int i = 0; i < 16; i++) {
                    str_x.append(newClusterCenter.getX()[i]).append(",");
                }
                sb.append(clusterNum).append(":").append(newClusterCenter.getId()).append(":")
                        .append(str_x).append(newClusterCenter.getY());
                context.write(new Text(sb.toString()), new Text(sbPoint.toString()));
            }

        }

        /*
         * 更新簇中心
         */
        public ParticleModel getNewClusterCenter(List<ParticleModel> clusterInfo) {

            double[] sumX = new double[16];
            double sumY = 0.0;
            double[] x = new double[16];
            DecimalFormat df = new DecimalFormat("#.###");
            //初始化赋值
            for (int i = 0; i < 16; i++) {
                sumX[i] = 0.0;
            }

            for (ParticleModel pm : clusterInfo) {

                for (int i = 0; i < 16; i++) {
                    sumX[i] = sumX[i] + pm.getX()[i];
                }

                sumY += pm.getY();
            }
            for (int i = 0; i < 16; i++) {
                x[i] = Double.parseDouble(df.format(sumX[i] / clusterInfo.size()));
            }

            ParticleModel pm = new ParticleModel();
            pm.setId("id" + counter);
            pm.setX(x);
            pm.setY(Double.parseDouble(df.format(sumY / clusterInfo.size())));
            counter++;
            return pm;
        }
    }
}
