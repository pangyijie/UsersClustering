package com.feiniu.recommend.usersclustering.util;

import org.apache.log4j.Logger;

/**
 * KMeans运行主程序
 * <p>
 * Created by yijie.pang on 2016/6/7.
 */
public class KmeansDriver {
    private static Logger log = Logger.getLogger(KmeansDriver.class);

    //迭代次数
    private static int iteratorNum = 20;

    //样本数据输入路径
    private static String dataInput = "/user/hadoop/pyj/kmeans/data/";

    //上一次迭代结果路径，初始为第一次随机初始簇中心路径
    private static String lastInput = "/user/hadoop/pyj/kmeans/result/iterator0";

    public static void main(String[] args) {

        //Step1:随机初始化聚类中心
        try {
            log.info("************************* Run KmeanMRStep1 ******************************");
            String[] parmArgs1 = new String[3];

            //样本数据输入路径
            parmArgs1[0] = dataInput;

            //初始簇中心输出路径
            parmArgs1[1] = lastInput;

            //初始簇的个数
            parmArgs1[2] = "16";

            KmeanMRStep1.main(parmArgs1);

        } catch (Exception e) {
            e.printStackTrace();
        }

        //Step2:迭代计算聚类结果
        //可用org.apache.hadoop.mapreduce.Counter进行优化 在reduce判断簇中心是否变化，退出迭代。
        //如果新的簇中心心跟老的簇中心是一样的，那么相应的计数器加1

        try {
            log.info("************************* Run KmeanMRStep2 ******************************");

            int k = 1;
            while (k <= iteratorNum) {
                log.info("************************* 开始第 " + k + " 次迭代计算 ************************");

                String[] parmArgs2 = new String[3];

                parmArgs2[1] = dataInput;
                parmArgs2[0] = lastInput;

                String outPut = "/user/hadoop/pyj/kmeans/result/" + "iterator" + k;
                parmArgs2[2] = outPut;

                KmeanMRStep2.main(parmArgs2);

                lastInput = outPut;

                k++;


            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
