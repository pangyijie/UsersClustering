package com.feiniu.recommend.usersclustering.entity;

/**
 * 质点特征模型
 * <p>
 * Created by yijie.pang on 2016/6/7.
 */
public class ParticleModel {

    //特征x：各物品种类数
    public double[] x;

    //特征y：用户消费水平
    public double y;

    //标量，标记质点
    public String id;

    public double[] getX() {
        return x;
    }

    public void setX(double[] x) {
        this.x = x;
    }

    public double getY() {
        return y;
    }

    public void setY(double y) {
        this.y = y;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
