package com.feiniu.recommend.usersclustering.entity;

/**
 * 定义簇中心
 * <p>
 * Created by yijie.pang on 2016/6/7.
 */
public class ClusterCenter {

    //簇的编号
    public int K;

    public ParticleModel particleModel;

    public int getK() {
        return K;
    }

    public void setK(int k) {
        K = k;
    }

    public ParticleModel getParticleModel() {
        return particleModel;
    }

    public void setParticleModel(ParticleModel particleModel) {
        this.particleModel = particleModel;
    }
}
