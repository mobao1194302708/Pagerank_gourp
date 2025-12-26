package org.example.giraph;

import org.apache.giraph.aggregators.FloatSumAggregator;
import org.apache.giraph.master.DefaultMasterCompute;

public class PageRankMaster extends DefaultMasterCompute {
    @Override
    public void initialize() throws InstantiationException, IllegalAccessException {
        // [核心修复] 必须注册聚合器，否则 Worker 读取时会报空指针
        // 注意：名字 "dangling_agg" 必须与 PageRankComputation 中的完全一致
        // 这里直接引用 PageRankComputation.DANGLING_AGG 防止拼写错误
        registerAggregator(PageRankComputation.DANGLING_AGG, FloatSumAggregator.class);
    }
}