package org.example.giraph;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

public class PageRankComputation extends BasicComputation<
        Text, FloatWritable, NullWritable, FloatWritable> {

    // 对应 MR: DAMPING_FACTOR
    public static final float DAMPING_FACTOR = 0.85f;
    // 聚合器名称必须与 Master 中注册的一模一样
    public static final String DANGLING_AGG = "dangling_agg";
    public static final String MAX_SUPERSTEPS_FLAG = "pagerank.max.supersteps";

    @Override
    public void compute(Vertex<Text, FloatWritable, NullWritable> vertex,
                        Iterable<FloatWritable> messages) throws IOException {

        // 1. 获取节点总数
        int confTotalNodes = getConf().getInt("total.nodes", -1);
        long totalNodes = (confTotalNodes != -1) ? confTotalNodes : getTotalNumVertices();

        // [修改 1] Superstep 0: 初始化节点值并分发
        if (getSuperstep() == 0) {
            // 标准 PageRank 初始化：将节点值设为 1/N
           // float initialVal = 1.0f / totalNodes;
            // vertex.setValue(new FloatWritable(initialVal));
            distributeVotes(vertex);
            return;
        }

        // 2. 汇总投票 (对应 MR Reducer: sumVotes)
        float sumVotes = 0.0f;
        for (FloatWritable message : messages) {
            sumVotes += message.get();
        }

        // [修改 2] 修复 NullPointerException
        // 逻辑调整：只有在 Superstep > 1 时才去获取聚合值。
        // 因为 Superstep 1 时你需要强制设为 0，所以没必要去 get，get 了反而可能崩。
        float totalDanglingMass = 0.0f;
        if (getSuperstep() >= 1) {
            FloatWritable danglingVal = getAggregatedValue(DANGLING_AGG);
            // 增加非空判断，防止崩盘
            if (danglingVal != null) {
                totalDanglingMass = danglingVal.get();
            }
        }

        float danglingAvg = totalDanglingMass / totalNodes;

        // 4. PageRank 公式
        float randomJump = (1.0f - DAMPING_FACTOR) / totalNodes;
        float newPr = randomJump + DAMPING_FACTOR * (sumVotes + danglingAvg);

        // 更新节点值
        vertex.setValue(new FloatWritable(newPr));

        // 获取最大迭代次数
        int maxSupersteps = getConf().getInt(MAX_SUPERSTEPS_FLAG, 30);

        if (getSuperstep() < maxSupersteps) {
            distributeVotes(vertex);
        } else {
            vertex.voteToHalt();
        }
    }

    private void distributeVotes(Vertex<Text, FloatWritable, NullWritable> vertex) {
        if (vertex.getNumEdges() == 0) {
            // 悬挂节点：将当前 PR 贡献给全局聚合器
            aggregate(DANGLING_AGG, vertex.getValue());
        } else {
            // 普通节点：平均分发
            float vote = vertex.getValue().get() / vertex.getNumEdges();
            sendMessageToAllEdges(vertex, new FloatWritable(vote));
        }
    }
}