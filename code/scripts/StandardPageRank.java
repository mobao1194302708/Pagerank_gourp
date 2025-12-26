package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class StandardPageRank {

    // 自定义计数器，用于统计悬挂节点的总 PR 值
    public enum MyCounters {
        DANGLING_MASS_X_SCALE // 存 long 类型，为了保留小数精度，我们会 * 10^9
    }

    // 常量定义
    private static final float DAMPING_FACTOR = 0.85f;
    private static final long PRECISION_SCALE = 1_000_000_000L; // 精度放大倍数

    /**
     * Mapper
     * 输入格式: Node \t CurrentPR \t AdjList
     * 逻辑:
     * 1. 输出图结构 (STRUCT)
     * 2. 如果有邻居，计算票数并分发 (VOTE)
     * 3. 如果无邻居 (悬挂节点)，将自身 PR 累加到全局计数器中
     */
    public static class PRMapper extends Mapper<Text, Text, Text, Text> {

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t", 2);

            float currentPr = 0.0f;
            String adjList = "";

            if (parts.length > 0) {
                try {
                    currentPr = Float.parseFloat(parts[0]);
                } catch (NumberFormatException e) {
                    currentPr = 0.0f;
                }
            }
            if (parts.length > 1) {
                adjList = parts[1];
            }

            // 1. 传递图结构，供 Reducer 重组
            // 输出: Node -> "STRUCT\tNeighbor1,Neighbor2..."
            context.write(key, new Text("STRUCT\t" + adjList));

            String[] neighbors = adjList.isEmpty() ? new String[0] : adjList.split(",");

            // 2. 处理分票逻辑
            if (neighbors.length == 0 || (neighbors.length == 1 && neighbors[0].trim().isEmpty())) {
                // === 悬挂节点 (Dangling Node) ===
                // 将其 PR 值记录到全局计数器中 (放大转为 Long)
                long scaledPr = (long) (currentPr * PRECISION_SCALE);
                context.getCounter(MyCounters.DANGLING_MASS_X_SCALE).increment(scaledPr);
            } else {
                // === 普通节点 ===
                float vote = currentPr / neighbors.length;
                for (String neighbor : neighbors) {
                    if (!neighbor.trim().isEmpty()) {
                        // 输出: Neighbor -> "VOTE\t0.125"
                        context.write(new Text(neighbor), new Text("VOTE\t" + vote));
                    }
                }
            }
        }
    }

    /**
     * Reducer
     * 逻辑:
     * 1. 汇总所有 VOTE
     * 2. 接收并保留 STRUCT
     * 3. 应用 PageRank 公式:
     *    NewPR = (1-d)/N + d * ( Sum(Votes) + DanglingAvg )
     */
    public static class PRReducer extends Reducer<Text, Text, Text, Text> {

        private float danglingAvg = 0.0f;
        private int totalNodes = 1;

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            totalNodes = conf.getInt("total.nodes", 1);

            // 获取上一轮计算出的悬挂节点总质量
            float totalDanglingMass = conf.getFloat("prev.dangling.mass", 0.0f);
            // 将悬挂节点的能量平均分给所有节点
            danglingAvg = totalDanglingMass / totalNodes;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            float sumVotes = 0.0f;
            String adjList = "";
            boolean structFound = false;

            for (Text val : values) {
                String strVal = val.toString();
                if (strVal.startsWith("STRUCT")) {
                    // 恢复结构
                    if (strVal.length() > 7) {
                        adjList = strVal.substring(7);
                    }
                    structFound = true;
                } else if (strVal.startsWith("VOTE")) {
                    // 累加投票
                    try {
                        sumVotes += Float.parseFloat(strVal.substring(5));
                    } catch (NumberFormatException ignored) {}
                }
            }

            // 如果只收到了投票但没有结构信息（可能是孤立被指向的点），也需要处理
            if (!structFound && adjList.isEmpty()) {
                // 视具体需求，可以忽略或补全。标准算法通常假设图结构是静态完整的。
            }

            // PageRank 核心公式
            // 随机跳转概率 + 阻尼系数 * (收到的票数 + 悬挂节点补偿)
            float randomJump = (1.0f - DAMPING_FACTOR) / totalNodes;
            float newPr = randomJump + DAMPING_FACTOR * (sumVotes + danglingAvg);

            // 输出: Node \t NewPR \t AdjList
            context.write(key, new Text(newPr + "\t" + adjList));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: StandardPageRank <input> <outputBase> <iterations> <totalNodes>");
            System.exit(1);
        }

        String inputPath = args[0];
        String outputBase = args[1];
        int iterations = Integer.parseInt(args[2]);
        int totalNodes = Integer.parseInt(args[3]);

        // 初始悬挂质量为 0
        float prevDanglingMass = 0.0f;

        for (int i = 0; i < iterations; i++) {
            Configuration conf = new Configuration();
            conf.setInt("total.nodes", totalNodes);
            // 将上一轮计算出的悬挂总值传入本轮 Job
            conf.setFloat("prev.dangling.mass", prevDanglingMass);

            Job job = Job.getInstance(conf, "PageRank-Iter-" + i);
            job.setJarByClass(StandardPageRank.class);

            // 使用 KeyValueTextInputFormat 处理 "Key \t Value" 格式更方便
            // 默认分隔符是 Tab，Key是第一个字段(Node)，Value是剩余部分(PR \t AdjList)
            job.setInputFormatClass(KeyValueTextInputFormat.class);

            job.setMapperClass(PRMapper.class);
            job.setReducerClass(PRReducer.class);

            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            // 输入路径: 第一次是 args[0]，之后是上一次的输出
            String in = (i == 0) ? inputPath : outputBase + "/iter-" + (i - 1);
            String out = outputBase + "/iter-" + i;

            FileInputFormat.addInputPath(job, new Path(in));
            FileOutputFormat.setOutputPath(job, new Path(out));

            // 清理输出目录
            FileSystem fs = FileSystem.get(conf);
            if (fs.exists(new Path(out))) {
                fs.delete(new Path(out), true);
            }

            if (!job.waitForCompletion(true)) {
                System.err.println("Job failed at iteration " + i);
                System.exit(1);
            }

            // === 关键步骤 ===
            // 任务结束后，从 Counter 中读取本轮发现的悬挂节点总 PR
            Counter counter = job.getCounters().findCounter(MyCounters.DANGLING_MASS_X_SCALE);
            long massScaled = counter.getValue();

            // 转换回 float，供下一轮使用
            prevDanglingMass = (float) massScaled / PRECISION_SCALE;

            System.out.println("Iteration " + i + " finished. Dangling Mass found: " + prevDanglingMass);
        }
    }
}