package org.example.giraph;

import org.apache.giraph.edge.Edge;
import org.apache.giraph.edge.EdgeFactory;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.io.formats.TextVertexInputFormat;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class PageRankInputFormat extends TextVertexInputFormat<Text, FloatWritable, NullWritable> {

    @Override
    public TextVertexReader createVertexReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new PageRankReader();
    }

    public class PageRankReader extends TextVertexReader {

        @Override
        public boolean nextVertex() throws IOException, InterruptedException {
            return getRecordReader().nextKeyValue();
        }

        @Override
        public Vertex<Text, FloatWritable, NullWritable> getCurrentVertex() throws IOException, InterruptedException {
            Text line = getRecordReader().getCurrentValue();
            String lineStr = line.toString();

            // -------------------------------------------------------------
            // [优化重点] 使用 indexOf 替代 split，实现“零垃圾”或“低垃圾”解析
            // -------------------------------------------------------------

            // 1. 解析 ID (找到第一个制表符)
            int firstTab = lineStr.indexOf('\t');
            String idStr;
            if (firstTab == -1) {
                idStr = lineStr; // 只有ID，没有其他数据
            } else {
                idStr = lineStr.substring(0, firstTab);
            }
            Text id = new Text(idStr);

            // 2. 解析 PR 值 (找到第二个制表符)
            float pr = 0.0f;
            int secondTab = -1;

            if (firstTab != -1) {
                secondTab = lineStr.indexOf('\t', firstTab + 1);
                String prStr;
                if (secondTab == -1) {
                    // 格式: ID \t PR
                    prStr = lineStr.substring(firstTab + 1);
                } else {
                    // 格式: ID \t PR \t Edges
                    prStr = lineStr.substring(firstTab + 1, secondTab);
                }

                if (!prStr.isEmpty()) {
                    try {
                        pr = Float.parseFloat(prStr);
                    } catch (NumberFormatException e) {
                        pr = 0.0f;
                    }
                }
            }

            // 3. 解析 Edges (遍历剩余字符串查找逗号)
            List<Edge<Text, NullWritable>> edges = new ArrayList<>();
            if (secondTab != -1) {
                int currentPos = secondTab + 1;
                int len = lineStr.length();

                // 循环查找逗号，避免 split(",") 创建大数组
                while (currentPos < len) {
                    int nextComma = lineStr.indexOf(',', currentPos);
                    if (nextComma == -1) {
                        nextComma = len;
                    }

                    // 提取子串并添加边 (跳过空字符串)
                    if (nextComma > currentPos) {
                        String neighborStr = lineStr.substring(currentPos, nextComma).trim();
                        if (!neighborStr.isEmpty()) {
                            edges.add(EdgeFactory.create(new Text(neighborStr), NullWritable.get()));
                        }
                    }
                    currentPos = nextComma + 1;
                }
            }

            // 4. 创建顶点
            Vertex<Text, FloatWritable, NullWritable> vertex = getConf().createVertex();
            vertex.initialize(id, new FloatWritable(pr), edges);
            return vertex;
        }
    }
}