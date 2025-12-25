import random

# ================= 配置区域 =================
# 1. 设置节点总数 (N)
NUM_NODES = int(8_000_000)

# 2. 设置每个节点最大可能的出度 (最大有多少个邻居)
# 实际出度会在 1 到 MAX_OUT_DEGREE 之间随机
MAX_OUT_DEGREE = 12

# 3. 输出文件名
OUTPUT_FILE = "random_pagerank_data_25.txt"


# ===========================================

def generate_dataset():
    print(f"开始生成随机数据集...")
    print(f" - 节点总数: {NUM_NODES}")
    print(f" - 最大出度: {MAX_OUT_DEGREE}")

    # 计算初始 PageRank 值 (1 / N)
    initial_pr = 1.0 / NUM_NODES
    print(f" - 初始 PR 值: {initial_pr:.10f}")

    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        for node_id in range(NUM_NODES):
            # 1. 随机决定这个节点有多少个邻居 (0 到 MAX 之间)
            # 如果是 0，这就是一个悬挂节点 (Dangling Node)
            num_neighbors = random.randint(1, MAX_OUT_DEGREE)

            # 2. 从所有节点中随机抽取 num_neighbors 个作为邻居
            # range(NUM_NODES) 包含了所有可能的节点 ID
            targets = random.sample(range(NUM_NODES), num_neighbors)

            # 3. (可选) 移除自环：如果邻居里包含了自己，将其移除
            if node_id in targets:
                targets.remove(node_id)

            # 4. 格式化输出
            # 将邻居列表转换为 "1,2,3" 的字符串格式
            neighbors_str = ",".join(map(str, targets))

            # 写入文件
            # 格式: NodeID \t InitialPR \t Target1,Target2...
            # 这里使用了制表符 \t 分隔，如果你的解析代码用空格，请把 \t 换成空格
            line = f"{node_id}\t{initial_pr:.10f}\t{neighbors_str}\n"
            f.write(line)

            # 打印进度 (每生成 10% 打印一次)
            if NUM_NODES >= 10 and (node_id + 1) % (NUM_NODES // 10) == 0:
                print(f"已生成: {node_id + 1}/{NUM_NODES} 行")

    print(f"生成完成！文件已保存为: {OUTPUT_FILE}")
    print("你可以直接将此文件上传到 HDFS 进行 MapReduce 测试。")


if __name__ == "__main__":
    generate_dataset()