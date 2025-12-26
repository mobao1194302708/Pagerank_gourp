import pandas as pd

# 1. 读取原始采集到的 CSV 文件
# 假设文件名是 mr_shuffle_times_results_fixed.csv
df = pd.read_csv('mr_shuffle_times_results_fixed.csv')

# 2. 定义处理函数：排序后剔除第一行，计算剩余部分的均值
def process_group(group):
    # 确保按 Job ID 的数字顺序排列
    sorted_group = group.sort_values('Job_ID')
    # 使用 .iloc[1:] 剔除第一行（即第一轮迭代）
    remaining_iters = sorted_group.iloc[1:]
    # 返回剩余迭代的平均值
    return remaining_iters['Avg_Shuffle_Time_ms'].mean()

# 3. 按任务规模分组并应用处理函数
final_results = df.groupby('Task_Size').apply(process_group).reset_index()
final_results.columns = ['Task_Size', 'Avg_Shuffle_Time_ms_Final']

# 4. 为了美观，按规模大小排序（可选）
size_map = {'25mb': 25, '50mb': 50, '100mb': 100, '300mb': 300}
final_results['sort_key'] = final_results['Task_Size'].map(size_map)
final_results = final_results.sort_values('sort_key').drop('sort_key', axis=1)

# 5. 保存到新的 CSV
final_results.to_csv('mr_iteration_average_shuffle.csv', index=False)

print("处理完成！每种规模下剔除第一轮后的平均 Shuffle 时间如下：")
print(final_results)