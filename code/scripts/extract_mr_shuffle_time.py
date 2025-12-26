import requests
import csv
import time

# 配置参数
JHS_HOST = "master" 
JHS_PORT = "19888"
OUTPUT_FILE = "mr_shuffle_times_results_fixed.csv"

# 任务 ID 范围保持不变
task_ranges = {
    "25mb":  ("application_1766627808924_0021", "application_1766627808924_0030"),
    "50mb":  ("application_1766627808924_0057", "application_1766627808924_0066"),
    "100mb": ("application_1766627808924_0001", "application_1766627808924_0010"),
    "300mb": ("application_1766545191683_0009", "application_1766545191683_0018")
}

def get_job_ids(start_id, end_id):
    prefix = "_".join(start_id.split("_")[:2]).replace("application", "job")
    start_num = int(start_id.split("_")[-1])
    end_num = int(end_id.split("_")[-1])
    return [f"{prefix}_{str(i).zfill(4)}" for i in range(start_num, end_num + 1)]

def fetch_avg_shuffle_time(job_id):
    """深层提取每个 Job 的 Shuffle 平均耗时"""
    tasks_url = f"http://{JHS_HOST}:{JHS_PORT}/ws/v1/history/mapreduce/jobs/{job_id}/tasks"
    try:
        resp = requests.get(tasks_url, timeout=10)
        resp.raise_for_status()
        tasks = resp.json().get('tasks', {}).get('task', [])
        
        reduce_tasks = [t for t in tasks if t['type'] == 'REDUCE' and t['state'] == 'SUCCEEDED']
        if not reduce_tasks: return 0.0

        total_shuffle_time = 0
        count = 0

        for rt in reduce_tasks:
            # 针对每个 Reduce 任务，访问其 Attempts 接口
            attempts_url = f"{tasks_url}/{rt['id']}/attempts"
            att_resp = requests.get(attempts_url, timeout=10)
            attempts = att_resp.json().get('taskAttempts', {}).get('taskAttempt', [])
            
            for att in attempts:
                if att['state'] == 'SUCCEEDED':
                    # 在 Attempt 层级获取关键指标
                    # 有些版本可能叫 elapsedShuffleTime，有些需要通过时间戳计算
                    s_time = att.get('elapsedShuffleTime')
                    if s_time is None:
                        # 兜底方案：使用 shuffleFinishTime - startTime
                        s_time = att.get('shuffleFinishTime', 0) - att.get('startTime', 0)
                    
                    if s_time > 0:
                        total_shuffle_time += s_time
                        count += 1
                        break # 只取成功的那个尝试
        
        return total_shuffle_time / count if count > 0 else 0.0
    except Exception as e:
        print(f"处理 {job_id} 时出错: {e}")
        return None

# --- 执行逻辑 ---
all_results = []
for size, (start_app, end_app) in task_ranges.items():
    job_ids = get_job_ids(start_app, end_app)
    for j_id in job_ids:
        avg_shuffle = fetch_avg_shuffle_time(j_id)
        if avg_shuffle is not None:
            print(f"[{size}] Job: {j_id} -> Avg Shuffle: {avg_shuffle:.2f} ms")
            all_results.append({
                'Task_Size': size,
                'Job_ID': j_id,
                'Avg_Shuffle_Time_ms': round(avg_shuffle, 2)
            })

# 保存 CSV
with open(OUTPUT_FILE, mode='w', newline='') as f:
    writer = csv.DictWriter(f, fieldnames=['Task_Size', 'Job_ID', 'Avg_Shuffle_Time_ms'])
    writer.writeheader()
    writer.writerows(all_results)
print(f"数据已成功保存至 {OUTPUT_FILE}")