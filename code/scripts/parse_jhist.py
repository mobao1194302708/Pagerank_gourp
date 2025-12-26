import subprocess
import re

# 配置
HDFS_PATH = "/mr-history/done/2025/12/23/000000/"
APP_ID_PREFIX = "job_1766453982193_"
# 500MB 任务范围 (排除 0002)
START_NUM = 3
END_NUM = 11

def get_job_shuffle_time(job_id):
    """利用正则从 hdfs cat 输出中强行抓取时间戳"""
    try:
        # 1. 定位文件
        ls_cmd = f"hdfs dfs -ls {HDFS_PATH} | grep {job_id} | grep .jhist"
        full_path = subprocess.check_output(ls_cmd, shell=True).decode('utf-8').split()[-1]
        
        # 2. 读取内容
        cat_cmd = f"hdfs dfs -cat {full_path}"
        process = subprocess.Popen(cat_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        stdout, _ = process.communicate()
        content = stdout.decode('utf-8', errors='ignore')
        
        # 3. 正则匹配
        # 我们寻找同一行内出现的 startTime 和 shuffleFinishTime
        # 格式示例: "startTime":1766464088887 ... "shuffleFinishTime":1766464500000
        starts = re.findall(r'"startTime":(\d+)', content)
        shuffles = re.findall(r'"shuffleFinishTime":(\d+)', content)
        
        durations = []
        # 由于一个 Job 可能有多个 Reduce Task，我们对齐提取到的时间戳
        for s, f in zip(starts, shuffles):
            diff = int(f) - int(s)
            if diff > 0:
                durations.append(diff)
        
        if not durations:
            return 0
        return sum(durations) / len(durations)
        
    except Exception as e:
        return None

# --- 执行主逻辑 ---
print("="*60)
print(f"{'Job ID':<25} | {'Avg Shuffle Time (ms)':<20}")
print("-"*60)

all_avgs = []
for i in range(START_NUM, END_NUM + 1):
    job_id = f"{APP_ID_PREFIX}{str(i).zfill(4)}"
    avg_ms = get_job_shuffle_time(job_id)
    
    if avg_ms is not None:
        print(f"{job_id:<25} | {avg_ms:<20.2f}")
        if avg_ms > 0: all_avgs.append(avg_ms)
    else:
        print(f"{job_id:<25} | 数据提取失败")

if all_avgs:
    final_res = sum(all_avgs) / len(all_avgs)
    print("="*60)
    print(f"500MB 任务稳定态平均 Shuffle 耗时: {final_res:.2f} ms")