#!/bin/bash
echo "🤖 增强版监控脚本 - 实时追踪 Giraph/MapReduce 任务"
echo "监控将覆盖 ACCEPTED 和 RUNNING 状态"
echo "=================================================="

# 1. 自动获取最新的 Application ID 或使用传入参数
# Giraph 任务在 YARN 中的名称通常包含 "Giraph" 或 "PageRank"
APP_NAME_PATTERN="Giraph\|PageRank"

# 监控文件
MONITOR_FILE="/tmp/giraph_slave2_monitor_$(date +%Y%m%d_%H%M%S).csv"
echo "时间戳,CPU%,内存MB,YARN状态" > "$MONITOR_FILE"

echo "📁 监控数据保存至: $MONITOR_FILE"

page_running=0
while true; do
    # 使用通配符匹配所有相关的 YARN 任务状态
    # 增加 ACCEPTED 状态捕获，防止漏掉启动阶段
    app_info=$(yarn application -list -appStates RUNNING,ACCEPTED 2>/dev/null | grep -i "$APP_NAME_PATTERN" | head -n 1)
    
    if [ -n "$app_info" ]; then
        app_id=$(echo "$app_info" | awk '{print $1}')
        app_state=$(echo "$app_info" | awk '{print $6}')
        
        if [ $page_running -eq 0 ]; then
            echo "🚀 检测到任务启动! ID: $app_id | 状态: $app_state"
            page_running=1
            start_time=$(date +%s)
        fi

        # 记录数据
        timestamp=$(date '+%H:%M:%S')
        # 注意：这里监控的是当前运行脚本的机器负载
        cpu_usage=$(top -bn1 | grep "Cpu(s)" | awk '{printf "%.1f", $2 + $4}')
        mem_used=$(free -m | awk '/Mem:/ {print $3}')
        
        echo "$timestamp,$cpu_usage,$mem_used,$app_state" >> "$MONITOR_FILE"
        printf "🕒 %s | %-10s | CPU: %5.1f%% | 内存: %6dMB\n" "$timestamp" "$app_state" "$cpu_usage" "$mem_used"
    else
        if [ $page_running -eq 1 ]; then
            echo "✅ 任务已从 YARN 运行列表消失（已结束或失败）。"
            end_time=$(date +%s)
            echo "⏱️  执行时长: $((end_time - start_time)) 秒"
            break
        fi
        echo -ne "⏳ 等待任务提交... \r"
    fi
    sleep 2
done