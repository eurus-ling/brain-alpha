#!/bin/bash

# 设置完整的环境变量
export PATH=/usr/local/bin:/usr/bin:/bin:/usr/local/sbin:/usr/sbin:/sbin
export HOME=/root
export TERM=xterm
export SHELL=/bin/bash

# 设置变量
SESSION_NAME="alpha-sim"
SCRIPT_DIR="/root/brain-alpha"
cd "$SCRIPT_DIR"

# 使用完整路径
PROGRAM_CMD="source $SCRIPT_DIR/alpha-env/bin/activate; python3 $SCRIPT_DIR/alpha_simulator.py"

echo "$(date): Starting restart process..."
echo "Current directory: $(pwd)"  # 调试信息
echo "PATH: $PATH"  # 调试信息

# 检查并结束所有相关的 screen 会话
echo "Stopping all related screen sessions..."
screen -list | grep -E "(\.${SESSION_NAME}$|\.${SESSION_NAME}\.)" | awk '{print $1}' | while read session; do
    echo "Stopping session: $session"
    screen -S "$session" -X quit
    sleep 2
done

# 确保所有相关进程都结束
pkill -f "$SESSION_NAME" 2>/dev/null
sleep 3

# 正确启动新的会话
echo "Starting new screen session: $SESSION_NAME"
screen -dmS "$SESSION_NAME" bash -c "cd $SCRIPT_DIR && $PROGRAM_CMD; exec bash"

# 添加验证步骤
sleep 5
echo "Checking if screen session is running:"
screen -list

echo "Checking if python process is running:"
ps aux | grep alpha_simulator.py | grep -v grep

echo "$(date): Restart completed"
echo "----------------------------------------"