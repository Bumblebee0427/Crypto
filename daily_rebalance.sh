#!/bin/bash
# 文件名: daily_rebalance.sh

# 设置Python解释器路径
PYTHON_PATH="$HOME/.pyenv/versions/3.10.10/bin/python"
JUPYTER_PATH="$HOME/.pyenv/versions/3.10.10/bin/jupyter"

# 设置日志目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
mkdir -p "$LOG_DIR"                                      
LOG_FILE="$LOG_DIR/daily_rebalance_$(date +%Y%m%d).log"

echo "===== $(date) 开始每日再平衡任务 =====" >> "$LOG_FILE"
echo "脚本执行时间: $(date) (UTC: $(TZ=UTC date))" >> "$LOG_FILE"
echo "使用Python解释器: $PYTHON_PATH" >> "$LOG_FILE"

# 设置工作目录为脚本所在目录
cd "$SCRIPT_DIR"
echo "工作目录: $(pwd)" >> "$LOG_FILE"

# 确保pyenv环境正确初始化
export PYENV_ROOT="$HOME/.pyenv"
export PATH="$PYENV_ROOT/bin:$PATH"
eval "$(pyenv init -)"

# 导出必要的环境变量
export PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH

# 1. 执行data_loader.py
echo "1. 正在执行data_loader.py..." >> "$LOG_FILE"
"$PYTHON_PATH" data_loader.py >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "data_loader.py 执行失败!" >> "$LOG_FILE"
    exit 1
fi

# 2. 执行code1_get_alpha1.ipynb
echo "2. 正在执行code1_get_alpha1.ipynb..." >> "$LOG_FILE"
"$JUPYTER_PATH" nbconvert --to script code1_get_alpha1.ipynb
"$PYTHON_PATH" code1_get_alpha1.py >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "code1_get_alpha1.ipynb 执行失败!" >> "$LOG_FILE"
    exit 1
fi

# 3. 执行code2_backtest_add.ipynb
echo "3. 正在执行code2_backtest_add.ipynb..." >> "$LOG_FILE"
"$JUPYTER_PATH" nbconvert --to script code2_backtest_add.ipynb
"$PYTHON_PATH" code2_backtest_add.py >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "code2_backtest_add.ipynb 执行失败!" >> "$LOG_FILE"
    exit 1
fi

# 4. 执行executor.py
echo "4. 正在执行executor.py..." >> "$LOG_FILE"
"$PYTHON_PATH" executor.py >> "$LOG_FILE" 2>&1
if [ $? -ne 0 ]; then
    echo "executor.py 执行失败!" >> "$LOG_FILE"
    exit 1
fi

echo "===== $(date) 每日再平衡任务完成 =====" >> "$LOG_FILE"