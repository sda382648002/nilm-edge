# src/preprocess_v1_hello.py
# Day 2: 环境与工作流验证脚本
# 规范: 仅验证环境+自动记录日志，不处理数据

import sys
import numpy as np
import pandas as pd
import torch
from pathlib import Path
from datetime import datetime

def main():
    print("=" * 45)
    print("✅ NILM Edge Pruning - Day 2 Verification")
    print(f"Python : {sys.version.split()[0]}")
    print(f"NumPy  : {np.__version__}")
    print(f"Pandas : {pd.__version__}")
    print(f"PyTorch: {torch.__version__}")
    print("=" * 45)

    # 自动追加日志（学术规范：每次运行必留痕）
    log_dir = Path("logs")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "experiment_log.txt"
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(f"[{timestamp}] v1_hello | Env verified | PyTorch={torch.__version__} ✅\n")
    
    print(f"📝 日志已追加至: {log_file}")
    print("💡 下一步: 按规范执行每日备份，提交验证截图")

if __name__ == "__main__":
    main()