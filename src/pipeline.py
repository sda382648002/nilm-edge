# pipeline.py
# 学术数据流水线总控：v2_load -> v3_resample -> 内存传递
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

sys.path.append(str(Path(__file__).parent / "src"))
from preprocess_v2_load import load_single_house_safe
from preprocess_v3_resample import resample_to_1hz

def run_pipeline(data_dir: Path, target_app: str = "kettle", safe_limit_min: int = 30):
    print("="*50)
    print("🚀 NILM Edge Pipeline | Run:", datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("="*50)
    
    print(f"⏳ Step 1/2: 加载与清洗 ({data_dir.name})...")
    df_clean = load_single_house_safe(data_dir, target_app, limit_minutes=safe_limit_min)
    print(f"   ✅ 清洗完成 | 样本: {len(df_clean)} | 频率: {df_clean.index.inferred_freq or '待重采样'}")
    
    print("⏳ Step 2/2: 重采样至 1Hz...")
    df_1hz = resample_to_1hz(df_clean)
    print(f"   ✅ 重采样完成 | 1Hz样本: {len(df_1hz)} | 跨度: {df_1hz.index.min()} → {df_1hz.index.max()}")
    
    # 集中日志
    log_path = Path("logs/experiment_log.txt")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Pipeline | {data_dir.name} | {target_app} | {len(df_clean)}->{len(df_1hz)} ✅\n")
        
    print("="*50)
    print("🎉 流水线执行完毕。数据驻留内存，可直连 v4 窗口化模块。")
    return df_1hz

if __name__ == "__main__":
    # 自动路由：优先真实数据，降级到 Mock
    REAL_DIR = Path("F:/UK-DALE-disaggregated/house_1")
    MOCK_DIR = Path("data/mock_house1")
    data_dir = REAL_DIR if REAL_DIR.exists() else MOCK_DIR
    
    # ⚠️ 首次运行加安全限制，确认流程后改为 None
    run_pipeline(data_dir, safe_limit_min=30)