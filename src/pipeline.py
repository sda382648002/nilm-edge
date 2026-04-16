# pipeline.py
# 学术数据流水线总控：v2_load -> v3_resample -> 输出中间态
import sys
from pathlib import Path
from datetime import datetime
import pandas as pd

# 导入独立模块（确保 Python 能找到 src 目录）
sys.path.append(str(Path(__file__).parent / "src"))
from preprocess_v2_load import load_single_house_safe
from preprocess_v3_resample import resample_to_1hz

def run_pipeline(data_dir: Path, target_app: str = "kettle"):
    print("="*50)
    print("🚀 NILM Edge Pipeline | Run:", datetime.now().strftime("%Y-%m-%d %H:%M"))
    print("="*50)
    
    # Step 1: 加载与清洗
    print("⏳ Step 1/2: 加载单House数据 (v2_load)...")
    df_clean = load_single_house_safe(data_dir, target_app)
    print(f"   ✅ 清洗完成 | 原始样本: {len(df_clean)} | 频率: {df_clean.index.inferred_freq}")
    
    # Step 2: 重采样至 1Hz
    print("⏳ Step 2/2: 重采样至 1Hz (v3_resample)...")
    df_1hz = resample_to_1hz(df_clean)
    print(f"   ✅ 重采样完成 | 1Hz样本: {len(df_1hz)} | 时间跨度: {df_1hz.index.min()} → {df_1hz.index.max()}")
    
    # 统一日志记录
    log_path = Path("logs/experiment_log.txt")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Pipeline | v2->v3 chained | Samples: {len(df_clean)}->{len(df_1hz)} ✅\n")
    
    print("="*50)
    print("🎉 流水线执行完毕。数据已驻留内存，可直连 v4 窗口化模块。")
    return df_1hz

if __name__ == "__main__":
    # 默认跑 Mock 数据验证流水线连通性
    MOCK_DIR = Path("data/mock_house1")
    if not MOCK_DIR.exists():
        # 若 mock 不存在，临时生成一次（仅首次）
        MOCK_DIR.mkdir(parents=True, exist_ok=True)
        dates = pd.date_range("2023-01-01", periods=100, freq="1min")
        pd.DataFrame({"timestamp": dates, "aggregate": range(100)}).to_csv(MOCK_DIR/"aggregate.csv", index=False)
        pd.DataFrame({"timestamp": dates, "kettle": [0]*50+[10]*50}).to_csv(MOCK_DIR/"appliance_kettle.csv", index=False)
        print("📦 首次运行：已生成 mock 数据")
        
    run_pipeline(MOCK_DIR, target_app="kettle")