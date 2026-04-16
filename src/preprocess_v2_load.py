# src/preprocess_v2_load.py
# Day 3: 单House原始数据加载与安全清洗
# 学术红线: 禁用bfill防未来泄露; 仅单建筑处理

import pandas as pd
from pathlib import Path
from datetime import datetime

def load_single_house_safe(house_dir: Path, target_appliance: str = "kettle") -> pd.DataFrame:
    agg_path = house_dir / "aggregate.csv"
    app_path = house_dir / f"appliance_{target_appliance}.csv"

    if not agg_path.exists() or not app_path.exists():
        raise FileNotFoundError(f"⚠️ 数据缺失: 请确认 {agg_path} 与 {app_path} 存在")

    
    df_agg = pd.read_csv(agg_path, names=["timestamp", "aggregate"], parse_dates=["timestamp"], index_col="timestamp", header=0, dtype={"aggregate": "float64"}, date_format="mixed")
    df_app = pd.read_csv(app_path, names=["timestamp", target_appliance], parse_dates=["timestamp"], index_col="timestamp", header=0, dtype={target_appliance: "float64"}, date_format="mixed")

    df = pd.concat([df_agg, df_app], axis=1)
    df = df.interpolate(method='linear', limit=10).ffill(limit=10)  # 学术红线：禁用bfill

    assert not df.isnull().any().any(), "⚠️ 存在未填充NaN，检查数据源缺失比例"
    assert df.index.is_monotonic_increasing, "⚠️ 时间索引未排序，请检查原始数据"

    return df

def run_day3_validation():
    """Day 3 验证入口"""
    # 使用模拟数据跑通流程（后续替换为真实UK-DALE路径）
    TEST_DIR = Path("data/mock_house1")
    TARGET = "kettle"

    if not TEST_DIR.exists():
        TEST_DIR.mkdir(parents=True, exist_ok=True)
        dates = pd.date_range("2023-01-01", periods=100, freq="1min")
        pd.DataFrame({"timestamp": dates, "aggregate": range(100)}).to_csv(TEST_DIR/"aggregate.csv", index=False)
        pd.DataFrame({"timestamp": dates, "kettle": [0]*50 + [10]*50}).to_csv(TEST_DIR/f"appliance_{TARGET}.csv", index=False)
        print("📦 已生成模拟数据用于流程验证")

    print("="*45)
    print("📥 Day 3: 数据加载与安全清洗")
    df = load_single_house_safe(TEST_DIR, TARGET)
    print(f"✅ 加载成功 | 样本量: {len(df)} | 时间跨度: {df.index.min()} → {df.index.max()}")
    print(f"   聚合功率范围: [{df['aggregate'].min():.1f}, {df['aggregate'].max():.1f}] W")
    print(f"   {TARGET} 功率范围: [{df[TARGET].min():.1f}, {df[TARGET].max():.1f}] W")
    print("="*45)

    # 自动追加日志
    log_path = Path("logs/experiment_log.txt")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] v2_load | Single-house loaded | Samples={len(df)} | No bfill ✅\n")

if __name__ == "__main__":
    run_day3_validation()