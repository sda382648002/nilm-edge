# src/preprocess_v2_load.py
# Day 3/6: 统一安全加载器（自动路由 CSV / UK-DALE .dat）
# 学术红线: 禁用 bfill | 强制 float64 | 时间单调 | NaN 拦截

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime

def load_single_house_safe(data_dir: Path, target_app: str = "kettle", limit_minutes: int = None) -> pd.DataFrame:
    """
    统一入口：自动检测数据格式并安全加载
    limit_minutes: 首次验证防 OOM 的安全截断参数，跑通后设为 None 全量加载
    """
    fmt = _detect_format(data_dir)
    
    if fmt == "ukdale_dat":
        df = _load_ukdale_binary(data_dir, target_app, limit_minutes)
    else:
        df = _load_csv_safe(data_dir, target_app, limit_minutes)
        
    # 🛡️ 统一清洗管道（学术红线）
    df = df.interpolate(method='linear', limit=10).ffill(limit=10)
    assert not df.isnull().any().any(), "⚠️ 存在未填充 NaN，检查缺失比例"
    assert df.index.is_monotonic_increasing, "⚠️ 时间索引未排序"
    
    if limit_minutes:
        print(f"📉 验证模式: 仅保留前 {limit_minutes} 分钟 ({len(df)} 样本)")
    return df

# ================= 内部路由函数 =================

def _detect_format(data_dir: Path) -> str:
    if (data_dir / "mains.dat").exists(): return "ukdale_dat"
    if (data_dir / "aggregate.csv").exists(): return "csv"
    raise FileNotFoundError("❌ 未找到支持的原始数据 (.dat 或 .csv)")

def _load_ukdale_binary(data_dir: Path, target_app: str, limit_min: int) -> pd.DataFrame:
    """解析 UK-DALE 官方二进制 (uint64 ts + int32 pwr)"""
    labels_path = data_dir / "labels.dat"
    ch_id = None
    with open(labels_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            if target_app.lower() in line.lower():
                ch_id = int(line.split()[0].replace('channel_', ''))
                break
    if ch_id is None:
        raise ValueError(f"❌ labels.dat 中未找到 '{target_app}'")
        
    dtype = np.dtype([('ts', '<u8'), ('pwr', '<i4')])
    count = (limit_min * 60) if limit_min else None  # 1Hz 采样率换算
    
    def read_ch(fname):
        data = np.fromfile(data_dir / fname, dtype=dtype, count=count)
        return pd.Series(data['pwr'], index=pd.to_datetime(data['ts'], unit='s'))
        
    df = pd.DataFrame({
        "aggregate": read_ch("mains.dat"),
        target_app: read_ch(f"channel_{ch_id}.dat")
    }).dropna(how='all').sort_index()
    return df

def _load_csv_safe(data_dir: Path, target_app: str, limit_min: int) -> pd.DataFrame:
    """兼容 Mock 数据与标准 CSV 格式"""
    def read_csv_col(fname, col_name):
        s = pd.read_csv(
            data_dir / fname, names=["ts", col_name], parse_dates=["ts"], 
            index_col="ts", header=0, dtype={col_name: "float64"}
        )[col_name]
        return s.iloc[:limit_min*60] if limit_min else s
        
    return pd.DataFrame({
        "aggregate": read_csv_col("aggregate.csv", "aggregate"),
        target_app: read_csv_col(f"appliance_{target_app}.csv", target_app)
    }).sort_index()

# ================= 验证入口 =================
def run_validation():
    REAL_DIR = Path("F:/UK-DALE-disaggregated/house_1")
    MOCK_DIR = Path("data/mock_house1")
    data_dir = REAL_DIR if REAL_DIR.exists() else MOCK_DIR
    
    print("="*45)
    print("📥 Day 3/6: 统一加载器验证")
    df = load_single_house_safe(data_dir, limit_minutes=10)  # 仅测 10 分钟
    print(f"✅ 加载成功 | 样本: {len(df)} | 范围: {df['aggregate'].min():.1f}~{df['aggregate'].max():.1f} W")
    print("="*45)
    
    log_path = Path("logs/experiment_log.txt")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] v2_load_unified | Format: {_detect_format(data_dir)} | Samples={len(df)} ✅\n")

if __name__ == "__main__":
    run_validation()