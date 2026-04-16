# src/preprocess_v3_resample.py
# Day 4: 1Hz 重采样（NILM 标准频率）
# 规范: 降采样取均值；验证时间频率；防数据漂移

import pandas as pd
from pathlib import Path
from datetime import datetime

def resample_to_1hz(df: pd.DataFrame) -> pd.DataFrame:
    """
    将 DataFrame 重采样至 1Hz
    输入：任意频率的时间序列 DataFrame
    输出：1Hz 频率的 DataFrame
    """
    # 1. 确保索引是 DatetimeIndex 且已排序
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("❌ 索引必须是 DatetimeIndex")
    df = df.sort_index()

    # 2. 重采样（1S = 1 Second）
    # 注意：降采样（如 1min -> 1s）通常插值，升采样（如 1s -> 1min）取均值
    # 但为了通用性，这里使用 .asfreq('1S').interpolate() 处理可能的时间间隙
    # 对于 NILM，如果是 1min 数据，通常需要 interpolate 填补中间秒
    
    current_freq = df.index.inferred_freq
    print(f"📊 原始频率估计: {current_freq}")

    # 策略：先重采样对齐，再插值填补
    df_1hz = df.resample('1S').mean()
    
    # 线性插值填补重采样产生的 NaN（仅在连续缺失较短时允许）
    df_1hz = df_1hz.interpolate(method='linear', limit=60) # 限制插值长度
    
    # 填充首尾可能的 NaN
    df_1hz = df_1hz.ffill().bfill()

    return df_1hz

def run_day4_validation():
    """Day 4 验证入口"""
    # 使用模拟数据（1 分钟频率）
    print("="*45)
    print("🔄 Day 4: 1Hz 重采样验证")
    
    # 模拟 1min 数据
    dates = pd.date_range("2023-01-01", periods=10, freq="1min")
    data = pd.DataFrame({
        "aggregate": [100, 110, 120, 130, 140, 150, 160, 170, 180, 190],
        "kettle": [0, 10, 20, 0, 0, 15, 15, 5, 0, 0]
    }, index=dates)
    
    print("📥 原始数据 (1min):")
    print(data.head(3))
    
    # 执行重采样
    df_1hz = resample_to_1hz(data)
    
    print("\n📤 重采样后数据 (1Hz):")
    print(df_1hz.head(5))
    print(f"   总样本量: {len(df_1hz)} (预期：~600 行)")
    print(f"   时间跨度: {df_1hz.index.min()} → {df_1hz.index.max()}")
    
    # 验证频率
    # 注意：resample 后 index.freq 可能不自动显示，需手动断言间隔
    time_diff = df_1hz.index[1] - df_1hz.index[0]
    assert time_diff.total_seconds() == 1.0, f"❌ 频率错误：间隔为 {time_diff.total_seconds()} 秒"
    
    print("\n✅ 1Hz 重采样验证通过！")
    print("="*45)

    # 记录日志
    log_path = Path("logs/experiment_log.txt")
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] v3_resample | 1Hz resampling validated | Samples={len(df_1hz)} ✅\n")

if __name__ == "__main__":
    run_day4_validation()