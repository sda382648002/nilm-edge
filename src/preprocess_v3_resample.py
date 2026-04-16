# src/preprocess_v3_resample.py
# Day 4: 1Hz 重采样（纯处理函数，不绑死数据源）
import pandas as pd
from datetime import datetime

def resample_to_1hz(df: pd.DataFrame) -> pd.DataFrame:
    """接收已清洗的 DataFrame，输出 1Hz 对齐数据"""
    if not isinstance(df.index, pd.DatetimeIndex):
        raise TypeError("❌ 输入必须为 DatetimeIndex")
        
    df = df.sort_index()
    # 重采样至 1S，线性插值填补间隙（limit=60 防长间隙扭曲）
    df_1hz = df.resample('1s').mean().interpolate(method='linear', limit=60)
    df_1hz = df_1hz.ffill().bfill()  # 首尾安全填充
    return df_1hz

def run_standalone_test():
    """仅用于独立单元测试（非主流程）"""
    dates = pd.date_range("2023-01-01", periods=10, freq="1min")
    mock_df = pd.DataFrame({"aggregate": [100]*10, "kettle": [0]*5+[10]*5}, index=dates)
    df_1hz = resample_to_1hz(mock_df)
    print(f"✅ 单元测试通过 | 1min→1Hz | 样本: {len(mock_df)}→{len(df_1hz)}")

if __name__ == "__main__":
    run_standalone_test()