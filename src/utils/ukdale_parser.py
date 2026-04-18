# src/utils/ukdale_parser.py
# UK-DALE 官方 .dat 二进制解析器 + 标签映射
import numpy as np
import pandas as pd
from pathlib import Path

def read_ukdale_dat(file_path: Path, max_samples: int = None) -> pd.Series:
    """
    解析 UK-DALE .dat 文件 (uint64 timestamp + int32 power)
    max_samples: 安全截断参数，首次测试建议 3600 (1小时)
    """
    # 二进制结构: 小端序 uint64(8B) 时间戳 + int32(4B) 功率
    dtype = np.dtype([('timestamp', '<u8'), ('power', '<i4')])
    count = max_samples if max_samples else -1
    data = np.fromfile(file_path, dtype=dtype, count=count)
    
    s = pd.Series(data['power'], index=pd.to_datetime(data['timestamp'], unit='s'))
    s.name = Path(file_path).stem
    return s

def load_ukdale_house1(data_dir: Path, target_app: str = "kettle", test_minutes: int = None) -> pd.DataFrame:
    labels_path = data_dir / "labels.dat"
    if not labels_path.exists():
        raise FileNotFoundError("❌ labels.dat 缺失，请确认数据完整性")

    # 1. 解析通道映射
    mapping = {}
    with open(labels_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'): continue
            parts = line.split()
            if len(parts) >= 2:
                ch_id = int(parts[0].replace('channel_', ''))
                mapping[ch_id] = parts[1].lower()

    # 2. 查找目标电器
    target_ch = next((ch for ch, app in mapping.items() if target_app in app), None)
    if target_ch is None:
        raise ValueError(f"❌ 未找到 '{target_app}'。可用通道: {mapping}")
    
    print(f"📍 定位成功: {target_app} → channel_{target_ch} ({mapping[target_ch]})")

    # 3. 安全加载（限制样本数防 OOM）
    max_s = test_minutes * 60 if test_minutes else None
    mains = read_ukdale_dat(data_dir / "mains.dat", max_samples=max_s)
    target = read_ukdale_dat(data_dir / f"channel_{target_ch}.dat", max_samples=max_s)

    # 4. 合并对齐
    df = pd.DataFrame({'aggregate': mains, target_app: target})
    df = df.dropna(how='all').sort_index()
    
    if test_minutes:
        print(f"⚠️ 测试模式: 仅加载前 {test_minutes} 分钟 ({len(df)} 样本)")
    return df