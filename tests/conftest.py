import sys
from pathlib import Path

# 将项目根目录加入 sys.path, 使 `from aqap import ...` 可用
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
# 添加 SDK 路径以确保跨层一致性测试可用
SDK_DIR = ROOT / "sdk"
sys.path.insert(0, str(SDK_DIR))
