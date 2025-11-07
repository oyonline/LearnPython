# tests/conftest.py
import os, sys
# 把项目根目录加入 sys.path（tests 的上一级目录）
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
