"""
测试initialize_agent.py
"""
import os
import sys

# 将src目录添加到Python路径（在导入之前）
src_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, src_dir)

from stages.initialize.agent.run import initialize_databases

if __name__ == "__main__":
    result = initialize_databases(["industrial_monitoring"])
    print(result)