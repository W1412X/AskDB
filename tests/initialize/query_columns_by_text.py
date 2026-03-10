import os
import sys

# 将src目录添加到Python路径（在导入之前）
src_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, src_dir)
from stages.initialize.embedding.query import get_columns_by_text

if __name__ == "__main__":
    result = get_columns_by_text("工厂的设备编号ID有没有重复的",["industrial_monitoring"])
    result=sorted(result,key=lambda x: x["similarity"],reverse=True)
    for item in result:
        print(item["database_name"],item["table_name"],item["column_name"],item["similarity"])