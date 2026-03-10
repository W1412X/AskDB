import os
import sys

# 将src目录添加到Python路径（在导入之前）
src_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, src_dir)
from stages.intent_divide import divide_intents

if __name__ == "__main__":
    result = divide_intents(
        query="检查用户表中手机号格式是否正确，订单表中是否存在空值，以及订单表和订单item表是否严格对应",
        database_names=["industrial_monitoring"],
        model_name="qwen3-max",
        max_retry_attempts=3,
        verbose=True,
    )
    print(result.to_dict())