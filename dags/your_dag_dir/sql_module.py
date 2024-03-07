import logging
import traceback
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.utils import AnalysisException

# 创建 SparkSession
spark = SparkSession.builder \
    .appName("HiveAPI") \
    .enableHiveSupport() \
    .getOrCreate()

def upload_data(data_list, table_name, file_name):
    """
    将数据上传到指定的 Hive 表中。

    Args:
        data_list (list): 包含数据的列表，每个元素是一个包含两个值的元组。
        table_name (str): 目标 Hive 表的名称。
    Returns:
        データ登録失敗
    """

    rdd = spark.sparkContext.parallelize(data_list)
    df = rdd.map(lambda x: Row(id=x[0], name=x[1])).toDF()

        # データHive入力
    df.write.mode("append").saveAsTable(table_name)
