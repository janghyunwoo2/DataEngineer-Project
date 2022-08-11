import sys, os, re
import json
import datetime, iso8601
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, DateType, TimestampType
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession, Row
import pyspark
import pyspark.sql
import pymongo_spark

# pyspark - kenesis 
from pyspark.streaming import StreamingContext
from pyspark.streaming.kinesis import KinesisUtils, InitialPositionInStream

# pymongo 활성화
pymongo_spark.activate()

sc = pyspark.SparkContext()
spark = pyspark.sql.SparkSession(sc).builder.getOrCreate() 

# spark streaming 객체 생성. 주기는 10초
PERIOD = 10
ssc = StreamingContext(sc, PERIOD)

# spark streaming 객체와 키네시스와 통신하는 객체 생성
kinesisStream = KinesisUtils.createStream(ssc, "pyspark_kinesis_app_2", "datastream_2", 'kinesis.ap-northeast-2.amazonaws.com','ap-northeast-2', InitialPositionInStream.LATEST, 10000)

# 전송된 인코딩된 json 객체를 디코딩해서 가져온다.
object_stream = kinesisStream.map(lambda x: json.loads(x))

# json 객체를 Row 객체로 변환
row_stream = object_stream.map(
    lambda x: Row(
      FlightDate=iso8601.parse_date(x['FlightDate']),
      Origin=x['Origin'],
      Distance=x['Distance'],
      DayOfMonth=x['DayOfMonth'],
      DayOfYear=x['DayOfYear'],
      DepDelay=x['DepDelay'],
      DayOfWeek=x['DayOfWeek'],
      FlightNum=x['FlightNum'],
      Dest=x['Dest'],
      Timestamp=iso8601.parse_date(x['Timestamp']),
      Carrier=x['Carrier'],
      UUID=x['UUID']
    )
  )


base_path = 'Project/03_PredictionModel_RealTime_Processing'

####  StringIndexerModel 객체 로드 ####
from pyspark.ml.feature import StringIndexerModel

string_indexer_models = {}
for column in ["Carrier", "Origin", "Dest", "Route"]:
    string_indexer_model_path = "{}/models/string_indexer_model_{}.bin".format(
      base_path,
      column
    )
    string_indexer_model = StringIndexerModel.load(string_indexer_model_path)
    string_indexer_models[column] = string_indexer_model

####  VectorAssembler객체 로드 ####
from pyspark.ml.feature import VectorAssembler
vector_assembler_path = "{}/models/numeric_vector_assembler.bin".format(base_path)
vector_assembler = VectorAssembler.load(vector_assembler_path)

####  RandomForestClassificationModel 객체 로드 ####
from pyspark.ml.classification import RandomForestClassifier, RandomForestClassificationModel

random_forest_model_path = "{}/models/spark_random_forest_classifier.flight_delays.5.0.bin".format(
      base_path
  )
rfc = RandomForestClassificationModel.load(
    random_forest_model_path
  )

# 아틀라스로 보내는 코드 있음. 아틀라스 클러스터 수정되였다면 그에 맞게 수정해야함.
# 주소 뒷부분에 test.my_collection? 넣어 줘야함
def classify_prediction_requests(row_stream):
    
    # 스키마
    schema = StructType([
        StructField("Carrier", StringType(), True),
        StructField("DayOfMonth", IntegerType(), True),
        StructField("DayOfWeek", IntegerType(), True),
        StructField("DayOfYear", IntegerType(), True),
        StructField("DepDelay", DoubleType(), True),
        StructField("Dest", StringType(), True),
        StructField("Distance", DoubleType(), True),
        StructField("FlightDate", DateType(), True),
        StructField("FlightNum", StringType(), True),
        StructField("Origin", StringType(), True),
        StructField("Timestamp", TimestampType(), True),
        StructField("UUID", StringType(), True),
    ])

    # Row 형태인 데이터를 DF로 전환
    prediction_requests_df = spark.createDataFrame(row_stream, schema=schema)
    
    # 운항 경로 생성하기
    from pyspark.sql.functions import lit, concat
    features_with_route = prediction_requests_df.withColumn(
        'Route',
        concat(
          prediction_requests_df.Origin,
          lit('-'),
          prediction_requests_df.Dest
        )
      )
    prediction_requests_with_route = features_with_route
    
    #### 불러온 StringIndexer 사용 ####
    for column in ["Carrier", "Origin", "Dest", "Route"]:
        string_indexer_model = string_indexer_models[column]
        prediction_requests_with_route = string_indexer_model.transform(prediction_requests_with_route)        
        
    #### 불러온 vectorize 사용 ####
    final_vectorized_features = vector_assembler.transform(prediction_requests_with_route)

    # 필요없는 컬럼 제거
    index_columns = ["Carrier_index", "Origin_index",
                       "Dest_index", "Route_index"]
    for column in index_columns:
        final_vectorized_features = final_vectorized_features.drop(column)

    #### 불러온 RandomForestClassificationModel 사용 ####
    predictions = rfc.transform(final_vectorized_features) # 예측하기

    # 필요없는 컬럼 제거  
    predictions = predictions.drop("Features_vec")
    final_predictions = predictions.drop("indices").drop("values").drop("rawPrediction").drop("probability")
    
    final_predictions.select('prediction').show(5)
    
    # mongoDB에 결과 값 저장
    if final_predictions.count() > 0:     
        final_predictions.rdd.map(lambda x:x.asDict()).saveToMongoDB('mongodb://jang:jang@ac-yquygh3-shard-00-00.u70vq2s.mongodb.net:27017,ac-yquygh3-shard-00-01.u70vq2s.mongodb.net:27017,ac-yquygh3-shard-00-02.u70vq2s.mongodb.net:27017/test.my_collection?ssl=true&replicaSet=atlas-tc2w1h-shard-0&authSource=admin&retryWrites=true&w=majority')

# 키네시스에서 가져온 데이터(Row)에 classify_prediction_requests 함수를 적용
row_stream.foreachRDD(classify_prediction_requests)

# 스트리밍 시작
ssc.start()
ssc.awaitTermination()