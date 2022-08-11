from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, DateType, TimestampType
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
import pyspark
import pyspark.sql
import os, sys, iso8601, datetime

# 시간를 문자열이나 숫자가 아니라 타임스탬프로 전환해야 된다.
def convert_hours(hours_minutes):
    hours = hours_minutes[:-2]
    minutes = hours_minutes[-2:]
  
    if hours == '24':
        hours = '23'
        minutes = '59'
  
    time_string = "{}:{}:00Z".format(hours, minutes)
    return time_string

def compose_datetime(iso_date, time_string):
    return "{} {}".format(iso_date, time_string)

def create_iso_string(iso_date, hours_minutes):
    time_string = convert_hours(hours_minutes)
    full_datetime = compose_datetime(iso_date, time_string)
    return full_datetime

def create_datetime(iso_string):
    return iso8601.parse_date(iso_string)

def convert_datetime(iso_date, hours_minutes):
    iso_string = create_iso_string(iso_date, hours_minutes)
    dt = create_datetime(iso_string)
    return dt

def day_of_year(iso_date_string):
    dt = iso8601.parse_date(iso_date_string)
    doy = dt.timetuple().tm_yday
    return doy

def alter_feature_datetimes(row):
  
    flight_date = iso8601.parse_date(row['FlightDate'])
    scheduled_dep_time = convert_datetime(row['FlightDate'], row['CRSDepTime'])
    scheduled_arr_time = convert_datetime(row['FlightDate'], row['CRSArrTime'])
  
    # 출발,도착 예상시간은 HHMM 이기 때문에 다음날을 표현을 못한다.
    # 그래서 출발 이후 날짜가 변경되었다면 하루(days=1)를 추가해 주는것이다.
    # 야간 운항 처리 
    if scheduled_arr_time < scheduled_dep_time:
        scheduled_arr_time += datetime.timedelta(days=1)
  
    doy = day_of_year(row['FlightDate'])
  
    return {
      'FlightNum': row['FlightNum'],
      'FlightDate': flight_date,
      'DayOfWeek': int(row['DayOfWeek']),
      'DayOfMonth': int(row['DayOfMonth']),
      'DayOfYear': doy,
      'Carrier': row['Carrier'],
      'Origin': row['Origin'],
      'Dest': row['Dest'],
      'Distance': row['Distance'],
      'DepDelay': row['DepDelay'],
      'ArrDelay': row['ArrDelay'],
      'CRSDepTime': scheduled_dep_time,
      'CRSArrTime': scheduled_arr_time,
    }

project_home = ''

# 스파크 객체 생성
conf = pyspark.SparkConf().setAll([('spark.driver.memory', '2g')])
sc = pyspark.SparkContext(conf=conf)
spark = pyspark.sql.SparkSession(sc).builder.getOrCreate()
spark.conf.set("spark.sql.files.ignoreCorruptFiles","true")

def main():
    #데이터 가져오기
    refined_data = spark.read.parquet("{}/data/*".format(project_home))

    # 테이블 등록
    refined_data.registerTempTable("Refined_Data")

    # 모델 훈련에 쓰일 데이터 생성
    training_yet_data = spark.sql("""
    SELECT
      FlightNum,
      FlightDate,
      DayOfWeek,
      DayofMonth AS DayOfMonth,
      CONCAT(Month, '-',  DayofMonth) AS DayOfYear,
      Carrier,
      Origin,
      Dest,
      Distance,
      DepDelay,
      ArrDelay,
      CRSDepTime,
      CRSArrTime
    FROM Refined_Data
    """)
    
    # alter_feature_datetimes : 날짜 파싱
    training_yet_data=training_yet_data.rdd.map(alter_feature_datetimes).toDF()
    
    # 항공편 번호를 운항 경로로 대체하기
    from pyspark.sql.functions import lit, concat

    features_with_route = training_yet_data.withColumn(
      'Route',
      concat(
        training_yet_data.Origin,
        lit('-'),
        training_yet_data.Dest
      )
    )
    
    #### Bucketizer:목표변수 분류 클래스 나누기 ####
    from pyspark.ml.feature import Bucketizer

    splits = [-float("inf"), -15.0, 0, 30.0, float("inf")]
    bucketizer = Bucketizer(
      splits=splits,
      inputCol="ArrDelay", #원시 목표변수
      outputCol="ArrDelayBucket" #클래스 나뉜 목표변수
    )

    # Bucketizer 객체 저장
    bucketizer_path = "{}/models/arrival_bucketizer_2.0.bin".format(project_home)
    print(bucketizer_path)
    bucketizer.write().overwrite().save(bucketizer_path)

    # Bucketizer로 데이터 변환
    ml_bucketized_features = bucketizer.transform(features_with_route)
    
    #### StringIndexer : String 타입의 범주 값을 해당 값의 정수 번호로 변환 ####
    from pyspark.ml.feature import StringIndexer

    for column in ["Carrier", "Origin", "Dest", "Route"]:
        string_indexer = StringIndexer(
        inputCol=column,
        outputCol=column + "_index"
        )
        string_indexer_model = string_indexer.fit(ml_bucketized_features)
        ml_bucketized_features = string_indexer_model.transform(ml_bucketized_features)

        ml_bucketized_features = ml_bucketized_features.drop(column)

        # StringIndexer 객체 저장
        string_indexer_output_path = "{}/models/string_indexer_model_{}.bin".format(
          project_home,
          column
        )
        print(string_indexer_output_path)
        string_indexer_model.write().overwrite().save(string_indexer_output_path)
    
    #### VectorAssembler: 데이터를 벡터화 하기 ####
    from pyspark.ml.feature import VectorAssembler

    numeric_columns = ["DepDelay", "Distance",
        "DayOfMonth", "DayOfWeek",
        "DayOfYear"]
    index_columns = ["Carrier_index", "Origin_index",
                       "Dest_index", "Route_index"]
    vector_assembler = VectorAssembler(
      inputCols=numeric_columns + index_columns,
      outputCol="Features_vec"
    )
    training_data = vector_assembler.transform(ml_bucketized_features)

    # VectorAssembler 객체 저장
    vector_assembler_path = "{}/models/numeric_vector_assembler.bin".format(project_home)
    print(vector_assembler_path)
    vector_assembler.write().overwrite().save(vector_assembler_path)

    # 필요없는 컬럼 제거
    for column in index_columns:
        training_data = training_data.drop(column)
        

    # 모델 : 랜덤포레스트
    from pyspark.ml.classification import RandomForestClassifier
    rfc = RandomForestClassifier(
      featuresCol="Features_vec",
      labelCol="ArrDelayBucket",
      maxBins=4657,
      maxMemoryInMB=1024,
      numTrees = 10,
      maxDepth = 10
    )
    
    # 훈련시작
    model = rfc.fit(training_data)

    # 모델 객체 저장
    model_output_path = "{}/models/spark_random_forest_classifier.flight_delays.5.0.bin".format(
        project_home
      )
    print(model_output_path)
    model.write().overwrite().save(model_output_path)
    
    
if __name__ == "__main__":
    main()