# 이 파이썬 파일은 같은 폴더에 있는 data_refine_DAG.py 에서 실행됌
import pyspark, os
import pyspark.sql

conf = pyspark.SparkConf().setAll([('spark.driver.memory', '2g')])
sc = pyspark.SparkContext(conf=conf)
spark = pyspark.sql.SparkSession(sc).builder.appName('jangDataTransform').getOrCreate()

project_home = ''

# null값을 카운트 한 다음, 퍼샌트로 만들어줌 (where 대신 filter 도 가능)
def column_nullCount_ToParcent(dataframe,column):
    return round((dataframe.where(dataframe[column].isNull()).count()/dataframe.count()) * 100,2)

def main():
    # 데이터 읽어오기
    raw_dataframe = spark.read.parquet("{}/data/*".format(project_home))

    # 테이블 등록
    raw_dataframe.registerTempTable("rawdata_table")
    
    # 각 컬럼 별로 null 값 검사(%)
    null_parcent = [(column, column_nullCount_ToParcent(raw_dataframe,column)) for column in raw_dataframe.columns]
    print(null_parcent)
    
    # null 비율이 10%이하인 컬럼 추출
    null_10parcent_down_col = []

    for column, nullParcent in null_parcent:
        if nullParcent < 10:
            null_10parcent_down_col.append(column)
    
    # 스파크SQL을 이용하여 null값이 10%이하인 컬럼(열)만 선택
    fit_dataframe = spark.sql(
      """SELECT {} FROM rawdata_table""".format(','.join(null_10parcent_down_col))
    )
    
    # 나머지 null인 행들 모두 제거 
    print("지우기 전:",fit_dataframe.count())
    fit_notnull_dataframe = fit_dataframe.na.drop()
    print("지운 후:",fit_notnull_dataframe.count())

    # 파퀘이로 저장
    fit_notnull_dataframe.repartition(1).write.mode('overwrite').parquet("{}/result/Refined_Data".format(project_home))
    os.system("hadoop fs -cp {}/result/Refined_Data/part* {}/result/Refined_Data.parquet".format(project_home,project_home)) # 파일 하나로 합치기
    os.system("hadoop fs -rm -r {}/result/Refined_Data".format(project_home)) # 이전 폴더 삭제
    
if __name__ == "__main__":
    main()