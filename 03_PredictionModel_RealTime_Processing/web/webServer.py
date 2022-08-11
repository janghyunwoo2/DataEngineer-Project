from flask import Flask, render_template, request
import sys, random, time, argparse, json, math, datetime,iso8601
from pymongo import MongoClient
from bson import json_util
from boto import kinesis

import uuid

# 몽고DB랑 연결하기위한 객체
# 아틀라스 클러스터 바뀌면 그에 맞게 수정해 줘야함 블로그 참고~
# https://blog.naver.com/ehrtk2002/221527266211
client = MongoClient('mongodb://jang:jang@ac-yquygh3-shard-00-00.u70vq2s.mongodb.net:27017,ac-yquygh3-shard-00-01.u70vq2s.mongodb.net:27017,ac-yquygh3-shard-00-02.u70vq2s.mongodb.net:27017/?ssl=true&replicaSet=atlas-tc2w1h-shard-0&authSource=admin&retryWrites=true&w=majority') 
db = client.test

# 플라스크 setup
app = Flask(__name__)

# 공항 사이 거리를 몽고DB에서 가져오는 함수
def get_flight_distance(client, origin, dest):
  query = {
    "Origin": origin,
    "Dest": dest,
  }
  record = client.Distance.find_one(query)
  return record["Distance"]

# 사용자가 입력한 날짜값을 파싱하는 함수
def get_regression_date_args(iso_date):
  dt = iso8601.parse_date(iso_date)
  day_of_year = dt.timetuple().tm_yday
  day_of_month = dt.day
  day_of_week = dt.weekday()
  return {
    "DayOfYear": day_of_year,
    "DayOfMonth": day_of_month,
    "DayOfWeek": day_of_week,
  }

# 데이터가 들어온 시간을 반환하는 함수
def get_current_timestamp():
  iso_now = datetime.datetime.now().isoformat()
  return iso_now

# 키네시스로 데이터 전송하는 함수
def put_words_in_stream(w):
    conn = kinesis.connect_to_region(region_name = 'ap-northeast-2')
    stream_name = "datastream_2"    
    conn.describe_stream(stream_name)
    w = json.dumps(w)
    try:
        conn.put_record(stream_name, w,"partitionkey")
        print("Put word: " + w + " into stream: " + stream_name)
    except Exception as e:
        sys.stderr.write("Encountered an exception while trying to put a word: "
                             + w + " into stream: " + stream_name + " exception was: " + str(e))


# form으로 submit 되어진 데이터를 파싱하는 라우팅
@app.route("/flights/delays/predict/classify_realtime", methods=['POST'])
def classify_flight_delays_realtime():
  
  # 보내진 데이터에 타입을 미리 정의
  api_field_type_map = \
    {
      "DepDelay": float,
      "Carrier": str,
      "FlightDate": str,
      "Dest": str,
      "FlightNum": str,
      "Origin": str
    }

  # 보내진 데이터에 타입 지정
  api_form_values = {}
  for api_field_name, api_field_type in api_field_type_map.items():
    api_form_values[api_field_name] = request.form.get(api_field_name, type=api_field_type)
  
  # 키네시스로 보낼 데이터를 담는 딕셔너리 생성
  prediction_features = {}
  for key, value in api_form_values.items():
    prediction_features[key] = value

  
  # Distance 키 생성하고 공항간 거리 값 넣기
  prediction_features['Distance'] = get_flight_distance(
    db, api_form_values['Origin'],
    api_form_values['Dest']
  )
  
  # 날짜데이터 파싱
  date_features_dict = get_regression_date_args(
    api_form_values['FlightDate']
  )

  # 파싱한 날짜데이터를 키네시스로 보낼 딕셔너리에 저장
  for api_field_name, api_field_value in date_features_dict.items():
    prediction_features[api_field_name] = api_field_value
  
  # 데이터가 들어온 시간을 키네시스로 보낼 딕셔너리에 저장
  prediction_features['Timestamp'] = get_current_timestamp()
  
  # 몽고DB에서 데이터를 찾기 위한 ID를 키네시스로 보낼 딕셔너리에 저장
  unique_id = str(uuid.uuid4())
  prediction_features['UUID'] = unique_id
  
  # 키네시스로 데이터 보네기
  put_words_in_stream(prediction_features)
    
  # 클라이언트로 응답 보내기(id로 클라이언트에서 몽고DB에서 데이터 찾는다)
  response = {"status": "OK", "id": unique_id}
  return json_util.dumps(response)


# 첫 페이지 랜더링
@app.route("/flights/delays/predict")
def flight_delays_page():

  # 페이지에 전송할 요소
  form_config = [
    {'field': 'FlightNum','label': '항공편 번호'},
      {'field': 'FlightDate','label': '항공편 날짜'},
      {'field': 'Carrier','label': '항공사'},
      {'field': 'Origin','label': '출발지'},
      {'field': 'Dest','label': '도착지'},
      {'field': 'DepDelay','label': '출발 지연시간'},
  ]  
  
  # jinja2 탬플릿
  return render_template('flight_delays_predict.html', form_config=form_config)

# 몽고DB에 ID값으로 데이터가 들어왔는지 확인하는 랜더링
@app.route("/flights/delays/predict/classify_realtime/response/<unique_id>")
def classify_flight_delays_realtime_response(unique_id):
  
  # 몽고DB에 ID로 검색해서 데이터가 있는지 확인
  mongoDBresult = db.my_collection.find_one(
    {
      "UUID": unique_id
    }
  )
  
  # 몽고DB에 데이터 없다면 WAIT, 있다면 OK 
  response = {"status": "WAIT", "id": unique_id}
  if mongoDBresult is not None:
    response["status"] = "OK"
    response["prediction"] = mongoDBresult['prediction']
  
  return json_util.dumps(response)

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')
