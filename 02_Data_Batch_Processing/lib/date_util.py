import iso8601, datetime

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