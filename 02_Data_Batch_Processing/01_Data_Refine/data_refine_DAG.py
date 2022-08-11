# -*- coding: utf-8 -*-
# airflow DAG에서 사용하는 파이썬 파일
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks import SSHHook
from datetime import datetime,timedelta
from airflow.utils import trigger_rule

import iso8601
import sys, os, re, subprocess

'''하루전 데이터를 처리하기 위해 현재시간에서 하루 뺀 시간'''
now = datetime.now() - timedelta(days=1)

year = now.year
month = now.month
day = now.day

if month < 10:
    month = '0'+str(month)

if day < 10:
    day = '0'+str(day)

# 테스트를 위해 설정한 시간
year = '2019'
month = '07'
day = '10'

now =  "{}-{}-{}".format(year, month, day)
'''-------------------------------------------- '''

ip_address = '172.31.20.78'
ip_domain = 'ip-172-31-20-78.ap-northeast-2.compute.internal'
region = 'ap-northeast-2'
project_home = '~/Project/02_Data_Batch_Processing'

# SSH 연결을 위한 SSHHook 정의
sshHook = SSHHook(ssh_conn_id='emr_cluster_conn',remote_host='{}'.format(ip_domain),username='hadoop',password='1234')

# DAG 정의
dag = DAG('DAG_data_refine',
                        schedule_interval = '*/30 * * * *',
                        start_date=datetime(2019, 8, 1), catchup=False)

# EMR 클러스터 생성하는 task
t1 = BashOperator(
  task_id = "emr_cluster_create",
  xcom_push=True,
  bash_command = """bash {}/shell_script/emr_cluster_create.sh {} {}""".format(project_home, ip_address, region),
  dag=dag
)

# EMR 클러스터에 원격으로 커맨드를 실행하기 위한 전초작업
# 확실하지 않지만, SSH 접속을 한번 해두어야 Airflow SSHOperator로 접속이 가능했었다.
t2 = BashOperator(
  task_id = "emr_ssh_connect",
  bash_command = """bash {}/shell_script/emr_ssh_connect.sh {} {}""".format(project_home,ip_address, ip_domain),
  dag=dag
)

# 하둡의 HDFS에 접속하기 위해선 보조IP가 아닌 메인IP로 해야된다. 그래서 보조IP를 메인IP에 포워딩 해준다.
# EMR 클러스터의 보조IP를 메인IP에 포워딩
t3 = SSHOperator(
        task_id="ip_forwarding",
        command="""(echo $(sudo ifconfig eth0 | grep 'inet addr' | cut -d: -f2 | awk '{{ print $1 }}') " {}") | sudo tee -a /etc/hosts""".format(ip_domain),
        ssh_hook=sshHook,
        dag=dag)


# S3에 있는 훈련데이터를 EMR 클러스터의 HDFS로 옴긴다.
t4 = SSHOperator(
        task_id="rawdata_s3_to_hdfs",
        command="""s3-dist-cp --src s3://jhw620/RawData/{}/{}/{}/ --dest hdfs://{}:8020/data/""".format(year,month,day,ip_domain),
        ssh_hook=sshHook,
        dag=dag)


# 데이터 정제하기
t5 = BashOperator(
  task_id = "data_refine",
  bash_command = """
export HADOOP_CONF_DIR='/home/ubuntu/project1/hadoop/etc/hadoop-datatransform';
export YARN_CONF_DIR='/home/ubuntu/project1/hadoop/etc/hadoop-datatransform';
spark-submit --deploy-mode client --master yarn --num-executors 4 --executor-cores 4 --executor-memory 18G {}/01_Data_Refine/data_refine.py
""".format(project_home),
  dag=dag
)

# 정제된 데이터 s3로 옴기기
t6 = SSHOperator(
        task_id="refined_data_hdfs_to_s3",
        command="""s3-dist-cp --src hdfs://{}:8020/result/ --dest s3://jhw620/RefineData/{}-{}-{}/""".format(ip_domain,year,month,day),
        ssh_hook=sshHook,
        dag=dag)

# glue크롤링 실행해서 테이블 업데이트 하기
t7_1 = BashOperator(
  task_id = "start_crawler",
  bash_command = """aws glue start-crawler --region ap-northeast-2 --name s3_rawdata_table""".format(now,now),
  dag=dag
)

# EMR 클러스터를 종료한다.
t7_2 = BashOperator(
  task_id = "emr_cluster_close",
  bash_command = """aws emr terminate-clusters --cluster-ids {{ ti.xcom_pull("emr_cluster_create") }}""",
  dag=dag
)
# 모든 클러스터 작업이 완료(실패든 성공이든)하면 EMR 클러스터를 종료한다.
t7_2.trigger_rule = trigger_rule.TriggerRule.ALL_DONE

# task 순서
t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> [t7_1, t7_2]
