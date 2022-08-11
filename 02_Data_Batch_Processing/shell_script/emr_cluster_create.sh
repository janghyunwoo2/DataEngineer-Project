# 생성할 EMR 클러스터 생성하기. 반환값은 생성된 클러스터ID
ClusterId="`aws emr create-cluster \
--auto-scaling-role EMR_AutoScaling_DefaultRole \
--applications Name=Hadoop Name=Spark Name=Zeppelin \
--log-uri 's3n://aws-logs-848045215644-ap-northeast-2/elasticmapreduce/' \
--ebs-root-volume-size 10 \
--ec2-attributes '{\"KeyName\":\"awsjang\",\"InstanceProfile\":\"EMR_EC2_DefaultRole\",\"SubnetId\":\"subnet-1753325b\",\"EmrManagedSlaveSecurityGroup\":\"sg-0bf4c038fa25023d8\",\"EmrManagedMasterSecurityGroup\":\"sg-0c88614dfcc57172a\"}' \
--service-role EMR_DefaultRole \
--release-label emr-5.23.0 \
--name 'emr_model_training' \
--instance-groups '[{\"InstanceCount\":2,\"BidPrice\":\"OnDemandPrice\",\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"r4.xlarge\",\"Name\":\"코어 인스턴스 그룹 - 2\"},{\"InstanceCount\":1,\"BidPrice\":\"OnDemandPrice\",\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"r4.xlarge\",\"Name\":\"마스터 인스턴스 그룹 - 1\"}]'  \
--scale-down-behavior TERMINATE_AT_TASK_COMPLETION \
--region ap-northeast-2 \
--bootstrap-actions Path=\"s3://jhw620/EMR_Install_File/install_user_passwd.sh\" Path=\"s3://jhw620/EMR_Install_File/assign_private_ip.py\",Args=[\"$1\",\"$2\"]`"

#--ec2-attributes '{\"KeyName\":\"awsjang\",\"InstanceProfile\":\"EMR_EC2_DefaultRole\",\"SubnetId\":\"subnet-1753325b\",\"EmrManagedSlaveSecurityGroup\":\"sg-0bf4c038fa25023d8\",\"EmrManagedMasterSecurityGroup\":\"sg-0c88614dfcc57172a\"}' \
#Path=\"s3://jhw620/EMR_Install_File/assign_private_ip.py\",Args=[\"$1\"]
#Path=\"s3://jhw620/EMR_Install_File/install_user_passwd.sh\"

#--instance-groups '[{\"InstanceCount\":2,\"BidPrice\":\"OnDemandPrice\",\"InstanceGroupType\":\"CORE\",\"InstanceType\":\"r3.xlarge\",\"Name\":\"코어 인스턴스 그룹 - 2\"},{\"InstanceCount\":1,\"BidPrice\":\"OnDemandPrice\",\"InstanceGroupType\":\"MASTER\",\"InstanceType\":\"r3.xlarge\",\"Name\":\"마스터 인스턴스 그룹 - 1\"}]'  \

# a = f989bb91
# b = f92acc82
# c = 1753325b
# d = d6665f8a


# 위 ClusterId 값은 json이므로 jq로 값을 찾는다.
ClusterId="$(echo $ClusterId | jq '.ClusterId')"

# 양옆에 "(큰따옴표)까지 들어가있어서 제거해준다.
ClusterId="${ClusterId#\"}"
ClusterId="${ClusterId%\"}"

# 클러스터ID로 클러스터 상태가 WAITING 될때까 반복
echo "EMR 클러스터 생성이 완료 될 때까지 시간이 필요합니다."
while true; do
     resp="`aws emr describe-cluster --cluster-id $ClusterId`"
     echo "EMR 클러스터 생성 중..."
     if [ "`echo \"$resp\" | grep 'WAITING'`" ]; then
         break
     elif [ "`echo \"$resp\" | grep \"'TERMINATING\|TERMINATED\|TERMINATED_WITH_ERRORS'\"`" ]; then
         echo "EMR 클러스터 생성 오류"
         exit "EMR 클러스터 생성 오류"
         break
     fi
     sleep 5
done
echo "EMR 클러스터 생성 완료"


# 나중에 클러스터 종료하기위해 xcom put
echo "$ClusterId"