# ssh 접속해서 개발컴이랑 연결
echo "ssh 접속 중.."
ssh-keygen -R $1
ssh-keygen -R $2
ssh_Connection="`sshpass -p 1234 ssh -oStrictHostKeyChecking=no hadoop@$2 \"ls\"`"

ssh_result="$?"

# ssh 접속 후 결과 값이 0이 아니면 접속 오류
if [ $ssh_result -ne "0" ]; then
    exit "ssh 접속 오류"
fi

echo "SSH 성공"