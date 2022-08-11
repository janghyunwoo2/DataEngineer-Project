// form submit 이벤트 발생할 때 실행
$( "#flight_delay_classification" ).submit(function( event ) { 
  console.log("요청 ID 응답 대기 중..wefwefwef. ");
  // form submit 일반적인 전송 막기
  event.preventDefault();

  // form 요소 가져오기
  var $form = $( this ),
    term = $form.find( "input[name='s']" ).val(),
    url = $form.attr( "action" );

  // post 방식으로 직렬화해서 데이터 전송
  var posting = $.post(
    url,
    $( "#flight_delay_classification" ).serialize()
  );
  
  // 응답 후 실행
  posting.done(function( data ) {
    response = JSON.parse(data);

    // 응답 메시지가 OK이면 '응답 대기중'
    if(response.status == "OK") {
      $( "#result" ).empty().append( "응답대기 중.." );

      // 1초에 한번 씩 id를 가지고 다시 OK 응답이 올때까지 반복
      poll(response.id);
    }
  });
});

// 서버에 ID값으로 몽고DB에 요청하기위해 비동기적으로 요청하는 함수
function poll(id) {
  var responseUrlBase = "/flights/delays/predict/classify_realtime/response/";
  console.log("요청 ID 응답 대기 중... " + id );

  var predictionUrl = responseUrlBase + id;

  // ajax로 비동기식 실행
  $.ajax(
  {
    url: predictionUrl,
    type: "GET",
    complete: conditionalPoll
  });
}

// 응답된 데이터에 따른 핸들러 함수
function conditionalPoll(data) {
  var response = JSON.parse(data.responseText);

  // 응답된 데이터가 OK 라면 그 결과를 화면에 표시
  if(response.status == "OK") {
    renderPage(response.prediction);
  }// WAIT라면 1초 간격으로 다시 서버에 요청
  else if(response.status == "WAIT") {
    setTimeout(function() {poll(response.id)}, 1000);
  }
}

// 예측 결과에 따른 클래스 나누기
function renderPage(response) {

  var displayMessage = '잘못된 값';

  if(response == '0') {
    displayMessage = "15분 이상 이른 도착 예정";
  }
  else if(response == '1') {
    displayMessage = "0~15분 이른 도착 예정";
  }
  else if(response == '2') {
    displayMessage = "0~30분 늦은 도착 예정";
  }
  else if(response == '3') {
    displayMessage = "30분 이상 늦은 도착 예정";
  }

  // 화면의 표시
  $( "#result" ).empty().append( displayMessage );
}
