# VARRAM 조회·추가 급여 프로토콜

## 확인 범위

2026-10-25에 사용자 제공 세션으로 실제 계정의 반려동물 목록과 최근 7일 급여·섭취 기록 조회를 확인했다. 급여 실행, 스케줄 변경, 푸시 등록, 계정 변경 요청은 실행하지 않았다. 아래 API는 공개 문서가 아닌 실제 웹앱 코드와 응답을 근거로 하므로 서버 변경 시 재검증한다. 인증정보와 개인정보는 이 문서 및 Git에 보관하지 않는다.

## 인증과 계정 목록

기준 호스트는 `https://web.varram-robot.com`이다. JSON 조회는 `auth: <access_token>` 헤더를 사용한다. 모바일 User-Agent, `app_version` 쿠키, 푸시용 `dev_token` 없이 조회가 성공했다.

- `GET /home/main`: 웹앱 실제 진입점. 화면은 access_token 쿠키를 사용한다. `/`는 모든 시험 User-Agent에 빈 HTTP 200을 반환했다.
- `GET /res/pet/info`: `{"type":"success","data":[{"pet_id":"...","name":"..."}]}`
- `GET /get/connected_pet`: `data.connect_list`의 pet_id, device_id, model_name으로 급식기 연결을 확인한다.
- `GET /res/dev/info`: 등록 기기 정보. 기록 센서 구현에는 불필요한 시리얼·MAC 등은 수집하지 않는다.
- 인증 없는 `/res/pet/info`는 HTTP 200이지만 `{"type":"fail","msg":"JsonWebTokenError"}`를 반환했다. HTTP 상태만으로 성공을 판단하지 않는다.

제공받은 JWT는 `userid`, `ptype`, `iat`, `exp` 필드를 가진 30일 유효 토큰이었다. 웹앱 `reference.js`의 GET_AT/GET_AT_Coolkie는 네이티브 앱이 넣어 준 쿠키와 localStorage를 읽는다. 이 코드의 토큰 재사용을 서버 갱신으로 간주하지 않는다.

네이티브 앱의 재로그인 캡처에서 실제 인증 API를 확인했다. `POST https://auth.varram-robot.com/v2/login`, `application/x-www-form-urlencoded`, 필드는 `email`, `password`다. 앱 User-Agent는 `(iPhone; CPU iPhone OS 18_7 like Mac OS X) pcss/app/vapp/2.3.7/KR`다. 성공은 HTTP 200과 `{"type":"success","data":{"access_token":"...","refresh_token":"..."}}`, 잘못된 자격증명은 HTTP 401과 `UnauthorizedError`였다. 쿠키 없이 같은 폼을 전송하여 새 토큰 발급, 기존 userid 일치 및 반려동물 2마리 조회까지 실제 검증했다. 발급 토큰의 유효기간은 30일이었다.

자동 갱신은 이메일·비밀번호 재로그인으로 구현한다. refresh_token의 존재는 확인했지만 교환 경로는 관찰하지 못했으므로 이를 저장하거나 추정 endpoint를 호출하지 않는다. 로그인 리다이렉트에 자격증명을 재전송하지 않는다. `resource.varram-robot.com/res/mobile/info`는 푸시 정보 등록이며 성공 응답에 토큰이 없으므로 갱신 API로 사용하지 않는다.

## 급여·섭취 기록

`POST /get/device/info/feed/v2/{pet_id}`는 기록 조회 요청이다.

```json
{"start_time":"2026-10-19 00:00:00","end_time":"2026-10-25 23:59:59"}
```

응답의 `data.data_list`에 `give_time`, `eat_time`, `status`, `gram`, `left`, `immediate`, `spend`, `eatCount`가 포함된다. 동일 give_time에 여러 누적 상태가 있으므로 행 수는 급여 횟수가 아니며 gram을 모든 행에 합산하면 중복이다. 조회 기간 안에 섭취한 이전 날의 끼니도 반환될 수 있다.

## 급식기 소켓 상태

웹앱의 `socketController.js`와 `homeMainController_hc.js`는 `wss://smb.varram-robot.com/?auth=<access_token>&target=<device_id>`에 연결한다. 연결 후 `{"type":"req_device_status","data":{"device_id":"...","msg_id":"JWT의 userid"}}`로 상태를 조회한다. 이 조회만 두 급식기에 실제 전송하여 `nt_device_status` 응답을 확인했다. 토큰이 URL에 들어가므로 URL·접속 오류 원문은 로그에 남기지 않는다.

응답 `data.feeding`은 현재 `type`과 `left_feed`, `power`는 전원 종류, `feed_lack`는 사료 부족 상태를 제공했다. 검증 당시 두 급식기의 waiting 잔량은 각각 20.07g와 22.77g로 최근 이력의 값과 일치했다. 이 소켓 응답에는 일별 섭취량이나 급여 이력 목록이 없으므로 HTTP 이력 조회를 함께 사용한다. 현재 잔량과 최근 기록 잔량은 별도 값이다. 그릇 제거는 0g 섭취로 처리하지 않는다. 설정 변경 명령은 전송하지 않는다. 추가 급여 명령의 근거와 검증 경계는 아래에 별도로 기록한다.

기기 변화 없이 수신되는 자발적 알림의 빈도·보장 여부는 검증하지 않았다. 소켓 연결만으로 모든 섭취 기록이 전달된다고 가정하지 않으며, 상태 조회와 5분 이력 조회를 유지한다.

두 급식기에 동시에 소켓을 열면 한쪽에서 1006 종료가 재현됐다. 앱 코드에도 급식기를 순회하며 조회하는 구조가 있어, 브리지는 한 계정에 소켓 하나만 열고 급식기를 차례로 조회한다. 두 기기의 실제 순차 수신을 검증했으며, 조회 중 waiting 잔량 변화와 eat 상태도 관찰했다. 약 60초 주기의 현재 상태 조회에서 변화가 보이면 10초 지연 후 이력을 다시 조회하고, 기본 5분 조회로도 보완한다. eat·feed_start 및 그릇 제거·급여 실패 상태에서는 확실하지 않은 현재 무게를 0g으로 게시하지 않는다.

## 추가 급여 명령

공개 웹앱 `homeMainController_hc.js`의 SHA256은 `d003f48079a0b951a2603a248b93a3d6beb9bdb97e1e1b347ac4e4bdd2f38a04`다.
`DC_feed_Popup`(14796–14824행)은 `optimization`을 사료 티칭 결과의 최소 급여량으로 표시한다.
`POST /get/mobile/info/feeder/{device_id}`에 `{"request_update_info":true,"request_check_optimization":true}`를 보내면
`data.optimization`, `opt_gain`, `opt_offset`, `update_info`가 반환된다. 실제 계정에서 이 조회만 실행하여
두 급식기의 optimization 6.69와 8.7을 확인했다. 개인 기기 식별자나 보정 계수는 저장하지 않는다.

`DC_send_req_feeding`(14877–14881행)과 `sendPacket_WS`(14951–14994행)의 명령은 다음과 같다.

```json
{"type":"req_feeding","data":{"device_id":"대상 기기","msg_id":"JWT userid","gram":1}}
```

`gram`은 표시 최소량과 관계없이 고정 1이다. 이를 실제 1g이나 임의 급여량 입력으로 해석하지 않는다.
현재 통합은 앱과 같은 최소량 추가 급여 버튼만 제공하며, 티칭 값이 없으면 실행하지 않는다.
`msg_id`는 사용자 ID이므로 요청별 중복 방지 키로 사용할 수 없다.

`TASK_req_feeding`(15186–15335행)은 먼저 같은 소켓으로 `req_device_status`를 조회한다.
앱은 `feed_start`, `none_bowl`, 미수신 상태에서 버튼을 막으며, 60g 초과 잔량은 경고한다.
통합은 더 보수적으로 60g 초과와 알려지지 않은 상태에서도 실행을 막는다.
한 계정의 조회·명령 소켓을 직렬화하고, 전송을 시도한 급식기는 60초 동안 다시 실행하지 않는다.

`rsp_feeding`은 ACK이며 실제 토출 완료 증거가 아니다. 앱은 이후 상태 변화와 `feed_finish`/`eat`를
관찰하고 `empty_feed`/`none_bowl`은 실패로 처리한다. 앱의 ACK·진행 대기 한도는 각각 10초이며 재전송은 없다.
통합 역시 명령 전송 시도 이후 타임아웃·연결 종료를 완료 불명으로 처리하고 자동 재전송하지 않는다.
기존 `eat` 상태를 새 명령 완료로 간주하거나 급여·섭취량을 낙관적으로 증가시키지 않는다.

2026-10-25 구현 검증은 모의 소켓·기록, 실제 읽기 전용 상태 및 Home Assistant 엔티티 등록으로 구분한다.
실제 추가 급여 명령을 보내 사료가 나오는지는 별도 실기 확인이 필요하다.

## 웹앱 계산 함수

실제 제공된 `/javascripts/varram/homeMainController_hc.js`의 주요 함수:

- `getDy_MealList` (관찰 시 21714행): 위 POST 조회와 기간 파라미터.
- `Convert_Ver2` (21581행): eat_time별 정렬과 상태 순서 처리.
- `LoadEatdata2` (21952행): 동일 give_time의 마지막 waiting 한 건으로 누적 섭취량 계산. 앱은 gram과 left를 각각 올림한 뒤 차를 사용하며 일부 0g 이벤트를 1g으로 표시한다.
- `getGiveData` (21027행): 끼니별 gram/immediate를 사용한 급여량 계산.
- `SbjDecodeMeal` (3250행 부근): 추가 급여, empty_feed, none_bowl 등을 구분한다.

일별 센서는 앱과 같은 끼니 기준(give_time 날짜)임을 표시하고, 실제 식사 시각 기준 하루 섭취량과 혼동하지 않는다. 기록상의 left는 마지막 관찰 잔량으로, 실시간 그릇 무게로 표현하지 않는다. 검증되지 않은 통계는 노출하지 않는다.

## 보안 및 검증 경계

실행 구현은 Home Assistant 내부의 Python 단독 통합이다. 토큰과 로그인 정보는 Home Assistant 설정 항목에 저장하고 로그·진단·Git에서 제외한다. 토큰 갱신 시 기존 userid 일치와 새 토큰 API 조회 성공을 확인한 뒤 반영한다. 인증 실패와 네트워크 장애를 정상적인 0g으로 바꾸지 않는다. 실제 데이터 조회, 집계 검증, Home Assistant 센서 등록·상태, 자동 재로그인 성공은 각각 별도로 확인한다.

## 집계 의미와 원본/앱 수치의 구분

2026-10-25 조회 자료에서 한 끼의 gram과 left가 모두 20.07g인데도 앱 보정으로 1g 섭취로 표시되는 사례를 확인했다. 따라서 원본 차이와 앱 표시값을 서로 다른 센서로 제공한다. `ceil(gram)-ceil(left)`와 조건부 1g 보정은 앱 비교용으로만 사용하며 실제 측정 섭취량으로 주장하지 않는다.

- 끼니 기준: 동일 give_time 그룹의 마지막 유효 waiting에서 gram-left를 계산하고 해당 give_time 날짜에 귀속한다. 누적 스냅샷을 전부 더하지 않는다.
- 앱 기준: 같은 그룹에 앱의 올림·최소 1g 규칙을 적용한다. 자정을 넘긴 섭취는 앱이 끼니 시작 날짜에 반영한다.
- 날짜 기준 관찰 변화(제공하는 경우): 같은 끼니의 누적 섭취량 증가분을 eat_time 날짜로 모은 추정치다. 직전 날짜 기록이 기준값으로 필요하며 샘플 간 발생 시각을 실제 순간 측정으로 해석하지 않는다.
- `eatCount`는 하드웨어/편집 카운터이며 식사 횟수가 아니다. 급여 기록수는 성공한 서로 다른 give_time 그룹 수로 정의한다.
- empty_feed는 실패, none_bowl은 식기 제거 이벤트다. 두 상태를 섭취나 정상적인 0g 잔량으로 취급하지 않는다.
- `gram-immediate` 기반 앱 급여량은 실제 추가 토출량의 독립 측정값으로 검증되지 않았다. 앱 기준 급여량으로만 표시한다.

개인정보를 제거한 고정 자료의 10월 25일 기대값: 첫 테스트 반려동물은 원본 끼니 섭취 0g / 앱 섭취 1g / 앱 급여 21g / 원본 잔량 20.07g, 둘째는 각각 3.33g / 4g / 27g / 22.77g이다. 기록 시점 변화로 본 날짜 기준 추정 섭취는 각각 3.09g와 3.33g이다. 실제 운영값은 이후 추가된 기록에 따라 달라질 수 있다.
