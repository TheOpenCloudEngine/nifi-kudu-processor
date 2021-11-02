# NiFi Custom Processor

이 프로젝트는 NiFi에서 사용할 수 있는 Custom Processor를 구현한 NAR 프로젝트입니다.

## Build

```
# mvn -Dmaven.test.skip=true clean package
```

## Dependency

* Maven Dependency를 추가할때에는 NiFi 자체의 구현체를 추가하지 않고 API 부분 등만 추가하도록 한다.
* 실제 구현체를 추가하는 경우 Dependency라고 할지라도 NiFi 자체 구현체까지 배포가 된다.

## Processors

### UpdateTimestampColumn

#### 용도

* Avro 등의 컬럼 메타데이터에서 Timestamp 컬럼의 값을 변경하기 위한 용도
* 날짜 포맷을 변경하거나 Timezone을 변경할 수 있음

#### 입력 파라미터

* 변환할 Timestamp 컬럼명 목록
  * column-names-values : create_time,track_out_time 
* 변환에 필요한 생성 규칙 목록 (예; ${create_time.value:format("yyyy-MM-dd HH:mm:ss", "Asia/Seoul")} )
  * column-values-string : /create_time=${create_time.value:format("yyyy-MM-dd HH:mm:ss", "Asia/Seoul")}

