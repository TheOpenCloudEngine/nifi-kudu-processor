# NiFi Custom Processor

이 프로젝트는 NiFi에서 사용할 수 있는 Custom Processor를 구현한 NAR 프로젝트입니다.

## Build

```
# mvn -Dmaven.test.skip=true clean package
```

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

