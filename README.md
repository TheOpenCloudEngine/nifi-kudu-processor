# NiFi Custom Kudu Processor

이 프로젝트는 NiFi에서 사용할 수 있는 Custom Kudu Processor를 구현한 NAR 프로젝트입니다.

## Build

```
# mvn clean package
```

## Deploy

```
# cp nifi-custom-kudu-nar-1.0.0.nar <NIFI_HOME>/lib
```

## Processors

## NanoTimestampSupportPutKudu

이 Processor는 PutKudu Processor가 Timestamp인 날짜를 지정할때 nanoseconds 단위의 데이터를 저장할 수 없으므로 이를 해결하기 위한 Processor이다. 
단, 날짜 컬럼은 문자열로 넘겨받아야 하며 NanoTimestampSupportPutKudu에서 nanoseconds를 지원하는 Timestamp로 변환한다.
이렇게 하는 근본적인 이유는 JDBC로 Timestamp 컬럼을 SELECT하는 경우 Timestamp 컬럼은 기본으로 millis로 처리하기 때문에 문자열로 반드시 캐스팅해야 한다.

ExecuteSQLRecord Processor에서 쿼리를 작성할 때에 다음과 같이 `c1` 컬럼이 Timestamp형인 경우 문자열로 조회한다.

```sql
select cast(timestamp1 as char) as timestamp1 from test
```

NanoTimestampSupportPutKudu Processor에서는 `timestamp1` 컬럼을 nanoseconds를 지원하도록 Timestamp로 변환하는데 이때 가장 중요한 것은 Kudu Table의 timestamp1 컬럼의 자료형이다.
반드시 이 자료형이 Timestamp가 되어야만 nanoseconds로 변환한다.

이제 Kudu에서 SQL로 test 테이블을 조회하면 다음과 같이 microseconds 및 nanoseconds를 지원하도록 Timestamp로 변환된다.

```
[sem1.io:21000] default> select * from test;
Query: select * from test
Query submitted at: 2021-12-08 16:56:04 (Coordinator: http://sem1.io:25000)
Query progress can be monitored at: http://sem1.io:25000/query_plan?query_id=c244d1b0435737e5:3a51767000000000
+-------------------------------+
| timestamp1                    |
+-------------------------------+
| 2021-11-11 02:11:11.111110000 |
+-------------------------------+
Fetched 1 row(s) in 0.13s
```