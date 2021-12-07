# NiFi Custom Processor

이 프로젝트는 NiFi에서 사용할 수 있는 Custom Processor를 구현한 NAR 프로젝트입니다.

## Build

```
# mvn -Dmaven.buildNumber.revisionOnScmFailure=no-scm -Dmaven.buildNumber.skip=true -Dmaven.test.skip=true clean package
```

## Deploy

### Deploy NAR

```
# cd nifi-custom-nar
# mvn antrun:run@scp
```

### NiFi Restart

```
# cd nifi-custom-nar
# mvn antrun:run@restart
```

## Dependency

* Maven Dependency를 추가할때에는 NiFi 자체의 구현체를 추가하지 않고 API 부분 등만 추가하도록 한다.
* 실제 구현체를 추가하는 경우 Dependency라고 할지라도 NiFi 자체 구현체까지 배포가 된다.

## ETC

```bash
#!/bin/sh

FILES=`ls -lsa $1/*.jar | awk '{print $10}'`
for FILE in $FILES
do
	echo $FILE
	jar tvf $FILE | grep $2	
done
```
