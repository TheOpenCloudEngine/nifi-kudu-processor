package io.datadynamics.nifi.processor.processors.custom;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.status.ProcessGroupStatus;
import org.apache.nifi.controller.status.ProcessorStatus;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.EventAccess;

import java.util.*;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"log", "monitoring", "custom"})
@WritesAttributes({
        @WritesAttribute(attribute = UpdateLogProcessor.STATUS_CODE, description = "처리 상태 코드 (예; 성공, 실패)"),
        @WritesAttribute(attribute = UpdateLogProcessor.STATUS_MESSAGE, description = "처리 상태 메시지 (예; 파일을 로딩할 수 없습니다)"),
})
public class UpdateLogProcessor extends AbstractProcessor {

    public final static String INSERT_QUERY = "INSERT INTO nifi_log (id, flow_name, ) VALUES(); ";

    public final static String STATUS_CODE = "updatelog.status.code";
    public final static String LOG_NAME = "updatelog.name";
    public final static String STATUS_MESSAGE = "updatelog.status.message";
    public final static String ATTRIBUTE_NAMES = "updatelog.attribute.names";

    public static final PropertyDescriptor attributeNamesPropertyDescriptor = new PropertyDescriptor.Builder()
            .name(ATTRIBUTE_NAMES)
            .displayName("처리 상태 코드")
            .description("처리 상태 코드 (예; 성공, 실패)")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor namePropertyDescriptor = new PropertyDescriptor.Builder()
            .name(LOG_NAME)
            .displayName("로그명")
            .description("로그명을 입력해주십시오(예; Oracle DB에 접속).")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor statusCodePropertyDescriptor = new PropertyDescriptor.Builder()
            .name(STATUS_CODE)
            .displayName("처리 상태 메시지")
            .description("처리 상태 메시지를 입력해주십시오(예; 파일을 로딩할 수 없습니다).")
            .required(true)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor statusMessagePropertyDescriptor = new PropertyDescriptor.Builder()
            .name(STATUS_MESSAGE)
            .displayName("기록에 남길 FlowFile Attribute명 목록(Comma separated)")
            .description("로그로 기록에 남길 속성명을 comma separated 형식으로 입력해 주십시오.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("성공적으로 기록에 남긴 상태")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("기록을 남기다가 실패한 상태")
            .build();

    // Database
    public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
            .name("데이터베이스 커넥션 풀 서비스")
            .description("데이터베이스에 연결하기 위한 Controller Service")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("최대 대기 시간")
            .description("로그 정보를 DB에 기록하기 위해서 쿼리 실행시 기다릴 수 있는 최대 시간. 0은 제한이 없음을 의미하며, 1보다 작으면 0으로 간주")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .sensitive(false)
            .build();

    public static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Collections.unmodifiableList(Arrays.asList(
            attributeNamesPropertyDescriptor,
            namePropertyDescriptor,
            statusCodePropertyDescriptor,
            statusMessagePropertyDescriptor
    ));

    protected DBCPService dbcpService;

    @Override
    public Set<Relationship> getRelationships() {
        Set<Relationship> RELATIONS = new HashSet<>();
        RELATIONS.add(REL_SUCCESS);
        RELATIONS.add(REL_FAILURE);
        return RELATIONS;
    }

    @Override
    protected List<org.apache.nifi.components.PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();

        if (dbcpService == null) {
            dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        }

        // Incoming Flow File이 없다면 무시한다.
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String identifier = this.getIdentifier();
        String processorName = context.getName();

        Map<String, String> allProperties = context.getAllProperties();

        long flowFileId = flowFile.getId();
        long entryDate = flowFile.getEntryDate();
        long size = flowFile.getSize();

        // Property Descriptor를 획득한다.
        Map<PropertyDescriptor, String> properties = context.getProperties();


        session.transfer(flowFile, REL_FAILURE);
    }

    private List<String> splitLineFeedAndTrim(String valueString) {
        String values[] = valueString.split("\\r?\\n");
        List<String> arr = new ArrayList();
        for (String value : values) {
            arr.add(value.trim());
        }
        return arr;
    }

    private List<String> splitSeparatorAndTrim(String valueString, String separator) {
        String values[] = StringUtils.splitByWholeSeparatorPreserveAllTokens(valueString, separator);
        List<String> arr = new ArrayList();
        for (String value : values) {
            arr.add(value.trim());
        }
        return arr;
    }
}
