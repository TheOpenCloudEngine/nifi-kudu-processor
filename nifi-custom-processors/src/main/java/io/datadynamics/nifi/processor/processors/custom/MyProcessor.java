package io.datadynamics.nifi.processor.processors.custom;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.expression.AttributeExpression;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_ALLOWED)
@Tags({"log", "monitoring", "custom"})
@WritesAttributes({
        @WritesAttribute(attribute = MyProcessor.STATUS_CODE, description = "처리 상태 코드 (예; 성공, 실패)"),
        @WritesAttribute(attribute = MyProcessor.STATUS_MESSAGE, description = "처리 상태 메시지 (예; 파일을 로딩할 수 없습니다)"),
})
@DynamicProperty(name = "Generated FlowFile attribute name", value = "Generated FlowFile attribute value",
        supportsExpressionLanguage = true,
        description = "Specifies an attribute on generated FlowFiles defined by the Dynamic Property's key and value." +
                " If Expression Language is used, evaluation will be performed only once per batch of generated FlowFiles.")
public class MyProcessor extends AbstractProcessor {

    public final static String INSERT_QUERY = "INSERT INTO nifi_log (unique_id, process_group_name, process_group_id, processor_name, processor_id, log_order, name, status, message, custom) VALUES(%s, '%s', '%s', '%s', '%s', %s, '%s', '%s', '%s', '%s'); ";

    public final static String STATUS_CODE = "updatelog.status.code";
    public final static String LOG_NAME = "updatelog.name";
    public final static String STATUS_MESSAGE = "updatelog.status.message";
    public final static String ATTRIBUTE_NAMES = "updatelog.attribute.names";
    public final static String DB_POOL = "updatelog.database.pool";
    public final static String DB_QUERY_TIMEOUT = "updatelog.database.query.timeout";

    public static final PropertyDescriptor attributeNamesPropertyDescriptor = new PropertyDescriptor.Builder()
            .name(STATUS_MESSAGE)
            .displayName("처리 상태 코드")
            .description("처리 상태 코드 (예; 성공, 실패)")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor namePropertyDescriptor = new PropertyDescriptor.Builder()
            .name(LOG_NAME)
            .displayName("로그명")
            .description("로그명을 입력해주십시오(예; Oracle DB에 접속).")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor statusCodePropertyDescriptor = new PropertyDescriptor.Builder()
            .name(STATUS_CODE)
            .displayName("처리 상태 메시지")
            .description("처리 상태 메시지를 입력해주십시오(예; 파일을 로딩할 수 없습니다).")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    public static final PropertyDescriptor statusMessagePropertyDescriptor = new PropertyDescriptor.Builder()
            .name(ATTRIBUTE_NAMES)
            .displayName("기록에 남길 FlowFile Attribute명 목록(Comma separated)")
            .description("로그로 기록에 남길 속성명을 comma separated 형식으로 입력해 주십시오.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
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
            .name(DB_POOL)
            .displayName("데이터베이스 커넥션 풀 서비스")
            .description("데이터베이스에 연결하기 위한 Controller Service")
            .required(true)
            .identifiesControllerService(DBCPService.class)
            .build();

    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name(DB_QUERY_TIMEOUT)
            .displayName("최대 대기 시간")
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
            statusMessagePropertyDescriptor,
            DBCP_SERVICE,
            QUERY_TIMEOUT
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
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .required(false)
                .addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING, true))
                .addValidator(StandardValidators.ATTRIBUTE_KEY_PROPERTY_NAME_VALIDATOR)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    @Override
    protected List<org.apache.nifi.components.PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @OnScheduled
    public void setup(ProcessContext context) {
        final ComponentLog logger = getLogger();
        logger.info("MyProcessor : setup");
        dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
    }

    @Override
    protected Collection<ValidationResult> customValidate(ValidationContext validationContext) {
        final ComponentLog logger = getLogger();
        Map<PropertyDescriptor, String> properties = validationContext.getProperties();
        Collection<ValidationResult> results = new ArrayList<>();
/*
new ValidationResult.Builder()
.subject("컬럼의 변환 규칙 누락")
.valid(false)
.explanation(String.format("컬럼 '%s'의 변환규칙이 존재하지 않습니다.", entry))
.build()
 */
        return results;
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();

        if (dbcpService == null) {
            dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        }

        // Incoming Flow File이 없다면 무시한다.
        FlowFile flowFile = session.get();

        // Property Descriptor를 획득한다.
        Map<PropertyDescriptor, String> processorProperties = context.getProperties();
        Map<String, String> generatedAttributes = new HashMap<String, String>();
        for (final Map.Entry<PropertyDescriptor, String> entry : processorProperties.entrySet()) {
            PropertyDescriptor property = entry.getKey();
            if (property.isDynamic() && property.isExpressionLanguageSupported()) {
                String dynamicValue = context.getProperty(property).evaluateAttributeExpressions().getValue();
                generatedAttributes.put(property.getName(), dynamicValue);
            }
        }

        Map<String, String> allProperties = context.getAllProperties();
        Map<String, String> merged = new HashMap(allProperties);
        merged.putAll(generatedAttributes);

        logger.info("ALL : " + allProperties);
        logger.info("GENERATE : " + generatedAttributes);

        String query = String.format(INSERT_QUERY,
                merged.get("uniqueId"),
                "",
                getIdentifier(),
                context.getName(),
                "",
                getOrder(merged),
                merged.get(LOG_NAME),
                merged.get(STATUS_CODE),
                merged.get(STATUS_MESSAGE),
                ""
        );
        logger.info(query);

        session.putAllAttributes(flowFile, merged);

        Connection connection = null;
        Statement statement = null;
        try {
            connection = dbcpService.getConnection();
            boolean originalAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(true);
            statement = connection.createStatement();
            statement.executeUpdate(query);
            connection.setAutoCommit(originalAutoCommit);
            statement.close();
            connection.close();

            session.getProvenanceReporter().modifyAttributes(flowFile);

            session.transfer(flowFile, REL_SUCCESS);
        } catch (Exception e) {
            logger.warn("에러 발생", e);
            if (connection != null) {
                try {
                    connection.rollback();
                } catch (SQLException ex) {
                }
            }

            session.transfer(flowFile, REL_FAILURE);
        } finally {
            if (statement != null) try {statement.close();} catch (SQLException ex) {}
            if (connection != null) try {connection.close();} catch (SQLException ex) {}
        }
    }

    private int getOrder(Map<String, String> generatedAttributes) {
        try {
            String logOrder = generatedAttributes.get("logOrder");
            return Integer.parseInt(logOrder);
        } catch (Exception e) {
            return 1;
        }
    }
}
