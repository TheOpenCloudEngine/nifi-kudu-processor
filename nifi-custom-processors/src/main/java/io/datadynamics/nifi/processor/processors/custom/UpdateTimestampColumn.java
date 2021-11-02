package io.datadynamics.nifi.processor.processors.custom;

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.FieldValue;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.RecordPathResult;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.*;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"update", "record", "custom", "timestamp"})
@CapabilityDescription("")
@WritesAttributes({
        @WritesAttribute(attribute = "record.error.message", description = "이 속성은 Reader 또는 Writer에서 실패시 에러 메시지를 제공합니다")
})
public class UpdateTimestampColumn extends AbstractRecordProcessor {

    private static final String FIELD_NAME = "field.name";
    private static final String FIELD_VALUE = "field.value";
    private static final String FIELD_TYPE = "field.type";

    private volatile RecordPathCache recordPathCache;

    /**
     * TODO : Processor를 제어할때 이 값이 제대로 유지되는지 중요
     */
    private volatile Map<String, String> valuesMap;

    /**
     * 문자열 기반의 Record Path 목록
     */
    private volatile List<String> recordPaths;

    static final PropertyDescriptor COLUMN_NAMES = new PropertyDescriptor.Builder()
            .name("timestamp-column-names")
            .displayName("타임스탬프 컬럼명 (comma separator)")
            .description("타임스탬프 형식을 가진 컬럼명 목록을 콤마 구분자를 갖도록 지정하십시오 (예; create_time,update_time)")
            .addValidator(new StandardValidators.StringLengthValidator(0, Integer.MAX_VALUE))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    static final PropertyDescriptor COLUMN_VALUES = new PropertyDescriptor.Builder()
            .name("timestamp-column-values-string")
            .displayName("타임스탬프 컬럼의 값")
            .description("타임스탬프 형식을 가진 컬럼의 값을 CR 구분자를 갖도록 지정하십시오")
            .addValidator(new StandardValidators.StringLengthValidator(0, Integer.MAX_VALUE))
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .required(true)
            .build();

    /**
     * 정적으로 사용자가 입력할 수 있는 속성에 대한 Property Descriptor를 정의합니다.
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>(super.getSupportedPropertyDescriptors());
        properties.add(COLUMN_NAMES);
        properties.add(COLUMN_VALUES);
        return properties;
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

    @Override
    protected Collection<ValidationResult> customValidate(final ValidationContext validationContext) {
        Map<PropertyDescriptor, String> properties = validationContext.getProperties();
        Collection<ValidationResult> results = new ArrayList<>();

        // 사용자가 입력한 값들을 정리하기 위해서 파싱해서 트림 처리도 한다.
        String valuesString = properties.get(COLUMN_VALUES);
        List<String> columnValuesArr = splitLineFeedAndTrim(valuesString);
        List<String> columnNamesArr = splitSeparatorAndTrim(properties.get(COLUMN_NAMES), ",");

        // 사용자가 입력한 컬럼 변환 값들을 컬럼명과 매핑하여 Map을 구성한다.
        if (this.valuesMap != null) {
            this.valuesMap.clear();
        } else {
            this.valuesMap = new HashMap();
        }

        columnValuesArr.stream().forEach(entry -> {
            String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(entry, "=");
            // RecordPath로 시작하는 경우
            if (strings[0].startsWith("/")) {
                String columnName = StringUtils.removeStart(strings[0], "/");
                valuesMap.put(columnName, strings[1]);
            } else {
                results.add(new ValidationResult.Builder()
                        .subject("RecordPath 에러")
                        .valid(false)
                        .explanation(String.format("RecordPath '%s'의 형식이 RecordPath가 아닙니다.", strings[0]))
                        .build());
            }
        });

        // 컬럼의 개수만큼 제대로 사용자가 입력했는지 검증한다.
        columnNamesArr.stream().forEach(entry -> {
            if (!valuesMap.containsKey(entry)) {
                results.add(new ValidationResult.Builder()
                        .subject("컬럼의 변환 규칙 누락")
                        .valid(false)
                        .explanation(String.format("컬럼 '%s'의 변환규칙이 존재하지 않습니다.", entry))
                        .build());
            }
        });

        return results;
    }

    @OnScheduled
    public void createRecordPaths(final ProcessContext context) {
        recordPathCache = new RecordPathCache(context.getProperties().size() * 2);

        final List<String> recordPaths = new ArrayList<>(context.getProperties().size() - 2);
        String columnValues = context.getProperties().get(COLUMN_VALUES);
        List<String> values = splitLineFeedAndTrim(columnValues);
        values.stream().forEach(entry -> {
            String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(entry, "=");
            // RecordPath로 시작하는 경우
            if (strings[0].startsWith("/")) {
                recordPaths.add(strings[0]);
            }
        });

        this.recordPaths = recordPaths;
    }

    @Override
    protected Record process(Record record, final FlowFile flowFile, final ProcessContext context, final long count) {
        // 사용자가 입력한 모든 RecordPath 문자열을 추출
        for (final String recordPathText : recordPaths) {
            // 스키마에서 없는 것은 Skip
            Optional<RecordField> field = record.getSchema().getField(StringUtils.removeStart(recordPathText, "/"));
            if (!field.isPresent()) {
                continue;
            }

            // RecordPath를 컴파일하고 그 결과를 반환
            final RecordPath recordPath = recordPathCache.getCompiled(recordPathText);
            final RecordPathResult result = recordPath.evaluate(record);

            Map<PropertyDescriptor, String> properties = context.getProperties();

            PropertyDescriptor pd = new PropertyDescriptor.Builder()
                    .name(recordPathText)
                    .displayName(recordPathText)
                    .description("타임스탬프 컬럼 : " + recordPathText)
                    .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
                    .build();

            if (properties.get(pd) == null) {
                // TODO : Not Supported Operation
                properties.put(pd, valuesMap.get(StringUtils.removeStart(recordPathText, "/")));
            }

            org.apache.nifi.util.MockPropertyValue pv = new org.apache.nifi.util.MockPropertyValue(
                    properties.get(pd),
                    context.getControllerServiceLookup(),
                    null, pd
            );

            if (pv.isExpressionLanguagePresent()) {
                final Map<String, String> fieldVariables = new HashMap<>();

                result.getSelectedFields().forEach(fieldVal -> {
                    fieldVariables.clear();
                    fieldVariables.put(FIELD_NAME, fieldVal.getField().getFieldName());
                    fieldVariables.put(FIELD_VALUE, DataTypeUtils.toString(fieldVal.getValue(), (String) null));
                    fieldVariables.put(FIELD_TYPE, fieldVal.getField().getDataType().getFieldType().name());

                    final String evaluatedReplacementVal = pv.evaluateAttributeExpressions(flowFile, fieldVariables).getValue();
                    fieldVal.updateValue(evaluatedReplacementVal, RecordFieldType.STRING.getDataType());
                });
            } else {
                final String evaluatedReplacementVal = pv.evaluateAttributeExpressions(flowFile).getValue();
                result.getSelectedFields().forEach(fieldVal -> fieldVal.updateValue(evaluatedReplacementVal, RecordFieldType.STRING.getDataType()));
            }
        }
        record.incorporateInactiveFields();
        return record;
    }

    private Record processAbsolutePath(final RecordPath replacementRecordPath, final Stream<FieldValue> destinationFields, final Record record) {
        final RecordPathResult replacementResult = replacementRecordPath.evaluate(record);
        final List<FieldValue> selectedFields = replacementResult.getSelectedFields().collect(Collectors.toList());
        final List<FieldValue> destinationFieldValues = destinationFields.collect(Collectors.toList());

        return updateRecord(destinationFieldValues, selectedFields, record);
    }

    private Record processRelativePath(final RecordPath replacementRecordPath, final Stream<FieldValue> destinationFields, Record record) {
        final List<FieldValue> destinationFieldValues = destinationFields.collect(Collectors.toList());

        for (final FieldValue fieldVal : destinationFieldValues) {
            final RecordPathResult replacementResult = replacementRecordPath.evaluate(record, fieldVal);
            final List<FieldValue> selectedFields = replacementResult.getSelectedFields().collect(Collectors.toList());
            final Object replacementObject = getReplacementObject(selectedFields);
            updateFieldValue(fieldVal, replacementObject);
        }

        return record;
    }

    private Record updateRecord(final List<FieldValue> destinationFields, final List<FieldValue> selectedFields, final Record record) {
        if (destinationFields.size() == 1 && !destinationFields.get(0).getParentRecord().isPresent()) {
            final Object replacement = getReplacementObject(selectedFields);
            if (replacement == null) {
                return record;
            }
            if (replacement instanceof Record) {
                return (Record) replacement;
            }

            final FieldValue replacementFieldValue = (FieldValue) replacement;
            if (replacementFieldValue.getValue() instanceof Record) {
                return (Record) replacementFieldValue.getValue();
            }

            final List<RecordField> fields = selectedFields.stream().map(FieldValue::getField).collect(Collectors.toList());
            final RecordSchema schema = new SimpleRecordSchema(fields);
            final Record mapRecord = new MapRecord(schema, new HashMap<>());
            for (final FieldValue selectedField : selectedFields) {
                mapRecord.setValue(selectedField.getField(), selectedField.getValue());
            }

            return mapRecord;
        } else {
            for (final FieldValue fieldVal : destinationFields) {
                final Object replacementObject = getReplacementObject(selectedFields);
                updateFieldValue(fieldVal, replacementObject);
            }
            return record;
        }
    }

    private void updateFieldValue(final FieldValue fieldValue, final Object replacement) {
        if (replacement instanceof FieldValue) {
            final FieldValue replacementFieldValue = (FieldValue) replacement;
            fieldValue.updateValue(replacementFieldValue.getValue(), replacementFieldValue.getField().getDataType());
        } else {
            fieldValue.updateValue(replacement);
        }
    }

    private Object getReplacementObject(final List<FieldValue> selectedFields) {
        if (selectedFields.size() > 1) {
            final List<RecordField> fields = selectedFields.stream().map(FieldValue::getField).collect(Collectors.toList());
            final RecordSchema schema = new SimpleRecordSchema(fields);
            final Record record = new MapRecord(schema, new HashMap<>());
            for (final FieldValue fieldVal : selectedFields) {
                record.setValue(fieldVal.getField(), fieldVal.getValue());
            }

            return record;
        }

        if (selectedFields.isEmpty()) {
            return null;
        } else {
            return selectedFields.get(0);
        }
    }
}
