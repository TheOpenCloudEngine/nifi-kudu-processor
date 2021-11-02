package io.datadynamics.nifi.processor.processors.custom;

import org.apache.commons.lang3.SystemUtils;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.serialization.record.MockRecordParser;
import org.apache.nifi.serialization.record.MockRecordWriter;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestUpdateTimestampColumn {

    private TestRunner runner;
    private MockRecordParser readerService;
    private MockRecordWriter writerService;

    @BeforeClass
    public static void setUpSuite() {
        Assume.assumeTrue("Test only runs on *nix", !SystemUtils.IS_OS_WINDOWS);
    }

    @Before
    public void setup() throws InitializationException {
        // 테스트를 위해서 Reader Reader와 Record Writer를 구성한다.

        readerService = new MockRecordParser();
        writerService = new MockRecordWriter("header", false);

        runner = TestRunners.newTestRunner(UpdateTimestampColumn.class);
        runner.addControllerService("reader", readerService);
        runner.enableControllerService(readerService);
        runner.addControllerService("writer", writerService);
        runner.enableControllerService(writerService);

        runner.setProperty(UpdateTimestampColumn.RECORD_READER, "reader");
        runner.setProperty(UpdateTimestampColumn.RECORD_WRITER, "writer");

        // Reader에 스키마를 지정한다.
        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);
        readerService.addSchemaField("create_time", RecordFieldType.TIMESTAMP);
        readerService.addSchemaField("track_out_time", RecordFieldType.TIMESTAMP);
    }


    @Test
    public void testTimestamp() {
        // 속성을 설정
        runner.setProperty("timestamp-column-names", "create_time,track_out_time");
        runner.setProperty("timestamp-column-values-string", "/create_time=${field.value:format(\"yyyy-MM-dd HH:mm:ss\", \"America/Los_Angeles\")}\n" +
                "/track_out_time=${field.value:format(\"yyyy-MM-dd HH:mm:ss\", \"Asia/Seoul\")}\n");

        runner.enqueue("");

        // 레코드 추가
        readerService.addRecord("Jane Doe", 35, "1632786464214", "1635786464214");
        runner.run();

        // 성공 확인
        runner.assertAllFlowFilesTransferred(UpdateTimestampColumn.REL_SUCCESS, 1);

        // FlowFile 확인
        final MockFlowFile out = runner.getFlowFilesForRelationship(UpdateTimestampColumn.REL_SUCCESS).get(0);
        out.assertContentEquals("header\nJane Doe,35,2021-09-27 16:47:44,2021-11-02 02:07:44\n");
    }
}