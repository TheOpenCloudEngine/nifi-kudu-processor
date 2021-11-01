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

import java.util.Collections;

public class TestUpdateTimestampColumn {

    private TestRunner runner;
    private MockRecordParser readerService;
    private MockRecordWriter writerService;

    //Apparently pretty printing is not portable as these tests fail on windows
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

        readerService.addSchemaField("name", RecordFieldType.STRING);
        readerService.addSchemaField("age", RecordFieldType.INT);
        readerService.addSchemaField("create_time", RecordFieldType.TIMESTAMP);
    }

    @Test
    public void testLiteralReplacementValue() {
        runner.setProperty("/name", "Jane Doe");
        runner.enqueue("");

        readerService.addRecord("John Doe", 35);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateTimestampColumn.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(UpdateTimestampColumn.REL_SUCCESS).get(0);
        out.assertContentEquals("header\nJane Doe,35\n");
    }

    @Test
    public void testLiteralReplacementValueExpressionLanguage() {
        runner.setProperty("/name", "${newName}");
        runner.enqueue("", Collections.singletonMap("newName", "Jane Doe"));

        readerService.addRecord("John Doe", 35);
        runner.run();

        runner.assertAllFlowFilesTransferred(UpdateTimestampColumn.REL_SUCCESS, 1);
        final MockFlowFile out = runner.getFlowFilesForRelationship(UpdateTimestampColumn.REL_SUCCESS).get(0);
        out.assertContentEquals("header\nJane Doe,35\n");
    }

}