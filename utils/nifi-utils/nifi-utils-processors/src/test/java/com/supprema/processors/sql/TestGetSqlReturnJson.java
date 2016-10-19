package com.supprema.processors.sql;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestGetSqlReturnJson {
	
//	final static String DB_LOCATION = "target/db";
	
//	private static final Logger LOGGER;
//    static {
//        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "info");
//        System.setProperty("org.slf4j.simpleLogger.showDateTime", "true");
//        System.setProperty("org.slf4j.simpleLogger.log.nifi.io.nio", "debug");
//        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.ExecuteSQL", "debug");
//        System.setProperty("org.slf4j.simpleLogger.log.nifi.processors.standard.TestExecuteSQL", "debug");
//        LOGGER = LoggerFactory.getLogger(TestGetSqlReturnJson.class);
//    }
	
	private TestRunner runner;
	
//	@BeforeClass
    public static void setupClass() {
        System.setProperty("derby.stream.error.file", "target/derby.log");
    }
	
//	@Before
    public void setup() throws InitializationException {
        final DBCPService dbcp = new DBCPServiceSimpleImpl();
        final Map<String, String> dbcpProperties = new HashMap<>();

        runner = TestRunners.newTestRunner(GetSqlReturnJson.class);
        runner.addControllerService("dbcp", dbcp, dbcpProperties);
        runner.enableControllerService(dbcp);
        runner.setProperty(GetSqlReturnJson.DBCP_SERVICE, "dbcp");
    }
	
//	@Test
	public void testOnTrigger() throws InitializationException {
		final TestRunner runner = TestRunners.newTestRunner(new GetSqlReturnJson());
		
		runner.setProperty("property teste", "value teste");
		runner.enqueue("ABCDEF");
		
		runner.run();
	}
	
//	@Test
    public void testIncomingConnectionWithNoFlowFileAndNoQuery() throws InitializationException {
        runner.setIncomingConnection(true);
        runner.run();
        runner.assertTransferCount(GetSqlReturnJson.REL_SUCCESS, 0);
        runner.assertTransferCount(GetSqlReturnJson.REL_FAILURE, 0);
    }
	
//	@Test
    public void testIncomingConnectionWithNoFlowFile() throws InitializationException {
        runner.setIncomingConnection(true);
        runner.setProperty(GetSqlReturnJson.SQL_SELECT_QUERY, "SELECT * FROM persons");
        runner.run();
        runner.assertTransferCount(GetSqlReturnJson.REL_SUCCESS, 0);
        runner.assertTransferCount(GetSqlReturnJson.REL_FAILURE, 0);
    }
	
	public void invokeOnTrigger(final Integer queryTimeout, final String query, final boolean incomingFlowFile, final boolean setQueryProperty) throws InitializationException, ClassNotFoundException, SQLException, IOException {

        if (queryTimeout != null) {
            runner.setProperty(GetSqlReturnJson.QUERY_TIMEOUT, queryTimeout.toString() + " secs");
        }

        // remove previous test database, if any
        final File dbLocation = new File(DB_LOCATION);
        dbLocation.delete();

        // load test data to database
        final Connection con = ((DBCPService) runner.getControllerService("dbcp")).getConnection();
//        TestJdbcHugeStream.loadTestData2Database(con, 100, 200, 100);
        LOGGER.info("test data loaded");

        // ResultSet size will be 1x200x100 = 20 000 rows
        // because of where PER.ID = ${person.id}
        final int nrOfRows = 20000;

        if (incomingFlowFile) {
            // incoming FlowFile content is not used, but attributes are used
            final Map<String, String> attributes = new HashMap<>();
            attributes.put("person.id", "10");
            if (!setQueryProperty) {
                runner.enqueue(query.getBytes(), attributes);
            } else {
                runner.enqueue("Hello".getBytes(), attributes);
            }
        }

        if(setQueryProperty) {
            runner.setProperty(GetSqlReturnJson.SQL_SELECT_QUERY, query);
        }

        runner.run();
        runner.assertAllFlowFilesTransferred(GetSqlReturnJson.REL_SUCCESS, 1);

        final List<MockFlowFile> flowfiles = runner.getFlowFilesForRelationship(GetSqlReturnJson.REL_SUCCESS);
        final InputStream in = new ByteArrayInputStream(flowfiles.get(0).toByteArray());
        
        //TODO Mostrar o json que está no conteúdo do ff
        
//        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
//        try (DataFileStream<GenericRecord> dataFileReader = new DataFileStream<>(in, datumReader)) {
//            GenericRecord record = null;
//            long recordsFromStream = 0;
//            while (dataFileReader.hasNext()) {
//                // Reuse record object by passing it to next(). This saves us from
//                // allocating and garbage collecting many objects for files with
//                // many items.
//                record = dataFileReader.next(record);
//                recordsFromStream += 1;
//            }
//
//            LOGGER.info("total nr of records from stream: " + recordsFromStream);
//            assertEquals(nrOfRows, recordsFromStream);
//        }
    }
	
	class DBCPServiceSimpleImpl extends AbstractControllerService implements DBCPService {

	    @Override
	    public String getIdentifier() {
	        return "dbcp";
	    }

        @Override
        public Connection getConnection() throws ProcessException {
            try {
                Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
                final Connection con = DriverManager.getConnection("jdbc:derby:" + DB_LOCATION + ";create=true");
                return con;
            } catch (final Exception e) {
                throw new ProcessException("getConnection failed: " + e);
            }
        }
	}
	
}