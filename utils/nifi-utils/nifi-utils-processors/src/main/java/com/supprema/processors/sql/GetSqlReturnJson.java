package com.supprema.processors.sql;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.dbcp.DBCPService;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;
import org.json.JSONException;
import org.json.JSONObject;

@EventDriven
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "sql", "select", "jdbc", "query", "database" })
@CapabilityDescription("Execute provided SQL select query. Query result will be converted to Avro format."
		+ " Streaming is used so arbitrarily large result sets are supported. This processor can be scheduled to run on "
		+ "a timer, or cron expression, using the standard scheduling methods, or it can be triggered by an incoming FlowFile. "
		+ "If it is triggered by an incoming FlowFile, then attributes of that FlowFile will be available when evaluating the "
		+ "select query. FlowFile attribute 'executesql.row.count' indicates how many rows were selected.")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class GetSqlReturnJson extends AbstractProcessor {

	public static final String ERROR_SQL = "error.message";
	public static final String ERROR_CODE = "error.code";

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("Successfully created FlowFile from SQL query result set.")
			.build();

	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure")
			.description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
			.build();

	private Set<Relationship> relationships;
	
	private List<PropertyDescriptor> propDescriptors;

	public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
			.name("Database Connection Pooling Service")
			.description("The Controller Service that is used to obtain connection to database")
			.required(true)
			.identifiesControllerService(DBCPService.class)
			.build();

	public static final PropertyDescriptor SQL_SELECT_QUERY = new PropertyDescriptor.Builder()
			.name("SQL select query")
			.description("SQL select query")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(true)
			.build();

	public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
			.name("Max Wait Time")
			.description("The maximum amount of time allowed for a running SQL select query, zero means there is no limit. Max time less than 1 second will be equal to zero.")
			.defaultValue("0 seconds")
			.required(true)
			.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
			.sensitive(false)
			.build();

	public static final PropertyDescriptor RETURN_WHEN_EMPTY = new PropertyDescriptor.Builder()
			.name("Return when empty")
			.description("Value to return when empty")
			.defaultValue("{}")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);

		final List<PropertyDescriptor> pds = new ArrayList<>();
		pds.add(DBCP_SERVICE);
		pds.add(SQL_SELECT_QUERY);
		pds.add(QUERY_TIMEOUT);
		pds.add(RETURN_WHEN_EMPTY);
		this.propDescriptors = Collections.unmodifiableList(pds);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return this.propDescriptors;
	}
	
	@OnScheduled
    public void setup(ProcessContext context) {
        if (!context.getProperty(SQL_SELECT_QUERY).isSet() && !context.hasIncomingConnection()) {
            final String errorString = "Either the Select Query must be specified or there must be an incoming connection "
                    + "providing flowfile(s) containing a SQL select query";
            getLogger().error(errorString);
            throw new ProcessException(errorString);
        }
    }

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		final List<FlowFile> originalFlowFile = session.get(1);
    	
    	FlowFile flowFile = null;
    	for (FlowFile ff : originalFlowFile) {
    		flowFile = ff;
    	}
		
    	final ComponentLog logger = getLogger();
    	
		final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
		final String selectQuery = context.getProperty(SQL_SELECT_QUERY).evaluateAttributeExpressions(flowFile).getValue();
		final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
		final String returnWhenEmpty = context.getProperty(RETURN_WHEN_EMPTY).getValue();
		
		final StopWatch stopWatch = new StopWatch(true);
		final Map<String, String> attribute = new HashMap<String, String>();
		
		Connection connection = null;
		Statement statement = null;
		try {
			connection = dbcpService.getConnection();
			
			statement = connection.createStatement();
			statement.setQueryTimeout(queryTimeout); // timeout in seconds

			attribute.put(ERROR_SQL, selectQuery);
			final ResultSet resultSet = statement.executeQuery(selectQuery);
			final ResultSetMetaData rsMeta = resultSet.getMetaData();
			final int columnCnt = rsMeta.getColumnCount();
			final List<String> columnNames = new ArrayList<String>();
			for (int i = 1; i <= columnCnt; i++) {
				columnNames.add(rsMeta.getColumnName(i));
			}
			final boolean resultSetEmpty = !resultSet.isBeforeFirst();
			if (resultSetEmpty) {
				FlowFile split = session.create(flowFile);
				split = session.write(split, new OutputStreamCallback() {
					@Override
					public void process(OutputStream out) throws IOException {
						out.write(returnWhenEmpty.getBytes(StandardCharsets.UTF_8));
					}
				});
			} else {
				while (resultSet.next()) {
					flowFile = session.write(flowFile, new OutputStreamCallback() {
						@Override
						public void process(OutputStream out) throws IOException {
							final JSONObject obj = new JSONObject();
							for (int i = 1; i <= columnCnt; i++) {
								try {
									obj.put(columnNames.get(i - 1), resultSet.getString(i));
								} catch (JSONException | SQLException e) {
									throw new ProcessException(e);
								}
							}
							out.write(obj.toString().getBytes(StandardCharsets.UTF_8));
						}
					});
				}
			}
			session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
			session.transfer(flowFile, REL_SUCCESS);
		} catch (Exception e) {
			e.printStackTrace();
			
			//Add errorMessage
			String errorMessage = attribute.get(ERROR_SQL);
			if (errorMessage != null)
				errorMessage = errorMessage.replaceAll("'", "\"");
			flowFile = session.putAttribute(flowFile, ERROR_SQL, errorMessage);
			
			//Add errorCode
			String errorCode = "";
			if (e instanceof SQLException) {
				errorCode = ((SQLException) e).getErrorCode() + "";
				logger.error(e + ", SQL ERROR CODE[" + errorCode + "]");
			}
			flowFile = session.putAttribute(flowFile, ERROR_CODE, errorCode);
			
            session.transfer(flowFile, REL_FAILURE);
		} finally {
			if (statement != null) {
        		try {
        			logger.info("Fechando o Statement");
        			statement.close();
				} catch (SQLException e) {
					e.printStackTrace();
					logger.error("SQLException ao fechar o Statement" + e.getErrorCode());
					logger.error("SQLException ao fechar o Statement" + e);
				}
        	}
        	if (connection != null) {
        		try {
        			logger.info("Fechando o Connection");
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
					logger.error("SQLException ao fechar a Connection" + e.getErrorCode());
					logger.error("SQLException ao fechar a Connection" + e);
				}
        	}
		}

	}
}