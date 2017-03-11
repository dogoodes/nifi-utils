package com.nifi.processors.db;

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
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

import java.io.*;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Tags({ "wave insert sql transaction tx" })
@CapabilityDescription("Execute multiple insert in a transactional context")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class MultiInsert extends AbstractProcessor {
	public static final String RESULT = "insert.sucessfully.done";
	public static final String SQL_CODE = "sql.error.code";
	public static final String ERROR_STMT = "error.message";
	
	private static final String PROPERTY = "Property" ;
	private static final String CONTENT = "Content";

	public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder()
			.name("Database Connection Pooling Service")
			.description("The Controller Service that is used to obtain connection to database")
			.required(true)
			.identifiesControllerService(DBCPService.class)
			.build();

	public static final PropertyDescriptor INSERT1 = new PropertyDescriptor.Builder()
			.name("statement1")
			.description("SQL insert statement: check default value")
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(true)
			.build();

	public static final PropertyDescriptor INSERT2 = new PropertyDescriptor.Builder()
			.name("statement2")
			.description("SQL insert statement: check default value")
			.required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(true)
			.build();

	public static final PropertyDescriptor INSERT3 = new PropertyDescriptor.Builder()
			.name("statement3")
			.description("SQL insert statement: check default value")
			.required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(true)
			.build();

	public static final PropertyDescriptor INSERT4 = new PropertyDescriptor.Builder()
			.name("statement4")
			.description("SQL insert statement: check default value")
			.required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(true)
			.build();

	public static final PropertyDescriptor INSERT5 = new PropertyDescriptor.Builder()
			.name("statement5")
			.description("SQL insert statement: check default value")
			.required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(true)
			.build();

	public static final PropertyDescriptor INSERT6 = new PropertyDescriptor.Builder()
			.name("statement6")
			.description("SQL insert statement: check default value")
			.required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(true)
			.build();

	public static final PropertyDescriptor INSERT7 = new PropertyDescriptor.Builder()
			.name("statement7")
			.description("SQL insert statement: check default value")
			.required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(true)
			.build();

	public static final PropertyDescriptor INSERT8 = new PropertyDescriptor.Builder()
			.name("statement8")
			.description("SQL insert statement: check default value")
			.required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(true)
			.build();

	public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
			.name("Maximum Buffer Size")
			.description("Specifies the maximum amount of data to buffer (per file) in order to apply the regular expressions.  Files larger than the specified maximum will not be fully evaluated.")
			.required(true)
			.addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
			.defaultValue("1 MB")
			.build();
	
	public static final PropertyDescriptor TYPE_INSERT = new PropertyDescriptor.Builder()
			.name("Type insert")
			.description("Type insert...")
			.required(true)
			.allowableValues(PROPERTY, CONTENT)
            .defaultValue(PROPERTY)
            .build();

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("Successfully created FlowFile from SQL query result set.")
			.build();
	
	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure")
			.description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship")
			.build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(DBCP_SERVICE);
		descriptors.add(TYPE_INSERT);
		descriptors.add(INSERT1);
		descriptors.add(INSERT2);
		descriptors.add(INSERT3);
		descriptors.add(INSERT4);
		descriptors.add(INSERT5);
		descriptors.add(INSERT6);
		descriptors.add(INSERT7);
		descriptors.add(INSERT8);
		descriptors.add(MAX_BUFFER_SIZE);
		this.descriptors = Collections.unmodifiableList(descriptors);

		final Set<Relationship> relationships = new HashSet<Relationship>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	@Override
	public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return descriptors;
	}

	@OnScheduled
	public void onScheduled(final ProcessContext context) {

	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		final ComponentLog logger = getLogger();
		final List<FlowFile> incomingFlowfiles = session.get(1);
		final Map<String, String> result = new HashMap<String, String>();

		for (FlowFile fileToProcess : incomingFlowfiles) {

			final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
			final String stm1 = context.getProperty(INSERT1).evaluateAttributeExpressions(fileToProcess).getValue();
			final String stm2 = context.getProperty(INSERT2).evaluateAttributeExpressions(fileToProcess).getValue();
			final String stm3 = context.getProperty(INSERT3).evaluateAttributeExpressions(fileToProcess).getValue();
			final String stm4 = context.getProperty(INSERT4).evaluateAttributeExpressions(fileToProcess).getValue();
			final String stm5 = context.getProperty(INSERT5).evaluateAttributeExpressions(fileToProcess).getValue();
			final String stm6 = context.getProperty(INSERT6).evaluateAttributeExpressions(fileToProcess).getValue();
			final String stm7 = context.getProperty(INSERT7).evaluateAttributeExpressions(fileToProcess).getValue();
			final String stm8 = context.getProperty(INSERT8).evaluateAttributeExpressions(fileToProcess).getValue();
			
			final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
			final StopWatch stopWatch = new StopWatch(true);
			
			final String typeInsert = context.getProperty(TYPE_INSERT).evaluateAttributeExpressions(fileToProcess).getValue();
	    	final List<String> lines = new ArrayList<>();

			final byte[] buffer = new byte[maxBufferSize];
			
			if (typeInsert.equals(PROPERTY)) {
				final Connection con = dbcpService.getConnection();
				try {
					final boolean originalAutoCommit = con.getAutoCommit();
					con.setAutoCommit(false);
					
					if (fileToProcess == null) {
						fileToProcess = session.create();
					}
					
					final AtomicInteger bufferedByteCount = new AtomicInteger(0);
					session.read(fileToProcess, new InputStreamCallback() {
						@Override
						public void process(final InputStream in) throws IOException {
							bufferedByteCount.set(StreamUtils.fillBuffer(in, buffer, false));
						}
					});
					
					final String contentString = new String(buffer, 0, bufferedByteCount.get(), "UTF-8");
					
					fileToProcess = session.write(fileToProcess, new OutputStreamCallback() {
						@Override
						public void process(final OutputStream out) throws IOException {
							PreparedStatement preparedStatementInsert1 = null, preparedStatementInsert2 = null,
									preparedStatementInsert3 = null, preparedStatementInsert4 = null, preparedStatementInsert5 = null
											, preparedStatementInsert6 = null, preparedStatementInsert7 = null, preparedStatementInsert8 = null;
							try {
								
								try {
									result.put(ERROR_STMT, stm1);
									logger.debug("MultiInsert performing " + stm1);
									preparedStatementInsert1 = con.prepareStatement(stm1);
									preparedStatementInsert2 =prepareIfIsValid(stm2,result,con);
									preparedStatementInsert3 =prepareIfIsValid(stm3,result,con);
									preparedStatementInsert4 =prepareIfIsValid(stm4,result,con);
									preparedStatementInsert5 =prepareIfIsValid(stm5,result,con);
									preparedStatementInsert6 =prepareIfIsValid(stm6,result,con);
									preparedStatementInsert7 =prepareIfIsValid(stm7,result,con);
									preparedStatementInsert8 =prepareIfIsValid(stm8,result,con);
									
								} catch (SQLException e) {
									e.printStackTrace();
									logger.error(" SQLException building " + result.get(ERROR_STMT));
									logger.error(" SQLException during prepareStatement " + e);
									throw new ProcessException(e);
								}
								int id = 1;
								result.put(ERROR_STMT, stm1);
								preparedStatementInsert1.executeUpdate();

								id = executeIfIsValid(stm2,result,preparedStatementInsert2,id);
								id = executeIfIsValid(stm3,result,preparedStatementInsert3,id);
								id = executeIfIsValid(stm4,result,preparedStatementInsert4,id);
								id = executeIfIsValid(stm5,result,preparedStatementInsert5,id);
								id = executeIfIsValid(stm6,result,preparedStatementInsert6,id);
								id = executeIfIsValid(stm7,result,preparedStatementInsert7,id);
								id = executeIfIsValid(stm8,result,preparedStatementInsert8,id);
								
								result.put(RESULT, id + "");
								
								con.commit();
							} catch (final SQLException e) {
								int error_code = e.getErrorCode();
								
								result.put(SQL_CODE, error_code+"");
								try {
									logger.error(" Exception performing " + result.get(ERROR_STMT));
									logger.error(" Exception error code [" + error_code + "]during executeUpdate " + e);
									con.rollback();
								} catch (SQLException e1) {
									e1.printStackTrace();
								}
								logger.error(" " + e);
								logger.error(" " + e.getMessage());
								throw new ProcessException(e);
							} finally {
								logger.debug("EXECUTO finally ");
								closePreparedStatement(preparedStatementInsert1);
								closePreparedStatement(preparedStatementInsert2);
								closePreparedStatement(preparedStatementInsert3);
								closePreparedStatement(preparedStatementInsert4);
								closePreparedStatement(preparedStatementInsert5);
								closePreparedStatement(preparedStatementInsert6);
								closePreparedStatement(preparedStatementInsert7);
								closePreparedStatement(preparedStatementInsert8);
								closeConnection(con, originalAutoCommit);
								out.write(contentString.getBytes("UTF-8"));
							}
						}
						
						private int executeIfIsValid(String stm, Map<String, String> result, PreparedStatement preparedStatementInsert, int id) throws SQLException {
							if (isAValidSQL(stm)) {
								result.put(ERROR_STMT, stm);
								preparedStatementInsert.executeUpdate();
								id++;
							}
							return id;
						}

						private PreparedStatement prepareIfIsValid(String stm, Map<String, String> result, Connection con) throws SQLException {
							PreparedStatement preparedStatementInsert = null;
							if (isAValidSQL(stm)) {
								result.put(ERROR_STMT, stm);
								preparedStatementInsert = con.prepareStatement(stm);
								logger.debug("MultiInsert preparing " + stm);
							}
							return preparedStatementInsert;
						}

						private boolean isAValidSQL(String stmString) {
							return (stmString != null && stmString.trim().length() > 0);
						}
					});
					
					fileToProcess = session.putAttribute(fileToProcess, RESULT, result.get(RESULT));
					
					session.getProvenanceReporter().modifyContent(fileToProcess, "Performing some inserts",	stopWatch.getElapsed(TimeUnit.MILLISECONDS));
					
					session.transfer(fileToProcess, REL_SUCCESS);
				} catch (final Throwable e) {
					logger.error(e + "");
					e.printStackTrace();

					String sqlErrorCode = "";
					if(result.get(SQL_CODE) != null && result.get(SQL_CODE).trim().length()>0){
						sqlErrorCode = "SqlErrCod["+result.get(SQL_CODE)+"] ";
						fileToProcess = session.putAttribute(fileToProcess, SQL_CODE, sqlErrorCode);
					}	
					
					String errorMessage = result.get(ERROR_STMT);
					if (errorMessage != null)
						errorMessage = errorMessage.replaceAll("'", "\"");
					fileToProcess = session.putAttribute(fileToProcess, ERROR_STMT, errorMessage);
					session.transfer(fileToProcess, REL_FAILURE);
					
				}
			} else if (typeInsert.equals(CONTENT)) {
				final Connection con = dbcpService.getConnection();
				try {
					final boolean originalAutoCommit = con.getAutoCommit();
					con.setAutoCommit(false);
					
					if (fileToProcess == null) {
						fileToProcess = session.create();
					}
					
					final AtomicInteger bufferedByteCount = new AtomicInteger(0);
					session.read(fileToProcess, new InputStreamCallback() {
						@Override
						public void process(final InputStream in) throws IOException {
							final BufferedReader br = new BufferedReader(new InputStreamReader(in));
							String line = null;
							while ((line = br.readLine()) != null) {
								if (!line.trim().equals("")) {
									lines.add(line.trim());
								}
							}
							bufferedByteCount.set(StreamUtils.fillBuffer(in, buffer, false));
						}
					});
					
					final String contentString = new String(buffer, 0, bufferedByteCount.get(), "UTF-8");
					
					fileToProcess = session.write(fileToProcess, new OutputStreamCallback() {
						@Override
						public void process(final OutputStream out) throws IOException {
							PreparedStatement preparedStatementInsert = null;
							
							try {
								int id = 1;
								for (String line : lines) {
									if (isAValidSQL(line)) {
										result.put(ERROR_STMT, line);
										preparedStatementInsert = con.prepareStatement(line);
										preparedStatementInsert.executeUpdate();
										id++;
									}
								}							
								result.put(RESULT, id + "");
								
								con.commit();
							} catch (final SQLException e) {
								int error_code = e.getErrorCode();
								result.put(SQL_CODE, error_code+"");
								
								try {
									logger.error(" Exception performing " + result.get(ERROR_STMT));
									logger.error(" Exception error code [" + error_code + "]during executeUpdate " + e);
									con.rollback();
								} catch (SQLException e1) {
									e1.printStackTrace();
								}
								logger.error(" " + e);
								logger.error(" " + e.getMessage());
								throw new ProcessException(e);
							} finally {
								closePreparedStatement(preparedStatementInsert);
								closeConnection(con, originalAutoCommit);
								out.write(contentString.getBytes("UTF-8"));
							}
						}
						
						private boolean isAValidSQL(String stmString) {
							return (stmString != null && stmString.trim().length() > 0);
						}
					});

					fileToProcess = session.putAttribute(fileToProcess, RESULT, result.get(RESULT));
					
					session.getProvenanceReporter().modifyContent(fileToProcess, "Performing some inserts",	stopWatch.getElapsed(TimeUnit.MILLISECONDS));
					
					session.transfer(fileToProcess, REL_SUCCESS);
				} catch (final Throwable e) {
					logger.error(e + "");
					e.printStackTrace();
					
					String sqlErrorCode = "";
					if(result.get(SQL_CODE) != null && result.get(SQL_CODE).trim().length()>0){
						sqlErrorCode = "SqlErrCod["+result.get(SQL_CODE)+"] ";
						fileToProcess = session.putAttribute(fileToProcess, SQL_CODE, sqlErrorCode);
					}
					
					String errorMessage = result.get(ERROR_STMT);
					if (errorMessage != null)
						errorMessage = errorMessage.replaceAll("'", "\"");
					fileToProcess = session.putAttribute(fileToProcess, ERROR_STMT, sqlErrorCode + errorMessage);
					session.transfer(fileToProcess, REL_FAILURE);
				}
			}
		}
	}

	private void closePreparedStatement(PreparedStatement preparedStatement) {
		if (preparedStatement != null) {
			try {
				preparedStatement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void closeConnection(Connection con, boolean originalAutoCommit) {
		if (con != null) {
			try {
				con.setAutoCommit(originalAutoCommit);
				con.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
