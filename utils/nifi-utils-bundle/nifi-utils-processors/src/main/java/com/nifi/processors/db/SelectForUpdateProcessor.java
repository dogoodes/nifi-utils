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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Tags({ "wave select update sql transaction tx" })
@CapabilityDescription("Execute a select for update statement, then perform a update (or a insert in case of no result "
		+ "for the previous select) and set the new id in attribute next.id of the returned flowfile")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class SelectForUpdateProcessor extends AbstractProcessor {

	public static final String SQL_CODE = "sql.error.code";
	public static final String ERROR_STMT = "error.message";
	private static final String SET_LOCK_STMT = "SET LOCK MODE TO WAIT ";

	public static final PropertyDescriptor DBCP_SERVICE = new PropertyDescriptor.Builder().name("Database Connection Pooling Service")
			.description("The Controller Service that is used to obtain connection to database").required(true)
			.identifiesControllerService(DBCPService.class).build();

	public static final PropertyDescriptor SQL_SELECT = new PropertyDescriptor.Builder().name("SQL select query")
			.description(
					"SQL select query: check default value, DO NOT USE \"FOR UPDATE\" CLAUSE IN STATEMENT, will concatenate automatically!! ")
			.required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.defaultValue("SELECT (s.contador+1) FROM sequencia_nifi s WHERE s.loja = '${json_id_seq}' AND integracao = '${tabela_origem}'")
			.expressionLanguageSupported(true).build();

	public static final PropertyDescriptor SQL_UPDATE = new PropertyDescriptor.Builder().name("SQL update query")
			.description("SQL update statement: check default value").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.defaultValue("UPDATE sequencia_nifi SET contador = contador+1 WHERE integracao = '${tabela_origem}' and loja = '${json_id_seq}'")
			.expressionLanguageSupported(true).build();

	public static final PropertyDescriptor SQL_INSERT = new PropertyDescriptor.Builder().name("SQL insert query")
			.description("SQL insert statement: check default value, first ID allowed MUST BE 1 !").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.defaultValue("INSERT INTO sequencia_nifi (integracao,loja, contador) VALUES ('${tabela_origem}','${json_id_seq}',1)")
			.expressionLanguageSupported(true).build();

	public static final PropertyDescriptor SQL_LOCK_TO = new PropertyDescriptor.Builder().name("Lock Timeout")
			.description("set the lock timeout for this context").required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).defaultValue("15")
			.expressionLanguageSupported(true).build();

	public static final PropertyDescriptor NEXT_ID_ATTRIBUTE_NAME = new PropertyDescriptor.Builder().name("Next id attribute name")
			.description("the name of the attribute holding the generated id to set on the flowfile ").required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).defaultValue("next.id").expressionLanguageSupported(true).build();

	public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder().name("Maximum Buffer Size")
			.description(
					"Specifies the maximum amount of data to buffer (per file) in order to apply the regular expressions.  Files larger than the specified maximum will not be fully evaluated.")
			.required(true).addValidator(StandardValidators.DATA_SIZE_VALIDATOR).defaultValue("1 MB").build();

	// Relationships
	public static final Relationship REL_SUCCESS = new Relationship.Builder().name("success")
			.description("Successfully created FlowFile from SQL query result set.").build();
	public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
			.description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship").build();

	private List<PropertyDescriptor> descriptors;

	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
		descriptors.add(DBCP_SERVICE);
		descriptors.add(SQL_SELECT);
		descriptors.add(SQL_UPDATE);
		descriptors.add(SQL_INSERT);
		descriptors.add(SQL_LOCK_TO);
		descriptors.add(NEXT_ID_ATTRIBUTE_NAME);
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

		for (FlowFile fileToProcess : incomingFlowfiles) {

			final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
			final String stm_SEL = context.getProperty(SQL_SELECT).evaluateAttributeExpressions(fileToProcess).getValue();
			final String stm_UP = context.getProperty(SQL_UPDATE).evaluateAttributeExpressions(fileToProcess).getValue();
			final String stm_INS = context.getProperty(SQL_INSERT).evaluateAttributeExpressions(fileToProcess).getValue();
			final String LOCK_TO = context.getProperty(SQL_LOCK_TO).evaluateAttributeExpressions(fileToProcess).getValue();
			final String NEXT_ID_ATT_NAME = context.getProperty(NEXT_ID_ATTRIBUTE_NAME).evaluateAttributeExpressions(fileToProcess).getValue();
			final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
			logger.debug(" SQL_SELECT " + stm_SEL);
			logger.debug(" SQL_UPDATE " + stm_UP);
			logger.debug(" SQL_INSERT " + stm_INS);
			logger.debug(" SQL_LOCK_TIMEOUT " + LOCK_TO);
			final StopWatch stopWatch = new StopWatch(true);

			final byte[] buffer = new byte[maxBufferSize];
			final Map<String, String> result = new HashMap<String, String>();
			final Connection con = dbcpService.getConnection();
			try {
				final boolean originalAutoCommit = con.getAutoCommit();
				con.setAutoCommit(false);
				final PreparedStatement psInsert = con.prepareStatement(stm_INS);
				final PreparedStatement psUpdate = con.prepareStatement(stm_UP);
				final PreparedStatement psSetLock = con.prepareStatement(SET_LOCK_STMT + LOCK_TO);
				final Statement st = con.createStatement();
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
						try {

							// mandatory setting: default LOCK TIME WAIT value
							// is NO
							// WAIT !!!!
							psSetLock.execute();

							result.put(ERROR_STMT, stm_SEL);
							final ResultSet resultSet = st.executeQuery(stm_SEL);
							int id = 0;
							if (resultSet.next()) {
								id = resultSet.getInt(1);

								if (id == 0) {
									// a new row will be insert with id 1
									logger.debug(" resultSet empty ! creating a new row ...");
									id = inserRow();
								} else {
									// the id will be incremented
									logger.debug(" id " + id + " returned, performing update ...");
									id = updateRow(st, psUpdate);
								}
							} else {
								// a new row will be insert with id 1
								logger.debug(" resultSet null! creating a new row ...");
								id = inserRow();
							}
							logger.debug(" next id " + id + "!");
							result.put(NEXT_ID_ATT_NAME, id + "");

							con.commit();
						} catch (final SQLException e) {
							int errorCode = e.getErrorCode();
							try {
								// rollback and restore due exception !
								con.rollback();
								closeStmt(st);
								closeStmt(psUpdate);
								closeStmt(psInsert);
								closeStmt(psSetLock);
								con.setAutoCommit(originalAutoCommit);
								con.close();
							} catch (SQLException e1) {
								logger.error("SQLExc errcod[" + errorCode + "] performing " + e + result.get(ERROR_STMT));
							}
							// if caused by duplicated_id or
							// constraint_violation
							if (errorCode == -268 || errorCode == -239) {
								// the id will be incremented
								logger.error(" duplicated id exception returned, performing update ...");
								try {
									Connection con1 = dbcpService.getConnection();
									con1.setAutoCommit(false);
									final PreparedStatement psUpdate1 = con1.prepareStatement(stm_UP);
									final PreparedStatement psSetLock1 = con1.prepareStatement(SET_LOCK_STMT + LOCK_TO);
									final Statement st1 = con.createStatement();
									try {

										logger.debug("SETTING LOCK MODE! ");
										psSetLock1.execute();
										updateRow(st1, psUpdate1);
										con1.commit();
									} catch (SQLException e2) {
										errorCode = e.getErrorCode();										
										result.put(SQL_CODE, errorCode+"");
										logger.error("error got on workaround try update row: " + e.getMessage());
									} finally {
										closeStmt(st1);
										closeStmt(psUpdate1);
										closeStmt(psSetLock1);
										if (con != null) {
											try {
												con.setAutoCommit(originalAutoCommit);
												con.close();
											} catch (SQLException e3) {
												e3.printStackTrace();
											}
										}
									}
								} catch (SQLException e1) {
									e1.printStackTrace();
								}
							} else{
								result.put(SQL_CODE, errorCode+"");
								throw new ProcessException(e);
							}
						} finally {
							closeStmt(st);
							closeStmt(psUpdate);
							closeStmt(psInsert);
							closeStmt(psSetLock);
							if (con != null) {
								try {
									con.setAutoCommit(originalAutoCommit);
									con.close();
								} catch (SQLException e) {
									logger.error("error got on workaround duplicated k finally catch: " + e.getMessage());
								}
							}
							out.write(contentString.getBytes());
						}
					}

					private int inserRow() throws SQLException {
						result.put(ERROR_STMT, stm_INS);
						psInsert.executeUpdate();
						logger.debug("insert done!");
						return 1;
					}

					private int updateRow(Statement st, PreparedStatement preparedStatementUpdate) throws SQLException {
						int id = 0;
						String newSelectWhitFORUPDATE = stm_SEL + " FOR UPDATE";
						result.put(ERROR_STMT, newSelectWhitFORUPDATE);

						final ResultSet resultSet2 = st.executeQuery(newSelectWhitFORUPDATE);
						if (resultSet2.next())
							id = resultSet2.getInt(1);

						logger.debug("got id " + id);
						result.put(ERROR_STMT, stm_UP);
						preparedStatementUpdate.executeUpdate();
						logger.debug("update done!");
						return id;
					}

				});

				fileToProcess = session.putAttribute(fileToProcess, NEXT_ID_ATT_NAME, result.get(NEXT_ID_ATT_NAME));

				session.getProvenanceReporter().modifyContent(fileToProcess, "Retrieved some rows", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
				session.transfer(fileToProcess, REL_SUCCESS);
			} catch (final Throwable e) {
				logger.error("error got on main catch: " + e.getMessage());
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

	private void closeStmt(Statement statement) {
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}

}
