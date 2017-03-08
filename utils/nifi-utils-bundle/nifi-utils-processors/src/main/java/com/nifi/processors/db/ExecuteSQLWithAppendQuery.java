package com.nifi.processors.db;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
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
import java.nio.charset.Charset;
import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@EventDriven
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"sql", "select", "insert", "jdbc", "query", "database", "wave"})
@CapabilityDescription("Preencher...")
public class ExecuteSQLWithAppendQuery extends AbstractProcessor {
	
	public static final String ERROR_STMT = "error.message";
	public static final String TRUE = "True";
    public static final String FALSE = "False";
	public static final String INSERT_CONTENT = "Insert como conteúdo";
	public static final String RESULTSET_ATTRIBUTE = "Resultado da select como atributo";
	
	private Set<Relationship> relationships;
	
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success")
            .build();
    
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("SQL query execution failed. Incoming FlowFile will be penalized and routed to this relationship.")
            .build();
    
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
            .defaultValue("select * from TABELA")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    
    public static final PropertyDescriptor SQL_RESULT_INSERT_QUERY = new PropertyDescriptor.Builder()
            .name("SQL insert query")
            .description("SQL insert query")
            .defaultValue("insert into TABELA")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor QUERY_TIMEOUT = new PropertyDescriptor.Builder()
            .name("Max Wait Time")
            .description("The maximum amount of time allowed for a running SQL select query "
                    + " , zero means there is no limit. Max time less than 1 second will be equal to zero.")
            .defaultValue("0 seconds")
            .required(true)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .sensitive(false)
            .build();
    
    public static final PropertyDescriptor CHARACTER_SET = new PropertyDescriptor.Builder()
            .name("Character Set")
            .description("The Character Set in which the file is encoded.")
            .required(true)
            .addValidator(StandardValidators.CHARACTER_SET_VALIDATOR)
            .defaultValue("UTF-8")
            .build();
    
    public static final PropertyDescriptor MAX_BUFFER_SIZE = new PropertyDescriptor.Builder()
            .name("Maximum Buffer Size")
            .description("Specifies the maximum amount of data to buffer (per file) in order to apply the regular expressions.  Files larger than the specified maximum will not be fully evaluated.")
            .required(true)
            .addValidator(StandardValidators.DATA_SIZE_VALIDATOR)
            .defaultValue("1 MB")
            .build();
    
    public static final PropertyDescriptor RESET_CONTENT = new PropertyDescriptor.Builder()
            .name("Reset Content")
            .description("Reset Content...")
            .allowableValues(TRUE, FALSE)
            .defaultValue(TRUE)
            .required(true)
            .build();
    
    public static final PropertyDescriptor TYPE_APPEND = new PropertyDescriptor.Builder()
            .name("Type Append")
            .description("Deseja adicionar ao FlowFile o insert no conteúdo ou o resultado da select como atributo?")
            .allowableValues(INSERT_CONTENT, RESULTSET_ATTRIBUTE)
            .defaultValue(INSERT_CONTENT)
            .required(true)
            .build();
    
    public static final PropertyDescriptor ATTRIBUTE_TO_RETURN_WHEN_EMPTY = new PropertyDescriptor.Builder()
			.name("Attribute to return when empty")
			.description("This is the name of the attribute of the flowfile returned when the resultset is empty")
			.defaultValue("NORESULT")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
    
    public static final PropertyDescriptor ITERATE = new PropertyDescriptor.Builder()
            .name("Iterate")
            .description("Iterate")
            .allowableValues(FALSE, TRUE)
            .defaultValue(FALSE)
            .required(true)
            .build();
    
    public static final PropertyDescriptor NUMBER_OF_TIMES_TO_ITERATE = new PropertyDescriptor.Builder()
			.name("Number of times to iterate")
			.description("Number of times to iterate.")
			.defaultValue("2")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
			.build();
    
    public static final PropertyDescriptor CUSTOM_EL = new PropertyDescriptor.Builder()
			.name("Custom Expression Language")
			.description("Custom Expression Language.")
			.defaultValue("#{iterate}")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
    
    public ExecuteSQLWithAppendQuery(){}
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
    	final Set<Relationship> relationship = new HashSet<>();
    	relationship.add(REL_SUCCESS);
    	relationship.add(REL_FAILURE);
    	this.relationships = Collections.unmodifiableSet(relationship);

    	final List<PropertyDescriptor> propertyDescriptor = new ArrayList<>();
    	propertyDescriptor.add(DBCP_SERVICE);
    	propertyDescriptor.add(SQL_SELECT_QUERY);
    	propertyDescriptor.add(SQL_RESULT_INSERT_QUERY);
    	propertyDescriptor.add(QUERY_TIMEOUT);
    	propertyDescriptor.add(CHARACTER_SET);
    	propertyDescriptor.add(MAX_BUFFER_SIZE);
    	propertyDescriptor.add(RESET_CONTENT);
    	propertyDescriptor.add(TYPE_APPEND);
    	propertyDescriptor.add(ATTRIBUTE_TO_RETURN_WHEN_EMPTY);
    	propertyDescriptor.add(ITERATE);
    	propertyDescriptor.add(NUMBER_OF_TIMES_TO_ITERATE);
    	propertyDescriptor.add(CUSTOM_EL);
    	this.propDescriptors = Collections.unmodifiableList(propertyDescriptor);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.propDescriptors;
    }
    
    private static final String SEPARADOR = ",";
    private static final String QUEBRA_DE_LINHA = "\n";
    private static final String INICIAR_PARAMETRO = "(";
    private static final String FINALIZAR_PARAMETRO = ")";
    private static final String INSTRUCAO_VALUES_DB = " values ";
    private static final String FINALIZAR_INSTRUCAO = ";";
    
	@Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
    	final List<FlowFile> updatedFlowfile = session.get(1);
    	
    	FlowFile flowFile = null;
    	for (FlowFile ff : updatedFlowfile) {
    		flowFile = ff;
    	}
    	
    	final ComponentLog logger = getLogger();
        final DBCPService dbcpService = context.getProperty(DBCP_SERVICE).asControllerService(DBCPService.class);
        String selectQuery = context.getProperty(SQL_SELECT_QUERY).evaluateAttributeExpressions(flowFile).getValue();
        final String resultInsertQuery = context.getProperty(SQL_RESULT_INSERT_QUERY).getValue();
        final Integer queryTimeout = context.getProperty(QUERY_TIMEOUT).asTimePeriod(TimeUnit.SECONDS).intValue();
        final Charset charset = Charset.forName(context.getProperty(CHARACTER_SET).getValue());
        final int maxBufferSize = context.getProperty(MAX_BUFFER_SIZE).asDataSize(DataUnit.B).intValue();
        final String resetContent = context.getProperty(RESET_CONTENT).getValue();
        final String appendAttribute = context.getProperty(TYPE_APPEND).getValue();
        final String attributeToReturnWhenEmpty = context.getProperty(ATTRIBUTE_TO_RETURN_WHEN_EMPTY).getValue();
        final String iterate = context.getProperty(ITERATE).getValue();
        final String numberOfTimesToIterate = context.getProperty(NUMBER_OF_TIMES_TO_ITERATE).evaluateAttributeExpressions(flowFile).getValue();
        final String customEL = context.getProperty(CUSTOM_EL).getValue();
        final StopWatch stopWatch = new StopWatch(true);
        final byte[] buffer = new byte[maxBufferSize];
        
        final Map<String, String> result = new HashMap<String, String>();
        final Map<String, String> resultAttributes = new HashMap<String, String>();
        final StringBuffer sb = new StringBuffer();
        
        if(iterate.equals(TRUE)) {
        	Integer nti = new Integer(0);
        	try {
        		nti = Integer.parseInt(numberOfTimesToIterate);
        		if (nti < 1) nti = 1;
        		selectQuery = evaluateSelectQuery(selectQuery, customEL, nti);
        	} catch (NumberFormatException e) {
        		e.printStackTrace();
        		logger.error("Não é possível iterar um valor não numérico!");
				logger.error("Valor: " + nti.toString());
        		session.transfer(flowFile, REL_FAILURE);
        	}
        }
        
        Connection connection = null;
        Statement statement = null;
        try {
        	connection = dbcpService.getConnection();
            statement = connection.createStatement();
            statement.setQueryTimeout(queryTimeout);
            
            logger.debug("Executando a query " + selectQuery);
			result.put(ERROR_STMT, selectQuery);
			
            final ResultSet resultSet = statement.executeQuery(selectQuery);
            final ResultSetMetaData rsMeta = resultSet.getMetaData();
            
            final boolean resultSetEmpty = !resultSet.isBeforeFirst();
            
            final int columnCnt = rsMeta.getColumnCount();
	        final List<String> columnNames = new ArrayList<String>();
	        for (int i=1; i<=columnCnt; i++) {
	            columnNames.add(rsMeta.getColumnName(i));
	        }
        
    		if (flowFile.getSize() > maxBufferSize) {
                session.transfer(flowFile, REL_FAILURE);
            }
    		
    		if (flowFile.getSize() > 0 && resetContent.equals(FALSE)) {
    			final AtomicInteger bufferedByteCount = new AtomicInteger(0);
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream in) throws IOException {
                        bufferedByteCount.set(StreamUtils.fillBuffer(in, buffer, false));
                    }
                });

                final String contentString = new String(buffer, 0, bufferedByteCount.get(), charset);
                sb.append(contentString + QUEBRA_DE_LINHA);
                
                logger.debug("Pegando o conteúdo do FlowFile " + contentString);
                result.put(ERROR_STMT, contentString);
            }

    		if (appendAttribute.equals(INSERT_CONTENT)) {
    			flowFile = session.write(flowFile, new OutputStreamCallback() {
    				public void process(final OutputStream out) throws IOException {
    					try {
    						while (resultSet.next()) {
    							final StringBuilder sbColunas = new StringBuilder();
    							final StringBuilder sbValores = new StringBuilder();
    							for (int i = 1; i <= columnCnt; i++) {
    								sbColunas.append(columnNames.get(i - 1));
    								sbValores.append(resultSet.getString(i));
    								if (i != columnCnt) {
    									sbColunas.append(SEPARADOR);
    									sbValores.append(SEPARADOR);
    								}
    							}
    							sb.append(resultInsertQuery + INICIAR_PARAMETRO + sbColunas.toString() + FINALIZAR_PARAMETRO + INSTRUCAO_VALUES_DB + INICIAR_PARAMETRO + sbValores.toString() + FINALIZAR_PARAMETRO + FINALIZAR_INSTRUCAO + QUEBRA_DE_LINHA);
    						}
    						out.write(sb.toString().getBytes(charset));
    					} catch (SQLException e) {
    						e.printStackTrace();
    						logger.error("SQLException ErrorCode do ResultSet: " + e.getErrorCode());
    						logger.error("SQLException e do ResultSet: " + e);
    						logger.error("SQLException do ResultSet: " + result.get(ERROR_STMT));
    					}
    				}
    			});
    		} else {
    			try {
    				if (resultSetEmpty) {
    					resultAttributes.put(attributeToReturnWhenEmpty, attributeToReturnWhenEmpty);
    				} else {
    					while (resultSet.next()) {
    						for (int i = 1; i <= columnCnt; i++) {
    							resultAttributes.put(columnNames.get(i - 1).toLowerCase(), resultSet.getString(i));
    						}
    					}
    				}
				} catch (SQLException e) {
					e.printStackTrace();
					logger.error("SQLException ErrorCode do ResultSet: " + e.getErrorCode());
					logger.error("SQLException e do ResultSet: " + e);
					logger.error("SQLException do ResultSet: " + result.get(ERROR_STMT));
				}
    		}
    		if (appendAttribute.equals(RESULTSET_ATTRIBUTE)) {
				flowFile = session.putAllAttributes(flowFile, resultAttributes);
			}
			session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
			session.transfer(flowFile, REL_SUCCESS);
        } catch (final SQLException e) {
        	String error_code = e.getErrorCode()+"";
        	e.printStackTrace();
        	logger.error("SQLException ErrorCode: " + error_code);
			logger.error("SQLException e: " + e);
			logger.error("SQLException " + result.get(ERROR_STMT));
			flowFile = session.putAttribute(flowFile, ERROR_STMT, "ErrorCode: " + error_code + " ErrorMessage: " + result.get(ERROR_STMT));
            session.transfer(flowFile, REL_FAILURE);
        } finally {
        	if (statement != null) {
        		try {
        			logger.debug("Fechando o Statement");
        			statement.close();
				} catch (SQLException e) {
					e.printStackTrace();
					logger.error("SQLException ao fechar o Statement" + e.getErrorCode());
					logger.error("SQLException ao fechar o Statement" + e);
				}
        	}
        	if (connection != null) {
        		try {
        			logger.debug("Fechando o Connection");
					connection.close();
				} catch (SQLException e) {
					e.printStackTrace();
					logger.error("SQLException ao fechar a Connection" + e.getErrorCode());
					logger.error("SQLException ao fechar a Connection" + e);
				}
        	}
        }
    }
	
	protected String evaluateSelectQuery(String selectQuery, String customEL, Integer numberOfTimesToIterate) {
		StringBuffer sb = new StringBuffer();
		for (int i = 1; i <= numberOfTimesToIterate; i++) {
			String newQuery = selectQuery.replace(customEL, i + "");
			sb.append(newQuery);
			if (i != numberOfTimesToIterate) sb.append(" union ");
		}
		
		return sb.toString();
	}
}