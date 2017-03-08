package com.nifi.processors.ws;//package com.nifi.processors.ws;
//
//import java.io.ByteArrayInputStream;
//import java.io.FileInputStream;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//import java.net.InetAddress;
//import java.net.URL;
//import java.net.UnknownHostException;
//import java.security.KeyStore;
//import java.security.SecureRandom;
//import java.text.ParseException;
//import java.util.ArrayList;
//import java.util.Collection;
//import java.util.Collections;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Set;
//import java.util.concurrent.TimeUnit;
//import java.util.regex.Pattern;
//
//import javax.net.ssl.KeyManagerFactory;
//import javax.net.ssl.SSLContext;
//import javax.net.ssl.SSLSocketFactory;
//import javax.net.ssl.TrustManager;
//import javax.net.ssl.TrustManagerFactory;
//import javax.xml.namespace.QName;
//import javax.xml.parsers.DocumentBuilderFactory;
//import javax.xml.parsers.ParserConfigurationException;
//import javax.xml.soap.MessageFactory;
//import javax.xml.soap.SOAPBody;
//import javax.xml.soap.SOAPConstants;
//import javax.xml.soap.SOAPElement;
//import javax.xml.soap.SOAPEnvelope;
//import javax.xml.soap.SOAPException;
//import javax.xml.soap.SOAPFactory;
//import javax.xml.soap.SOAPMessage;
//import javax.xml.soap.SOAPPart;
//import javax.xml.ws.BindingProvider;
//import javax.xml.ws.Dispatch;
//import javax.xml.ws.Service;
//import javax.xml.ws.WebServiceException;
//import javax.xml.ws.soap.SOAPBinding;
//
//import org.apache.commons.lang3.StringUtils;
//import org.apache.nifi.annotation.behavior.InputRequirement;
//import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
//import org.apache.nifi.annotation.behavior.SupportsBatching;
//import org.apache.nifi.annotation.documentation.CapabilityDescription;
//import org.apache.nifi.annotation.documentation.Tags;
//import org.apache.nifi.annotation.notification.OnPrimaryNodeStateChange;
//import org.apache.nifi.annotation.notification.PrimaryNodeState;
//import org.apache.nifi.components.AllowableValue;
//import org.apache.nifi.components.PropertyDescriptor;
//import org.apache.nifi.components.ValidationContext;
//import org.apache.nifi.components.ValidationResult;
//import org.apache.nifi.expression.AttributeExpression;
//import org.apache.nifi.flowfile.FlowFile;
//import org.apache.nifi.logging.ProcessorLog;
//import org.apache.nifi.processor.AbstractProcessor;
//import org.apache.nifi.processor.ProcessContext;
//import org.apache.nifi.processor.ProcessSession;
//import org.apache.nifi.processor.ProcessorInitializationContext;
//import org.apache.nifi.processor.Relationship;
//import org.apache.nifi.processor.io.OutputStreamCallback;
//import org.apache.nifi.processor.util.StandardValidators;
//import org.apache.nifi.ssl.SSLContextService;
//import org.w3c.dom.Document;
//import org.xml.sax.SAXException;
//
//@SupportsBatching
//@Tags({ "wave", "webservice", "ws", "client" })
//@InputRequirement(Requirement.INPUT_ALLOWED)
//@CapabilityDescription("Processor responsavel pela execucao de um WebService. O WebService que sera acessado deve ser informado no atributo endpointaddress.")
//public class InvokeWS extends AbstractProcessor {
//	 static final AllowableValue RUNNER_ON_CLUSTER = new AllowableValue("Cluster", "Cluster", "This component will be performed on all nodes of the cluster.");
//	 static final AllowableValue RUNNER_ON_PRIMARY = new AllowableValue("Primary", "Primary", "This component will be performed on Primary Node Only and another node can pick up where the last node left off, if the Primary Node changes");
//
//	 public static final PropertyDescriptor PROP_RUNNER_ON = new PropertyDescriptor.Builder()
//	            .name("Modo de execucao")
//	            .description("This is used to determine whether state should be stored locally or across the cluster.")
//	            .allowableValues(RUNNER_ON_CLUSTER, RUNNER_ON_PRIMARY)
//	            .defaultValue(RUNNER_ON_PRIMARY.getValue())
//	            .required(true)
//	            .build();
//
//	public static final PropertyDescriptor PROP_SOAPVERSION = new PropertyDescriptor.Builder().name("Versao do SOAP")
//			.allowableValues("SOAP1.1", "SOAP1.2").description("A versao do SOAP que deve ser usado")
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true)
//			.build();
//
//	 public static final PropertyDescriptor PROP_SSL_CONTEXT_SERVICE = new PropertyDescriptor.Builder()
//	            .name("SSL Context Service")
//	            .description("The Controller Service to use in order to obtain an SSL Context")
//	            .required(false)
//	            .identifiesControllerService(SSLContextService.class)
//	            .build();
//
//	public static final PropertyDescriptor PROP_TARGETNAMESPACE = new PropertyDescriptor.Builder()
//			.name("URL do TargetNameSpace").description("Endereco do targetnamespace do WSDL")
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).addValidator(StandardValidators.URL_VALIDATOR)
//			.expressionLanguageSupported(true)
//			.required(true).build();
//
//	public static final PropertyDescriptor PROP_ENDPOINTADDRESS = new PropertyDescriptor.Builder()
//			.name("Endereco do WebService")
//			.description("Pode ser localizado no atributo location do elemento soap:address no WSDL.")
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).addValidator(StandardValidators.URL_VALIDATOR)
//			.required(true).expressionLanguageSupported(true).build();
//
//	public static final PropertyDescriptor PROP_SOAPACTION = new PropertyDescriptor.Builder()
//			.name("Endereco do SoapAction").description("Pode ser localizado no atributo soapAction do soap:operation.")
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//			.required(false).expressionLanguageSupported(true).build();
//
//	public static final PropertyDescriptor PROP_SERVICENAME = new PropertyDescriptor.Builder().name("Nome do Servico")
//			.description("O nome do servico pode ser encontrado no node wsdl:service atributo name do WSDL")
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).expressionLanguageSupported(true)
//			.build();
//
//	public static final PropertyDescriptor PROP_PORTNAME = new PropertyDescriptor.Builder().name("Nome da Porta")
//			.description(
//					"O nome da porta pode ser encontrado no node wsdl:port dentro de wsdl:service, verificar o atributo name do WSDL")
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(true).expressionLanguageSupported(true)
//			.build();
//
//	public static final PropertyDescriptor PROP_MESSAGENAME = new PropertyDescriptor.Builder()
//			.name("Nome da Mensagem de Envio")
//			.description("O nome da mensagem pode ser encontrado no node wsdl:message atributo name do WSDL")
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR).required(false).expressionLanguageSupported(true)
//			.build();
//
//	public static final PropertyDescriptor PROP_CONNECT_TIMEOUT = new PropertyDescriptor.Builder()
//			.name("Timeout de Conexao")
//			.description("Tempo maximo de espera pela resposta do WebService, caso o WebService nao retorne "
//					+ "uma resposta em tempo habil o FlowFile sera marcado como yield.")
//			.required(true).defaultValue("30 secs").addValidator(StandardValidators.TIME_PERIOD_VALIDATOR).build();
//
//	public static final PropertyDescriptor PROP_BASIC_AUTH_USERNAME = new PropertyDescriptor.Builder()
//			.name("Nome do Usuario (basic - authentication)")
//			.description("Nome do Usuario caso o WebService precise de Basic Authentication").required(false)
//			.addValidator(StandardValidators
//					.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x39\\x3b-\\x7e\\x80-\\xff]+$")))
//			.build();
//
//	public static final PropertyDescriptor PROP_BASIC_AUTH_PASSWORD = new PropertyDescriptor.Builder()
//			.name("Senha do Usuario (basic - authentication)")
//			.description("Senha do Usuario caso o WebService precise de Basic Authentication").required(false)
//			.sensitive(true)
//			.addValidator(
//					StandardValidators.createRegexMatchingValidator(Pattern.compile("^[\\x20-\\x7e\\x80-\\xff]+$")))
//			.build();
//
//	public static final PropertyDescriptor PROP_SUBJECTALTERNATIVENAME = new PropertyDescriptor.Builder()
//			.name("Subject Aternative Name")
//			.description("Nome ALternativo para WS que usam DNS")
//			.required(false)
//			.addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
//			.expressionLanguageSupported(true).build();
//
//	public static final PropertyDescriptor PROP_BODYXML = new PropertyDescriptor.Builder().name("bodyxml")
//			.description("O XML que devera ser enviado no corpo da mensagem.")
//			.required(false)
//			.addValidator(StandardValidators.createAttributeExpressionLanguageValidator(AttributeExpression.ResultType.STRING))
//			.expressionLanguageSupported(true).build();
//
//	public static final Relationship SUCCESS = new Relationship.Builder().name("success").build();
//	public static final Relationship RESPONSE = new Relationship.Builder().name("response").build();
//	public static final Relationship FAILURE = new Relationship.Builder().name("failure").build();
//
//	private List<PropertyDescriptor> properties;
//	private Set<Relationship> relationships;
//	private volatile boolean justElectedPrimaryNode = false;
//
//	@OnPrimaryNodeStateChange
//    public void onPrimaryNodeChange(final PrimaryNodeState newState) {
//        justElectedPrimaryNode = (newState == PrimaryNodeState.ELECTED_PRIMARY_NODE);
//        try{
//        	getLogger().debug("Setting onPrimaryNodeChange [" + justElectedPrimaryNode + "] to Host [" + InetAddress.getLocalHost().getHostAddress() + "]");
//        }catch(UnknownHostException e){
//        	getLogger().debug("Setting onPrimaryNodeChange [" + justElectedPrimaryNode + "]" );
//        }
//    }
//
//	@Override
//	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
//	    return properties;
//	}
//
//
//	 @Override
//    public Set<Relationship> getRelationships() {
//        return relationships;
//    }
//
//	@Override
//	protected void init(final ProcessorInitializationContext context) {
//		final List<PropertyDescriptor> properties = new ArrayList<>();
//		properties.add(PROP_RUNNER_ON);
//		properties.add(PROP_SOAPVERSION);
//		properties.add(PROP_TARGETNAMESPACE);
//		properties.add(PROP_ENDPOINTADDRESS);
//		properties.add(PROP_SOAPACTION);
//		properties.add(PROP_SERVICENAME);
//		properties.add(PROP_PORTNAME);
//		properties.add(PROP_MESSAGENAME);
//		properties.add(PROP_CONNECT_TIMEOUT);
//		properties.add(PROP_BASIC_AUTH_USERNAME);
//		properties.add(PROP_BASIC_AUTH_PASSWORD);
//		properties.add(PROP_BODYXML);
//		properties.add(PROP_SSL_CONTEXT_SERVICE);
//		properties.add(PROP_SUBJECTALTERNATIVENAME);
//		this.properties = Collections.unmodifiableList(properties);
//
//		final Set<Relationship> relationships = new HashSet<>();
//		relationships.add(SUCCESS);
//		relationships.add(RESPONSE);
//		relationships.add(FAILURE);
//		this.relationships = Collections.unmodifiableSet(relationships);
//	}
//
//	 @Override
//    protected Collection<ValidationResult> customValidate(final ValidationContext context) {
//        final Collection<ValidationResult> results = new ArrayList<>();
//
//        if (context.getProperty(PROP_ENDPOINTADDRESS).evaluateAttributeExpressions().getValue().startsWith("https") && context.getProperty(PROP_SSL_CONTEXT_SERVICE).getValue() == null) {
//            results.add(new ValidationResult.Builder()
//                    .explanation("Endpointaddress is set to HTTPS protocol but no SSLContext has been specified")
//                    .valid(false)
//                    .subject("SSL Context")
//                    .build());
//        }
//        return results;
//	 }
//
//	public WSClient setUpWSClient(final ProcessContext context,FlowFile Session) throws IOException{
//		final String subjectAlternativeName  = StringUtils.trimToEmpty(context.getProperty(PROP_SUBJECTALTERNATIVENAME).evaluateAttributeExpressions().getValue());
//		final String endpointAddress = StringUtils.trimToEmpty(context.getProperty(PROP_ENDPOINTADDRESS).evaluateAttributeExpressions().getValue());
//		final String targetNameSpace = StringUtils.trimToEmpty(context.getProperty(PROP_TARGETNAMESPACE).evaluateAttributeExpressions().getValue());
//		final String soapVersion = StringUtils.trimToEmpty(context.getProperty(PROP_SOAPVERSION).getValue());
//		final String serviceName = StringUtils.trimToEmpty(context.getProperty(PROP_SERVICENAME).evaluateAttributeExpressions().getValue());
//		final String portName = StringUtils.trimToEmpty(context.getProperty(PROP_PORTNAME).evaluateAttributeExpressions().getValue());
//		final String bodyXML = StringUtils.trimToEmpty(context.getProperty(PROP_BODYXML).evaluateAttributeExpressions(Session).getValue());
//		final int connectionTimeout = context.getProperty(PROP_CONNECT_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS)
//				.intValue();
//		final String userName = StringUtils.trimToEmpty(context.getProperty(PROP_BASIC_AUTH_USERNAME).getValue());
//		final String password = StringUtils.trimToEmpty(context.getProperty(PROP_BASIC_AUTH_PASSWORD).getValue());
//		final String messageName = StringUtils.trimToEmpty(context.getProperty(PROP_MESSAGENAME).evaluateAttributeExpressions().getValue());
//		final String soapAction = StringUtils.trimToEmpty(context.getProperty(PROP_SOAPACTION).evaluateAttributeExpressions().getValue());
//		final SSLContextService sslContextService = context.getProperty(PROP_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
//		WSClient wsClient = new WSClient();
//		wsClient.setBodyXML(bodyXML);
//		wsClient.setConnectionTimeout(connectionTimeout);
//		wsClient.setEndpointAddress(endpointAddress);
//		wsClient.setMessageName(messageName);
//		wsClient.setPassword(password);
//		wsClient.setPortName(portName);
//		wsClient.setServiceName(serviceName);
//		wsClient.setSoapAction(soapAction);
//		wsClient.setTargetNameSpace(targetNameSpace);
//		wsClient.setSoapVersion(soapVersion);
//		wsClient.setUserName(userName);
//		wsClient.setSSLContext(sslContextService);
//		wsClient.setSubjectAlternativeName(subjectAlternativeName);
//		return wsClient;
//	}
//
//
//	@Override
//	public void onTrigger(final ProcessContext context, final ProcessSession session) {
//		String runnerOn = context.getProperty(PROP_RUNNER_ON).getValue();
//		getLogger().debug("OnSchedule fired IsPrimaryNode [" + justElectedPrimaryNode + "]");
//		//if (runnerOn.equals(RUNNER_ON_PRIMARY.getValue()) && !justElectedPrimaryNode){
//
//		//	return;
//		//}
//
//		FlowFile requestFlowFile = session.get();
//		final ProcessorLog logger = getLogger();
//		FlowFile responseFlowFile = null;
//		try{
//			WSClient wsClient =  setUpWSClient(context, requestFlowFile);
//			final URL url = new URL(wsClient.getEndpointAddress());
//			if (requestFlowFile != null){
//				session.getProvenanceReporter().send(requestFlowFile, url.toExternalForm(), true);
//			}
//			final long startNanos = System.nanoTime();
//			final SOAPMessage message = wsClient.newCall();
//			if (requestFlowFile != null) {
//                responseFlowFile = session.create(requestFlowFile);
//            } else {
//                responseFlowFile = session.create();
//            }
//			responseFlowFile = session.write(responseFlowFile, new OutputStreamCallback() {
//	            @Override
//	            public void process(final OutputStream out) throws IOException {
//	            	try{
//	            		message.writeTo(out);
//	            	}catch(SOAPException e){
//	            		throw new IOException (e);
//	            	}
//
//	            }
//	        });
//			final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);
//			if(requestFlowFile != null) {
//                session.getProvenanceReporter().fetch(responseFlowFile, url.toExternalForm(), millis);
//            } else {
//                session.getProvenanceReporter().receive(responseFlowFile, url.toExternalForm(), millis);
//            }
//			if (requestFlowFile != null) {
//                session.transfer(requestFlowFile, SUCCESS);
//            }
//            if (responseFlowFile != null) {
//                session.transfer(responseFlowFile, RESPONSE);
//            }
//		} catch (final Exception e) {
//            // penalize or yield
//            if (requestFlowFile != null) {
//                logger.error("Roteando para {} devido a excecao: {}", new Object[]{FAILURE.getName(), e}, e);
//                requestFlowFile = session.penalize(requestFlowFile);
//                // transfer original to failure
//                session.transfer(requestFlowFile, FAILURE);
//            } else {
//                logger.error("Yielding processor devido a uma excecao encontrada no processor: {}", e);
//                context.yield();
//            }
//
//
//            // cleanup response flowfile, if applicable
//            try {
//                if (responseFlowFile != null) {
//                    session.remove(responseFlowFile);
//                }
//            } catch (final Exception e1) {
//                logger.error("Nao foi possivel fazer a reciclagem do FlowFile de resposta devido a excecao: {}", new Object[]{e1}, e1);
//            }
//        }
//
//
//	}
//
//	private class WSClient {
//		private final String SOAP_VERSION_1_1 = "SOAP1.1";
//		private final String SOAP_VERSION_1_2 = "SOAP1.2";
//
//		private String endpointAddress;
//		private String serviceName;
//		private String portName;
//		private String userName;
//		private String password;
//		private String messageName;
//		private String soapVersion;
//		private String bodyXML;
//		private String targetNameSpace;
//		private String soapAction;
//		private int connectionTimeout;
//		private SSLContextService sslContextService;
//		private String subjectAlternativeName;
//
//
//		private QName qServiceName = null;
//		private QName qPortName = null;
//
//		public SOAPMessage newCall() throws WebServiceException, SOAPException, ParseException {
//			MessageFactory mf = null;
//			SOAPFactory soapFactory = null;
//			Service service = Service.create(qServiceName);
//			Dispatch<SOAPMessage> dispatch = null;
//			if (getSoapVersion().equals(SOAP_VERSION_1_1)) {
//				service.addPort(qPortName, SOAPBinding.SOAP11HTTP_BINDING, getEndpointAddress());
//				dispatch = service.createDispatch(qPortName, SOAPMessage.class, Service.Mode.MESSAGE);
//				mf = MessageFactory.newInstance(SOAPConstants.SOAP_1_1_PROTOCOL);
//				soapFactory = SOAPFactory.newInstance(SOAPConstants.SOAP_1_1_PROTOCOL);
//			} else {
//				service.addPort(qPortName, SOAPBinding.SOAP12HTTP_BINDING, getEndpointAddress());
//				dispatch = service.createDispatch(qPortName, SOAPMessage.class, Service.Mode.MESSAGE);
//				mf = MessageFactory.newInstance(SOAPConstants.SOAP_1_2_PROTOCOL);
//				soapFactory = SOAPFactory.newInstance(SOAPConstants.SOAP_1_2_PROTOCOL);
//			}
//			SOAPMessage request = mf.createMessage();
//			request.setProperty(SOAPMessage.CHARACTER_SET_ENCODING, "utf-8");
//			SOAPPart part = request.getSOAPPart();
//			SOAPEnvelope env = part.getEnvelope();
//			SOAPBody body = env.getBody();
//			SOAPElement bodyRequest = env.getBody();
//			if (!StringUtils.isBlank(getMessageName())){
//				String[] tokenizerElements = getMessageName().split("[.]");
//				for(String tokenizerElement : tokenizerElements){
//					if (tokenizerElement != null && !StringUtils.isBlank(tokenizerElement)){
//						bodyRequest = bodyRequest.addChildElement(tokenizerElement, "", getTargetNameSpace());
//					}
//				}
//			}
//			if (getBodyXML() != null && !getBodyXML().equals("")) {
//				try {
//					//bodyRequest.setNodeValue(getBodyXML());
//					//bodyRequest.setValue(getBodyXML());
//					//bodyRequest.setTextContent(getBodyXML());
//					bodyRequest.addChildElement(soapFactory.createElement(toDOM(getBodyXML()).getDocumentElement()));
//				} catch (Exception e) {
//					throw new ParseException(e.getMessage(), 0);
//				}
//			}
//			if (!StringUtils.isBlank(getSoapAction())){
//				dispatch.getRequestContext().put(Dispatch.SOAPACTION_URI_PROPERTY, getSoapAction());
//			}
//			dispatch.getRequestContext().put(BindingProvider.SOAPACTION_USE_PROPERTY, Boolean.TRUE);
//			if (getUserName() != null && !getUserName().trim().equals("")) {
//				dispatch.getRequestContext().put(BindingProvider.USERNAME_PROPERTY, getUserName()); // daniel.nahas
//				if (getPassword() != null && !getPassword().trim().equals("")) {
//					dispatch.getRequestContext().put(BindingProvider.PASSWORD_PROPERTY, getPassword()); // "SBF@0001"
//				}
//			}
//
//			if (sslContextService != null){
//				try{
//					dispatch.getRequestContext().put("com.sun.xml.internal.ws.transport.https.client.SSLSocketFactory", getSocketFactory(sslContextService));
//				}catch(Exception e){
//					throw new WebServiceException(e);
//				}
//			}
//
//			if (subjectAlternativeName != null){
//				dispatch.getRequestContext().put("com.sun.xml.internal.ws.transport.https.client.hostname.verifier", new HostNameVerifier(subjectAlternativeName));
//			}
//
//			request.saveChanges();
//			try{
//				System.out.println("\n");
//				request.writeTo(System.out);
//			}catch(IOException e){
//			}
//			SOAPMessage response = dispatch.invoke(request);
//
//			try{
//				System.out.println("\n");
//				response.writeTo(System.out);
//				System.out.println("fim");
//			}catch(IOException e){
//			}
//			//return dispatch.invoke(request);
//			return response;
//		}
//
//		public String getTargetNameSpace() {
//			return targetNameSpace;
//		}
//
//		public String getSoapAction() {
//			return soapAction;
//		}
//
//		public void setSoapAction(String soapAction) {
//			this.soapAction = soapAction;
//		}
//
//		public String getSubjectAlternativeName() {
//			return subjectAlternativeName;
//		}
//
//		public void setSubjectAlternativeName(String subjectAlternativeName) {
//			this.subjectAlternativeName = subjectAlternativeName;
//		}
//
//		public void setTargetNameSpace(String targetNameSpace) {
//			if (getServiceName() != null) {
//				qServiceName = new QName(targetNameSpace, getServiceName());
//			}
//			if (getPortName() != null) {
//				qPortName = new QName(targetNameSpace, getPortName());
//			}
//			this.targetNameSpace = targetNameSpace;
//		}
//
//		public String getEndpointAddress() {
//			return endpointAddress;
//		}
//
//		public void setEndpointAddress(String endpointAddress) {
//			this.endpointAddress = endpointAddress;
//		}
//
//		public String getServiceName() {
//			return serviceName;
//		}
//
//		public void setServiceName(String serviceName) {
//			if (getTargetNameSpace() != null) {
//				qServiceName = new QName(getTargetNameSpace(), serviceName);
//			}
//			this.serviceName = serviceName;
//		}
//
//		public String getPortName() {
//			return portName;
//		}
//
//		public void setPortName(String portName) {
//			if (getTargetNameSpace() != null) {
//				qPortName = new QName(getTargetNameSpace(), portName);
//			}
//			this.portName = portName;
//		}
//
//		public String getUserName() {
//			return userName;
//		}
//
//		public void setUserName(String userName) {
//			this.userName = userName;
//		}
//
//		public String getPassword() {
//			return password;
//		}
//
//		public void setPassword(String password) {
//			this.password = password;
//		}
//
//		public String getMessageName() {
//			return messageName;
//		}
//
//		public void setMessageName(String messageName) {
//			this.messageName = messageName;
//		}
//
//		public String getSoapVersion() {
//			return soapVersion;
//		}
//
//		public void setSoapVersion(String soapVersion) {
//			this.soapVersion = soapVersion;
//		}
//
//		public String getBodyXML() {
//			return bodyXML;
//		}
//
//		public void setBodyXML(String bodyXML) {
//			this.bodyXML = bodyXML;
//		}
//
//		public int getConnectionTimeout() {
//			return connectionTimeout;
//		}
//
//		public void setConnectionTimeout(int connectionTimeout) {
//			this.connectionTimeout = connectionTimeout;
//		}
//
//		public void setSSLContext(SSLContextService sslContextService){
//			this.sslContextService = sslContextService;
//		}
//	}
//
//	protected Document toDOM(String xml) throws Exception {
//		Document dom;
//		try {
//			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
//			factory.setNamespaceAware(true);
//			dom = factory.newDocumentBuilder().parse(new ByteArrayInputStream(xml.getBytes("UTF-8")));
//		} catch (ParserConfigurationException e) {
//			throw e;
//		} catch (SAXException e) {
//			throw e;
//		} catch (IOException e) {
//			throw e;
//		}
//		return dom;
//	}
//
//
//
//	protected SSLSocketFactory getSocketFactory(SSLContextService sslContextService) throws Exception{
//		//String ambiente = properties.getProperty("com.wave.erp.vendas.processing.saopaulo.ambiente");
//		String pathCertOrigem = sslContextService.getKeyStoreFile(); //waveParametros.getCaminhoCertificadoWave(); //properties.getProperty(ambiente + ".com.wave.erp.vendas.processing.betim.keystore.path");
//		String pswdCertOrigem = sslContextService.getKeyStorePassword();//properties.getProperty(ambiente + ".com.wave.erp.vendas.processing.betim.keystore.pswd");
//		String typeCertOrigem =  sslContextService.getKeyStoreType(); //"PKCS12";//properties.getProperty(ambiente + ".com.wave.erp.vendas.processing.betim.keystore.type");
//
//		String tpathCertDestino = sslContextService.getTrustStoreFile();//waveParametros.getCaminhoCertificadoReceita();//properties.getProperty(ambiente + ".com.wave.erp.vendas.processing.betim.truststore.path");
//		String tpswdCertDestino = sslContextService.getTrustStorePassword(); //properties.getProperty(ambiente + ".com.wave.erp.vendas.processing.betim.truststore.pswd");
//		String ttypeCertDestino = sslContextService.getTrustStoreType();//properties.getProperty(ambiente + ".com.wave.erp.vendas.processing.betim.truststore.type");
//
//		char[] keystorepass = pswdCertOrigem.toCharArray();
//		InputStream keystoreFile = new FileInputStream(pathCertOrigem);
//
//		try {
//			KeyStore keyStore = KeyStore.getInstance(typeCertOrigem);
//			keyStore.load(keystoreFile, keystorepass);
//			KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
//			kmf.init(keyStore, keystorepass);
//
//			char[] truststorepass = tpswdCertDestino.toCharArray();
//			KeyStore trustStore = KeyStore.getInstance(ttypeCertDestino);
//			trustStore.load(new FileInputStream(tpathCertDestino), truststorepass);
//			//trustStore.load(waveParametros.getCertificadoReceitaStream(), truststorepass);
//			TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
//			tmf.init(trustStore);
//
//			SSLContext sslContext = SSLContext.getInstance(sslContextService.getSslAlgorithm());
//			sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), new SecureRandom());
//
//			return sslContext.getSocketFactory();
//		} catch (Exception e) {
//			throw new Exception("Error creating context for SSLSocket!", e);
//		}
//	}
//
//}
