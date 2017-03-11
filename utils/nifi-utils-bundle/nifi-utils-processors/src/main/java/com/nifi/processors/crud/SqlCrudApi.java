package com.nifi.processors.crud;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.util.*;
import java.util.concurrent.TimeUnit;

//TODO Por enquanto validarei apenas a assinatura, pois ainda não entendi muito bem sobre audience e issuer
@EventDriven
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "api", "sql", "rest", "crud" })
@CapabilityDescription("")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class SqlCrudApi extends AbstractProcessor {
	
	private static final String NAME_TOKEN_JWT = "Token";
	private static final String NAME_SIGNATURE_JWT = "Nome do segredo para generate o token";
//	private static final String NAME_ALGORITHM_JWT = "Algoritmo de encriptação";
//	private static final String NAME_EXPIRATION_TIME_JWT = "Tempo de expiração do token";
//	private static final String NAME_RETURN_VALUE = "Nome do token";
	
//	private static final String VALID_TOKEN = "Token válido";
//	private static final String INVALID_TOKEN = "Token inválido";
	
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("")
			.build();

	private Set<Relationship> relationships;
	
	private List<PropertyDescriptor> propDescriptors;
	
	public static final PropertyDescriptor TOKEN_JWT = new PropertyDescriptor.Builder()
			.name(NAME_TOKEN_JWT)
			.description("")
			.defaultValue("my secret")
			.required(true)
			.expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	public static final PropertyDescriptor SIGNATURE_JWT = new PropertyDescriptor.Builder()
			.name(NAME_SIGNATURE_JWT)
			.description("")
			.defaultValue("my secret")
			.required(true)
			.expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

//	public static final PropertyDescriptor RETURN_VALUE = new PropertyDescriptor.Builder()
//			.name(NAME_RETURN_VALUE)
//			.description("")
//			.defaultValue("Authorization")
//			.required(true)
//			.expressionLanguageSupported(true)
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//			.build();

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		this.relationships = Collections.unmodifiableSet(relationships);

		final List<PropertyDescriptor> pds = new ArrayList<>();
		pds.add(SIGNATURE_JWT);
//		pds.add(RETURN_VALUE);
		this.propDescriptors = Collections.unmodifiableList(pds);
	}

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}
	
	@Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .name(propertyDescriptorName)
                .description("Atributos que serão adicionados ao Flowfile")
                .expressionLanguageSupported(true)
                .build();
    }

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return this.propDescriptors;
	}
	
	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		final List<FlowFile> originalFlowFile = session.get(1);
    	
    	FlowFile flowFile = null;
    	for (FlowFile ff : originalFlowFile) {
    		flowFile = ff;
    	}
		
    	final ComponentLog logger = getLogger();
    	
//    	final String tokenJwt = context.getProperty(TOKEN_JWT).evaluateAttributeExpressions(flowFile).getValue();
//    	final String signatureJwt = context.getProperty(SIGNATURE_JWT).evaluateAttributeExpressions(flowFile).getValue();
//		final String returnValue = context.getProperty(RETURN_VALUE).evaluateAttributeExpressions(flowFile).getValue();
		
		final StopWatch stopWatch = new StopWatch(true);
		
		final Map<PropertyDescriptor, String> properties = context.getProperties();
//		final HashMap<String, Object> claims = new HashMap<String, Object>();
		
		
//		JWTVerifier verifier = new JWTVerifier("I.O.U a secret", null, "samples.auth0.com");
//      verifier.verify("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.wLlz9xDltxqKHQC7BeauPi5Q4KQK4nDjlRqQPvKVLYk");
		
		
//		final JWTSigner signer = new JWTSigner(signatureJwt);
		
		logger.info("Properties of the Processor JWTValidateToken.");
        for (Map.Entry<PropertyDescriptor, String> property : properties.entrySet()) {
        	if (!property.getKey().getDisplayName().equals(NAME_SIGNATURE_JWT)) {
        		final String prop = context.getProperty(property.getKey().getName()).evaluateAttributeExpressions(flowFile).getValue();
        		logger.info("Payload do property: " + prop);
//        		claims.put(property.getKey().getDisplayName(), prop);
        	}
    	}
        
//        final String token = signer.sign(claims);
        
//		flowFile = session.putAttribute(flowFile, returnValue, token);

		session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
		session.transfer(flowFile, REL_SUCCESS);
	}
	
}