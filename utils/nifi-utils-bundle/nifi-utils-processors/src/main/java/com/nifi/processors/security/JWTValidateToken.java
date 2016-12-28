package com.nifi.processors.security;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import com.auth0.jwt.JWTVerifier;
import com.nifi.components.nifi.service.IJWTControllerService;

@EventDriven
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "jwt", "security", "rest", "dogood" })
@CapabilityDescription("")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class JWTValidateToken extends AbstractProcessor {
	
	private static final String VALID_TOKEN = "token.valido";
	private static final String INVALID_TOKEN = "token.invalido";
	private static final String ERROR_TOKEN = "erro.token";
	
	private Set<Relationship> relationships;
	
	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}
	
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("")
			.build();
	
	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure")
			.description("")
			.build();
	
	private List<PropertyDescriptor> propDescriptors;
	
	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return this.propDescriptors;
	}
	
	public static final PropertyDescriptor CONTROLLER_SERVICE_JWT = new PropertyDescriptor.Builder()
            .name("JWT Controller Service")
            .description("")
            .required(true)
            .identifiesControllerService(IJWTControllerService.class)
            .build();
	
	public static final PropertyDescriptor TOKEN_JWT = new PropertyDescriptor.Builder()
			.name("Token")
			.description("")
			.required(true)
			.expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);

		final List<PropertyDescriptor> pds = new ArrayList<>();
		pds.add(CONTROLLER_SERVICE_JWT);
		pds.add(TOKEN_JWT);
		this.propDescriptors = Collections.unmodifiableList(pds);
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		final List<FlowFile> originalFlowFile = session.get(1);
    	
    	FlowFile flowFile = null;
    	for (FlowFile ff : originalFlowFile) {
    		flowFile = ff;
    	}
		
    	final ComponentLog logger = getLogger();
    	
    	final IJWTControllerService jwtControllerService = context.getProperty(CONTROLLER_SERVICE_JWT).asControllerService(IJWTControllerService.class);
    	final String signatureJwt = jwtControllerService.getSignateJwt();
    	final String tokenJwt = context.getProperty(TOKEN_JWT).evaluateAttributeExpressions(flowFile).getValue();
    	
		logger.info("Validar o token.");
		JWTVerifier verifier = new JWTVerifier(signatureJwt);
		try {
			verifier.verify(tokenJwt);
			
			flowFile = session.putAttribute(flowFile, VALID_TOKEN, tokenJwt);
			session.transfer(flowFile, REL_SUCCESS);
		} catch (Exception e) {
			logger.info("Erro no token.");
			e.printStackTrace();
			flowFile = session.putAttribute(flowFile, INVALID_TOKEN, tokenJwt);
			flowFile = session.putAttribute(flowFile, ERROR_TOKEN, e.getMessage());
			session.transfer(flowFile, REL_FAILURE);
		}
		
	}
	
}