package com.nifi.processors.security;

import java.util.ArrayList;
import java.util.Collections;
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
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.StopWatch;

import com.auth0.jwt.Algorithm;
import com.auth0.jwt.JWTAlgorithmException;
import com.auth0.jwt.JWTSigner;
import com.nifi.components.nifi.service.IJWTControllerService;

@EventDriven
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "jwt", "security", "rest", "dogood" })
@CapabilityDescription("")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class JWTGenerateToken extends AbstractProcessor {
	
	private Set<Relationship> relationships;
	
	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}
	
	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
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
	
	@Override
	protected void init(final ProcessorInitializationContext context) {
		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		this.relationships = Collections.unmodifiableSet(relationships);

		final List<PropertyDescriptor> pds = new ArrayList<>();
		pds.add(CONTROLLER_SERVICE_JWT);
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
    	final String algorithmJwt = jwtControllerService.getAlgorithmJwt();
    	final Integer expirationTimeJwt = jwtControllerService.getExpirationTimeJwt();
		final String returnValue = jwtControllerService.getReturnValueJwt();
		final Map<String, Object> claims = jwtControllerService.getPropertyClains();
		
		final StopWatch stopWatch = new StopWatch(true);
		final JWTSigner signer = new JWTSigner(signatureJwt);
		
        logger.info("Buscar o algoritmo.");
        final Algorithm algorithm = findAlgorithm(algorithmJwt);
        
        String token = "";
        if (expirationTimeJwt > 0)
        	token = signer.sign(claims, new JWTSigner.Options().setAlgorithm(algorithm).setExpirySeconds(expirationTimeJwt));
        else
        	token = signer.sign(claims, new JWTSigner.Options().setAlgorithm(algorithm));
        
//        logger.info("Codificar o token.");
//        final String encodeToken = encodeToken(token);
        
		flowFile = session.putAttribute(flowFile, returnValue, token);

		session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
		session.transfer(flowFile, REL_SUCCESS);
	}
	
	protected Algorithm findAlgorithm(String algorithm) {
		try {
			return Algorithm.findByName(algorithm);
		} catch (JWTAlgorithmException e) {
			//TODO Adicionar a exception
			e.printStackTrace();
			return null;
		}
	}
	
	//TODO Estou tendo problemas na codificação
//	protected String encodeToken(String token) {
//		final Base64 encoder = new Base64(true);
//		try {
//			final byte[] encodeToken = (byte[]) encoder.encode(token);
//			return new String(encodeToken, "UTF-8");
//		} catch (EncoderException | UnsupportedEncodingException e) {
//			//TODO Adicionar a exception
//			e.printStackTrace();
//			return null;
//		}
//	}
	
}