package com.supprema.nifi.service;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;

import com.nifi.components.nifi.service.IJWTControllerService;

@Tags({ "jwt", "security", "rest" })
@CapabilityDescription("")
public class JWTControllerService extends AbstractControllerService implements IJWTControllerService {
	
	private static final String NAME_SIGNATURE_JWT = "Nome do segredo para gerar o token";
	private static final String NAME_ALGORITHM_JWT = "Algoritmo de encriptação";
	private static final String NAME_EXPIRATION_TIME_JWT = "Tempo de expiração do token";
	private static final String NAME_RETURN_VALUE = "Nome do token";
	
	//TODO Incluir nos combos
	// Allowable values for client auth
//    public static final AllowableValue CLIENT_NONE = new AllowableValue("No Authentication", "No Authentication",
//            "Processor will not authenticate clients. Anyone can communicate with this Processor anonymously");
//    public static final AllowableValue CLIENT_WANT = new AllowableValue("Want Authentication", "Want Authentication",
//            "Processor will try to verify the client but if unable to verify will allow the client to communicate anonymously");
//    public static final AllowableValue CLIENT_NEED = new AllowableValue("Need Authentication", "Need Authentication",
//            "Processor will reject communications from any client unless the client provides a certificate that is trusted by the TrustStore"
//            + "specified in the SSL Context Service");
	
	private static final String HS256 = "HS256";
	private static final String HS384 = "HS384";
	private static final String HS512 = "HS512";
	private static final String RS256 = "HS256";
	private static final String RS384 = "RS384";
	private static final String RS512 = "RS512";
	
	public static final PropertyDescriptor SIGNATURE_JWT = new PropertyDescriptor.Builder()
			.name(NAME_SIGNATURE_JWT)
			.description("")
			.defaultValue("my secret")
			.required(true)
			.expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor ALGORITHM_JWT = new PropertyDescriptor.Builder()
			.name(NAME_ALGORITHM_JWT)
			.description("")
			.required(true)
			.allowableValues(HS256, HS384, HS512, RS256, RS384, RS512)
			.defaultValue(HS256)
			.build();
	
	public static final PropertyDescriptor EXPIRATION_TIME_JWT = new PropertyDescriptor.Builder()
			.name(NAME_EXPIRATION_TIME_JWT)
			.description("Se deixar com 0 segundos, não terá tempo de expiração.")
			.defaultValue("0 seconds")
			.required(true)
			.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
			.sensitive(false)
			.build();
	
	public static final PropertyDescriptor RETURN_VALUE = new PropertyDescriptor.Builder()
			.name(NAME_RETURN_VALUE)
			.description("")
			.defaultValue("authorization")
			.required(true)
			.expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();
	
	@Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .name(propertyDescriptorName)
                .description("Atributos que serão adicionados ao Flowfile")
                .expressionLanguageSupported(true)
                .build();
    }

	private ConfigurationContext configContext;
	
    private static final List<PropertyDescriptor> properties;
    static {
        List<PropertyDescriptor> props = new ArrayList<>();
        props.add(SIGNATURE_JWT);
        props.add(ALGORITHM_JWT);
        props.add(EXPIRATION_TIME_JWT);
        props.add(RETURN_VALUE);
        properties = Collections.unmodifiableList(props);
    }
    
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
    
    private final Map<String, Object> claims = new ConcurrentHashMap<String, Object>();
    
    @Override
    public Map<String, Object> getPropertyClains() {
        return claims;
    }
    	
    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
    	configContext = context;
    	
    	final Map<PropertyDescriptor, String> properties = context.getProperties();
		
        for (Map.Entry<PropertyDescriptor, String> property : properties.entrySet()) {
        	if (!property.getKey().getDisplayName().equals(NAME_SIGNATURE_JWT) 
        			&& !property.getKey().getDisplayName().equals(NAME_ALGORITHM_JWT)
        			&& !property.getKey().getDisplayName().equals(NAME_EXPIRATION_TIME_JWT)
        			&& !property.getKey().getDisplayName().equals(NAME_RETURN_VALUE)) {
        		final String prop = context.getProperty(property.getKey()).evaluateAttributeExpressions().getValue();
        		claims.put(property.getKey().getDisplayName(), prop);
        	}
    	}
    	
    }

    @Override
    public String getSignateJwt() {
        return configContext.getProperty(SIGNATURE_JWT).evaluateAttributeExpressions().getValue();
    }
    
    @Override
    public String getAlgorithmJwt() {
        return configContext.getProperty(ALGORITHM_JWT).getValue();
    }
    
    @Override
    public Integer getExpirationTimeJwt() {
        return configContext.getProperty(EXPIRATION_TIME_JWT).asTimePeriod(TimeUnit.SECONDS).intValue();
    }
    
    @Override
    public String getReturnValueJwt() {
        return configContext.getProperty(RETURN_VALUE).evaluateAttributeExpressions().getValue();
    }
    
    @Override
    public String toString() {
        return "JWTControllerService[id=" + getIdentifier() + "]";
    }
}