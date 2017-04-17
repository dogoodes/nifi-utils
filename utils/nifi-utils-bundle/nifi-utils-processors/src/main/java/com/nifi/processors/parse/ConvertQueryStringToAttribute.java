package com.nifi.processors.parse;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.util.StandardValidators;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;

@Tags({"convert", "query string", "attribute"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("")
public class ConvertQueryStringToAttribute extends AbstractProcessor {

    public static final String TRUE = "true";

    public static final PropertyDescriptor HTTP_REQUEST_URL = new PropertyDescriptor.Builder()
		.name("Http request url")
		.description("Http request url")
        .required(true)
        .dynamic(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();

    public static final PropertyDescriptor PREFIX = new PropertyDescriptor.Builder()
            .name("Prefix")
            .description("Prefix.")
            .required(false)
            .dynamic(false)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor POSFIX = new PropertyDescriptor.Builder()
            .name("Posfix")
            .description("Posfix.")
            .required(false)
            .dynamic(false)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor ATTRIBUTE_VALUE_IF_EMPTY = new PropertyDescriptor.Builder()
            .name("Attribute name if empty query string")
            .description("Empty attribute if it doesn't have query string.")
            .required(false)
            .dynamic(false)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
    	final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(HTTP_REQUEST_URL);
        properties.add(PREFIX);
        properties.add(POSFIX);
        properties.add(ATTRIBUTE_VALUE_IF_EMPTY);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
    
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            flowFile = session.create();
        }

        final ComponentLog logger = getLogger();

        Map<String, String> attributes = new HashMap<>();

        final String httpRequestUrl = context.getProperty(HTTP_REQUEST_URL).evaluateAttributeExpressions(flowFile).getValue();
        final String prefix = context.getProperty(PREFIX).evaluateAttributeExpressions(flowFile).getValue();
        final String posfix = context.getProperty(POSFIX).evaluateAttributeExpressions(flowFile).getValue();
        final String attributeValueIfEmpty = context.getProperty(ATTRIBUTE_VALUE_IF_EMPTY).evaluateAttributeExpressions(flowFile).getValue();

        List<NameValuePair> params = null;
        try {
            logger.info("Fazendo o parse do url.");
//            params = URLEncodedUtils.parse(new URI(httpRequestUrl), StandardCharsets.UTF_8);
            params = URLEncodedUtils.parse(new URI(httpRequestUrl), "UTF-8");
        } catch (URISyntaxException e) {
            e.printStackTrace();
            logger.error("Exception: " + e);
            session.transfer(flowFile, FAILURE);
        }

        if (!params.isEmpty())
            for (NameValuePair param : params)
                attributes.put(prefix + param.getName() + posfix, param.getValue());
        else if (!StringUtils.isEmpty(attributeValueIfEmpty))
            attributes.put(attributeValueIfEmpty, TRUE);

        flowFile = session.putAllAttributes(flowFile, attributes);
        session.transfer(flowFile, SUCCESS);
    }

    public static void main(String[] args) throws URISyntaxException {
        String httpRequestUrl = "http://localhost:8888/something.html?one=1&two=2&three=3&three=3a";
//        List<NameValuePair> params = URLEncodedUtils.parse(new URI(httpRequestUrl), StandardCharsets.UTF_8);
        List<NameValuePair> params = URLEncodedUtils.parse(new URI(httpRequestUrl), "UTF-8");
        for (NameValuePair param : params)
            System.out.println(param.getName() + param.getValue());
    }

}