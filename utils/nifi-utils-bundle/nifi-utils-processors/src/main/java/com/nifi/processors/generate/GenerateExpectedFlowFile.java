package com.nifi.processors.generate;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.*;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

@Tags({"flowfile", "generate"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("This processor will create a new FlowFile with the same values of the their attributes If their does not have a Incoming Relations, otherwise the processing will affect the Flowfile that is coming. Other possibilities are create a custom flowfile with specific content or include new attributes in FlowFile.")
public class GenerateExpectedFlowFile extends AbstractProcessor {

    public static final String CONTENT_OF_ATTRIBUTE = "ContentToAttribute";
	public static final AllowableValue DESTINATION_ATTRIBUTE = new AllowableValue("flowfile-attribute", "flowfile-attribue", "Include the dynamics attribute in new flowfile or the flowfile that is coming relation");
	public static final AllowableValue DESTINATION_CONTENT = new AllowableValue("flowfile-content", "flowfile-content", "Include the Content property value in new flowfile or the flowfile that is coming relation");
	public static final AllowableValue DESTINATION_CONTENT_TO_ATTRIBUTE = new AllowableValue("flowfile-content-to-attribute", "flowfile-content-to-attribute",
			"Set the Content of the flowfile that is coming in attribute that was declared in value of the property " + CONTENT_OF_ATTRIBUTE + ". Only String content is supported, see the Base64 transformation if you have types of information different from String");
	
    public static final PropertyDescriptor DESTINATION = new PropertyDescriptor.Builder()
    	.name("Destination")
    	.description("Control if attributes will be written as a new flowfile attribute" +
            "or written in the flowfile content.")
            .required(true)
            .allowableValues(DESTINATION_ATTRIBUTE, DESTINATION_CONTENT, DESTINATION_CONTENT_TO_ATTRIBUTE)
            .defaultValue(DESTINATION_ATTRIBUTE.getValue())
            .build();
    
    public static final PropertyDescriptor CONTENT = new PropertyDescriptor.Builder()
		.name("Content")
		.description("If flowfile-content has been setted, the content value can be used to fill the flowfile-content and the dynamics properties can be used to set attributes.")
        .required(false)
        .dynamic(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();
    
    public static final PropertyDescriptor CONTENT_OF_ATTR = new PropertyDescriptor.Builder()
    	.name(CONTENT_OF_ATTRIBUTE)
		.description("If flowfile-content-to-attribute has been setted on Destination property then the value of this attribute will be a new attribute with the content of flowfile that is coming")
        .required(false)
        .dynamic(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .defaultValue("null")
        .build();

    
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    
    private volatile Map<String, PropertyValue> propertyMap = new HashMap<>();
    private volatile Set<String> dynamicPropertyNames = new HashSet<>();
	

    @Override
    protected void init(final ProcessorInitializationContext context) {
    	final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(DESTINATION);
        properties.add(CONTENT);
        properties.add(CONTENT_OF_ATTR);
        this.properties = Collections.unmodifiableList(properties);

        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }
    
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .required(false)
                .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
                .name(propertyDescriptorName)
                .dynamic(true)
                .expressionLanguageSupported(true)
                .build();
    }
    
   @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }
    
    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (!descriptor.equals(DESTINATION) && !descriptor.equals(CONTENT) && !descriptor.equals(CONTENT_OF_ATTR)) {
            final Set<String> newDynamicPropertyNames = new HashSet<>(dynamicPropertyNames);
            if (newValue == null) {
                newDynamicPropertyNames.remove(descriptor.getName());
            } else if (oldValue == null) {    // new property
                newDynamicPropertyNames.add(descriptor.getName());
            }
            this.dynamicPropertyNames = Collections.unmodifiableSet(newDynamicPropertyNames);
        }
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
    	
    	final Map<String, PropertyValue> newPropertyMap = new HashMap<>();
        for (final PropertyDescriptor descriptor : context.getProperties().keySet()) {
            if (descriptor.isDynamic()) {
            	getLogger().debug("Adding new dynamic property: {}", new Object[]{descriptor});
            	newPropertyMap.put(descriptor.getName(), context.getProperty(descriptor));
            }
        }
        this.propertyMap = newPropertyMap;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
    	FlowFile flowFile = session.get();
    	boolean isComingFlowFile = true;
    	if (flowFile == null){
    		isComingFlowFile = false;
    		flowFile = session.create();
    	}
    	final Map<String, PropertyValue> propMap = this.propertyMap;
        final Map<String, String> attributes = new HashMap<>();
        for (final Map.Entry<String, PropertyValue> entry : propMap.entrySet()) {
            final PropertyValue value = entry.getValue().evaluateAttributeExpressions();
            attributes.put(entry.getKey(), value.getValue());
        }
       
    	String destination = context.getProperty(DESTINATION).getValue();
    	if (destination.equals(DESTINATION_CONTENT.getValue())){
    		final String content = context.getProperty(CONTENT).evaluateAttributeExpressions(flowFile).getValue();
    		if (content != null && !content.equals("")){
    			flowFile = session.write(flowFile, new OutputStreamCallback() {
    				@Override
                    public void process(final OutputStream out) throws IOException {
    					out.write(content.getBytes());
    				}
    			});
    		}
    	}
    	if ( attributes != null && !attributes.isEmpty() ){
    		for(final Map.Entry<String, String> entrie : attributes.entrySet()){
    			flowFile = session.putAttribute(flowFile, entrie.getKey(), entrie.getValue()); 
    		}
    	}
    	if (isComingFlowFile && destination.equals(DESTINATION_CONTENT_TO_ATTRIBUTE.getValue())){
    		String attributeName = context.getProperty(CONTENT_OF_ATTR).getValue();
    		if (attributeName != null && !attributeName.equals("null") && attributeName.length() > 0){
    			ByteArrayOutputStream out = new ByteArrayOutputStream();
    			session.exportTo(flowFile, out);
    			try{
    				flowFile = session.putAttribute(flowFile, attributeName, out.toString("UTF-8"));
    			}catch(UnsupportedEncodingException e){
    				getLogger().error("Error on transfer flowfile's content to attribute {}. Check if the content is String value!", new String[]{attributeName}, e );
    			}
    		}
    	}
    	
        //session.getProvenanceReporter().create(flowFile);
        session.transfer(flowFile, SUCCESS);
    }
}
