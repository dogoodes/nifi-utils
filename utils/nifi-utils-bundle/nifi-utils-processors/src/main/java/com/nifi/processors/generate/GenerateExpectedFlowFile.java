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
import java.nio.charset.StandardCharsets;
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

@Tags({"generate", "flowfile"})
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@CapabilityDescription("This processor will create a new FlowFile with the same values of the their attributes If their does not have a Incoming Relations, otherwise the processing will affect the Flowfile that is coming. Other possibilities are create a custom flowfile with specific content or include new attributes in FlowFile.")
public class GenerateExpectedFlowFile extends AbstractProcessor {

    public static final PropertyDescriptor CONTENT = new PropertyDescriptor.Builder()
		.name("Content Text")
		.description("If flowfile-content has been setted, the content value can be used to fill the flowfile-content and the dynamics properties can be used to set attributes.")
        .required(false)
        .dynamic(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(false)
        .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    
    private volatile Map<String, PropertyValue> propertyMap = new HashMap<>();

    @Override
    protected void init(final ProcessorInitializationContext context) {
    	final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(CONTENT);
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
                .description("Attribute to be added to Flowfile.")
                .dynamic(true)
                .expressionLanguageSupported(false)
                .build();
    }
    
   @Override
    public Set<Relationship> getRelationships() {
       return relationships;
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
    	if (flowFile == null) {
    		flowFile = session.create();
    	}

        final String content = context.getProperty(CONTENT).getValue();
    	final Map<String, PropertyValue> propMap = this.propertyMap;
        final Map<String, String> attributes = new HashMap<>();

        for (final Map.Entry<String, PropertyValue> entry : propMap.entrySet()) {
            final PropertyValue value = entry.getValue().evaluateAttributeExpressions();
            attributes.put(entry.getKey(), value.getValue());
        }
       

        if (content != null && !content.equals("")){
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(content.getBytes(StandardCharsets.UTF_8));
                }
            });
        }

    	if ( attributes != null && !attributes.isEmpty() ){
    		for(final Map.Entry<String, String> entrie : attributes.entrySet()){
    			flowFile = session.putAttribute(flowFile, entrie.getKey(), entrie.getValue()); 
    		}
    	}

        session.transfer(flowFile, SUCCESS);
    }
}
