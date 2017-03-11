package com.nifi.processors.parse;

import java.io.*;

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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

@Tags({"convert", "content", "attribute"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("This processor will create a new FlowFile with the same values of the their attributes If their does not have a Incoming Relations, otherwise the processing will affect the Flowfile that is coming. Other possibilities are create a custom flowfile with specific content or include new attributes in FlowFile.")
public class ConvertContentToAttribute extends AbstractProcessor {

    public static final PropertyDescriptor ATTRIBUTE_NAME = new PropertyDescriptor.Builder()
		.name("Attribute Name")
		.description("")
        .required(false)
        .dynamic(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();

    public static final PropertyDescriptor ATTRIBUTE_VALUE_IF_EMPTY = new PropertyDescriptor.Builder()
            .name("Attribute Name If Empty Content")
            .description("Attribute Value If Empty Content")
            .required(false)
            .dynamic(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();
    
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .build();

    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
    	final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(ATTRIBUTE_NAME);
        properties.add(ATTRIBUTE_VALUE_IF_EMPTY);
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
    public Set<Relationship> getRelationships() {
        return relationships;
    }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) {
    	FlowFile flowFile = session.get();
    	if (flowFile == null) {
    		flowFile = session.create();
    	}

        final String attributeName = context.getProperty(ATTRIBUTE_NAME).evaluateAttributeExpressions(flowFile).getValue();
        final String attributeValueIfEmpty = context.getProperty(ATTRIBUTE_VALUE_IF_EMPTY).evaluateAttributeExpressions(flowFile).getValue();
        final int lengthFlowFile = (int) flowFile.getSize();
        final byte[] buffer = new byte[lengthFlowFile];

        if (lengthFlowFile > 0) {
            final AtomicInteger bufferedByteCount = new AtomicInteger(0);
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    bufferedByteCount.set(StreamUtils.fillBuffer(in, buffer, false));
                }
            });

            final String contentString = new String(buffer, 0, bufferedByteCount.get(), StandardCharsets.UTF_8);

            flowFile = session.putAttribute(flowFile, attributeName, contentString);
        } else {
            flowFile = session.putAttribute(flowFile, attributeName, attributeValueIfEmpty);
        }

        session.transfer(flowFile, SUCCESS);
    }

}