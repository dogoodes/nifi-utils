package com.nifi.processors.parse;

import com.nifi.processors.utils.QueryParams;
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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

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

@Tags({"convert", "query string", "attribute"})
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("")
public class ConvertQueryStringToMap extends AbstractProcessor {

    public static final PropertyDescriptor HTTP_REQUEST_URL = new PropertyDescriptor.Builder()
		.name("Http Request URL")
		.description("Http Request URL")
        .required(false)
        .dynamic(false)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
        .expressionLanguageSupported(true)
        .build();

    public static final PropertyDescriptor PREFIX = new PropertyDescriptor.Builder()
            .name("Prefix")
            .description("Prefix")
            .required(false)
            .dynamic(false)
            .addValidator(StandardValidators.ATTRIBUTE_EXPRESSION_LANGUAGE_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor POSFIX = new PropertyDescriptor.Builder()
            .name("Posfix")
            .description("Posfix")
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
        final List<FlowFile> originalFlowFile = session.get(1);

        FlowFile flowFile = null;
        for (FlowFile ff : originalFlowFile) {
            flowFile = ff;
        }

        final ComponentLog logger = getLogger();

        Map<String, String> attributes = new HashMap<String, String>();

        final String httpRequestUrl = context.getProperty(HTTP_REQUEST_URL).evaluateAttributeExpressions(flowFile).getValue();
        final String prefix = context.getProperty(PREFIX).evaluateAttributeExpressions(flowFile).getValue();
        final String posfix = context.getProperty(POSFIX).evaluateAttributeExpressions(flowFile).getValue();

        try {
            List<NameValuePair> params = URLEncodedUtils.parse(new URI(httpRequestUrl), StandardCharsets.UTF_8);
        } catch (URISyntaxException e) {
            //TODO Acho que é necessário colocar o caminho de falha agora
            e.printStackTrace();
        }


//        final int lengthFlowFile = (int) flowFile.getSize();
//        final byte[] buffer = new byte[lengthFlowFile];

//        if (lengthFlowFile > 0) {
//            final AtomicInteger bufferedByteCount = new AtomicInteger(0);
//            session.read(flowFile, new InputStreamCallback() {
//                @Override
//                public void process(final InputStream in) throws IOException {
//                    bufferedByteCount.set(StreamUtils.fillBuffer(in, buffer, false));
//                }
//            });
//
//            final String contentString = new String(buffer, 0, bufferedByteCount.get(), StandardCharsets.UTF_8);
//
//            flowFile = session.putAttribute(flowFile, attributeName, contentString);
//        } else {
//            flowFile = session.putAttribute(flowFile, attributeName, attributeValueIfEmpty);
//        }

        session.transfer(flowFile, SUCCESS);
    }

}