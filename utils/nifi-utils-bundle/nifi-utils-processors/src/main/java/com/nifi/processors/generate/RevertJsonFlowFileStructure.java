package com.nifi.processors.generate;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.nifi.processors.utils.FlowFileStructure;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@EventDriven
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"json", "attribute", "flow file"})
@CapabilityDescription("")
public class RevertJsonFlowFileStructure extends AbstractProcessor {

    private Set<Relationship> relationships;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success")
            .build();

    public static final PropertyDescriptor ATTRIBUTE_VALUE_IF_EMPTY = new PropertyDescriptor.Builder()
            .name("Attribute Name If Empty Content")
            .description("Attribute Value If Empty Content.")
            .defaultValue("no.attribute")
            .required(true)
            .dynamic(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    private List<PropertyDescriptor> propDescriptors;

    public RevertJsonFlowFileStructure(){}

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationship = new HashSet<>();
        relationship.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(relationship);

        final List<PropertyDescriptor> propertyDescriptor = new ArrayList<>();
        propertyDescriptor.add(ATTRIBUTE_VALUE_IF_EMPTY);
        this.propDescriptors = Collections.unmodifiableList(propertyDescriptor);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return this.propDescriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final List<FlowFile> updatedFlowfile = session.get(1);

        FlowFile flowFile = null;
        for (FlowFile ff : updatedFlowfile) {
            flowFile = ff;
        }

        final String attributeValueIfEmpty = context.getProperty(ATTRIBUTE_VALUE_IF_EMPTY).evaluateAttributeExpressions(flowFile).getValue();

        final ComponentLog logger = getLogger();
        final Gson gson = new GsonBuilder().create();
        final StopWatch stopWatch = new StopWatch(true);
        Map<String, String> attributes = new HashMap<>();

        FlowFileStructure flowFileStructure = null;

        final byte[] buffer = new byte[(int) flowFile.getSize()];
        if (flowFile.getSize() > 0) {
            final AtomicInteger bufferedByteCount = new AtomicInteger(0);
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    bufferedByteCount.set(StreamUtils.fillBuffer(in, buffer, false));
                }
            });

            final String contentString = new String(buffer, 0, bufferedByteCount.get(), StandardCharsets.UTF_8);
            logger.debug("Getting the FlowFile content " + contentString);

            flowFileStructure = gson.fromJson(contentString, FlowFileStructure.class);
            attributes = gson.fromJson(flowFileStructure.getAttribute(), new TypeToken<Map<String, String>>(){}.getType());

            flowFile = session.putAllAttributes(flowFile, attributes);

            JsonElement content = flowFileStructure.getContent();
            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(OutputStream out) throws IOException {
                    out.write(gson.toJson(content).getBytes(StandardCharsets.UTF_8));
                }
            });

            session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
        } else
            flowFile = session.putAttribute(flowFile, attributeValueIfEmpty, "true");

        session.transfer(flowFile, REL_SUCCESS);
    }

}