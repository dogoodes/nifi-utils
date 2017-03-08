package com.nifi.processors.parse;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.StreamUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@EventDriven
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"json", "attribute"})
@CapabilityDescription("")
public class ConvertJsonToAttribute extends AbstractProcessor {
	
	public static final String LOWER_CASE_SENSITIVE = "Lower";
	public static final String UPPER_CASE_SENSITIVE = "Upper";
	public static final String DEFAULT_CASE_SENSITIVE = "Default";

	public static final String ERROR = "error.name.attribute";
	public static final String UTF8 = "UTF-8";
	
	private Set<Relationship> relationships;
	
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success")
            .build();
    
    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Failure")
            .build();
    
    public static final PropertyDescriptor CASE_SENSITIVE = new PropertyDescriptor.Builder()
            .name("Case sensitive")
            .description("Case sensitive")
            .allowableValues(LOWER_CASE_SENSITIVE, UPPER_CASE_SENSITIVE, DEFAULT_CASE_SENSITIVE)
            .defaultValue(DEFAULT_CASE_SENSITIVE)
            .required(true)
            .build();
    
	public static final PropertyDescriptor RETURN_WHEN_EMPTY = new PropertyDescriptor.Builder()
			.name("Return when empty")
			.description("Value to return when empty")
			.defaultValue("json.empty")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();    
    
    private List<PropertyDescriptor> propDescriptors;
    
    public ConvertJsonToAttribute(){}
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
    	final Set<Relationship> relationship = new HashSet<>();
    	relationship.add(REL_SUCCESS);
    	relationship.add(REL_FAILURE);
    	this.relationships = Collections.unmodifiableSet(relationship);

    	final List<PropertyDescriptor> propertyDescriptor = new ArrayList<>();
    	propertyDescriptor.add(CASE_SENSITIVE);
    	propertyDescriptor.add(RETURN_WHEN_EMPTY);
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
    	
    	Map<String, String> attributes = new HashMap<String, String>();
    	Map<String, String> attributesCaseSensitive = new HashMap<String, String>();
    	
    	FlowFile flowFile = null;
    	for (FlowFile ff : updatedFlowfile) {
    		flowFile = ff;
    	}
    	
    	final ComponentLog logger = getLogger();
        
        final String caseSensitive = context.getProperty(CASE_SENSITIVE).getValue();
        final String returnWhenEmpty = context.getProperty(RETURN_WHEN_EMPTY).getValue();
        
        final byte[] buffer = new byte[(int) flowFile.getSize()];
        if (flowFile.getSize() > 0) {
			final AtomicInteger bufferedByteCount = new AtomicInteger(0);
            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    bufferedByteCount.set(StreamUtils.fillBuffer(in, buffer, false));
                }
            });

            String contentString = null;
			try {
				contentString = new String(buffer, 0, bufferedByteCount.get(), UTF8);
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
				flowFile = session.putAttribute(flowFile, ERROR, e.getMessage());
				session.transfer(flowFile, REL_FAILURE);
			}
            logger.debug("Getting the FlowFile content " + contentString);
            
            Gson gson = new Gson();
            attributes = gson.fromJson(contentString, new TypeToken<Map<String, String>>(){}.getType());
            
            if (attributes.size() > 0)
            	attributesCaseSensitive = manipular(attributes, caseSensitive);
            else
            	attributesCaseSensitive.put(returnWhenEmpty, contentString);
        }
                        
        flowFile = session.putAllAttributes(flowFile, attributesCaseSensitive);
		session.transfer(flowFile, REL_SUCCESS);
    }
    
	protected Map<String, String> manipular(Map<String, String> map, String caseSensitive) {
		Map<String, String> attributes = new HashMap<String, String>();
		if (caseSensitive.equalsIgnoreCase(DEFAULT_CASE_SENSITIVE))
			return map;
		else {
			for (Map.Entry<String, String> mapy : map.entrySet()) {
	    		attributes.put(caseSensitive(mapy.getKey(), caseSensitive), mapy.getValue());
	    	}
			return attributes;
		}
	}

    protected String caseSensitive(String key, String caseSensitive) {
    	if (caseSensitive.equalsIgnoreCase(LOWER_CASE_SENSITIVE))
    		return key.toLowerCase();
		else
			return key.toUpperCase();
    }
    
}