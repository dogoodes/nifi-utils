package com.supprema.processors.security;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
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
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

@EventDriven
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({ "jwt", "security", "rest" })
@CapabilityDescription("")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class JWTSigner extends AbstractProcessor {

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("")
			.build();

	private Set<Relationship> relationships;
	
	private List<PropertyDescriptor> propDescriptors;

	public static final PropertyDescriptor RETURN_VALUE = new PropertyDescriptor.Builder()
			.name("Return when empty")
			.description("")
			.defaultValue("Authorization")
			.required(true)
			.expressionLanguageSupported(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		this.relationships = Collections.unmodifiableSet(relationships);

		final List<PropertyDescriptor> pds = new ArrayList<>();
		pds.add(RETURN_VALUE);
		this.propDescriptors = Collections.unmodifiableList(pds);
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
		final List<FlowFile> originalFlowFile = session.get(1);
    	
    	FlowFile flowFile = null;
    	for (FlowFile ff : originalFlowFile) {
    		flowFile = ff;
    	}
		
    	final ComponentLog logger = getLogger();
    	
		final String MUDAR = context.getProperty(RETURN_VALUE).evaluateAttributeExpressions(flowFile).getValue();
		
		final StopWatch stopWatch = new StopWatch(true);
		
		
		
		session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
		session.transfer(flowFile, REL_SUCCESS);

	}
}