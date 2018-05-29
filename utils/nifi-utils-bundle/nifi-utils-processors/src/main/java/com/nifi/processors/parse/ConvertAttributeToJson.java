package com.nifi.processors.parse;

import com.nifi.processors.utils.Node;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.TimeUnit;

@EventDriven
@InputRequirement(Requirement.INPUT_REQUIRED)
@Tags({"carrefour", "dolphin", "convert", "json", "attribute"})
@CapabilityDescription("")
public class ConvertAttributeToJson extends AbstractProcessor {

	public static final String STRING = "String";

	private Set<Relationship> relationships;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Success")
            .build();

	public static final PropertyDescriptor ROOT_JSON = new PropertyDescriptor.Builder()
            .name("Json root name")
			.description("Json root name.")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.build();

	public static final PropertyDescriptor RETURN_WHEN_EMPTY = new PropertyDescriptor.Builder()
			.name("Value when it has no attributes")
			.description("Value when it has no attributes.")
			.defaultValue("no.attribute")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(true)
			.build();

    private List<PropertyDescriptor> propDescriptors;

	private volatile Map<String, PropertyValue> propertyMap = new HashMap<>();

	public ConvertAttributeToJson(){}
    
    @Override
    protected void init(final ProcessorInitializationContext context) {
    	final Set<Relationship> relationship = new HashSet<>();
    	relationship.add(REL_SUCCESS);
    	this.relationships = Collections.unmodifiableSet(relationship);

    	final List<PropertyDescriptor> propertyDescriptor = new ArrayList<>();
		propertyDescriptor.add(ROOT_JSON);
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
	protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
		return new PropertyDescriptor.Builder()
				.required(false)
				.name(propertyDescriptorName)
				.description("Mapped attributes to be added to json in content flowfile.")
				.allowableValues(STRING)
				.defaultValue(STRING)
				.expressionLanguageSupported(false)
				.build();
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
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.get();
		if (flowFile == null)
			flowFile = session.create();

		final String rootJson = context.getProperty(ROOT_JSON).evaluateAttributeExpressions(flowFile).getValue();
		final String returnWhenEmpty = context.getProperty(RETURN_WHEN_EMPTY).evaluateAttributeExpressions(flowFile).getValue();

		final Map<String, PropertyValue> mappedAttributes = this.propertyMap;
		final Map<String, String> attributesFF = flowFile.getAttributes();
		final Map<String, String> matchedAttributes = matchAttributes(mappedAttributes, attributesFF);

		final int lengthFlowFile = (int) flowFile.getSize();
		final byte[] buffer = new byte[lengthFlowFile];
		final StopWatch stopWatch = new StopWatch(true);

		final Node<String> root = new Node<>(rootJson);

		if (matchedAttributes != null || !matchedAttributes.isEmpty()) {
			for(final Map.Entry<String, String> attribute : matchedAttributes.entrySet()) {
				String[] attr = attribute.getKey().split("\\.");
				String value = root.getData();
				Node<String> node = root;
				for (int i=0; i<attr.length; i++) {
					if(!attr[i].equalsIgnoreCase(root.getData())) {
						value += "."+attr[i];
						node = associate(node, value, true);
					}
				}
			}

			final String json = generateJson(root, matchedAttributes, root);

			if (json != null && !json.equals("")){
				flowFile = session.write(flowFile, new OutputStreamCallback() {
					@Override
					public void process(final OutputStream out) throws IOException {
						out.write(json.getBytes(StandardCharsets.UTF_8));
					}
				});
			}
		} else
			flowFile = session.putAttribute(flowFile, returnWhenEmpty, returnWhenEmpty);

		session.getProvenanceReporter().modifyContent(flowFile, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
		session.transfer(flowFile, REL_SUCCESS);
	}

	protected Map<String, String> matchAttributes(final Map<String, PropertyValue> mappedAttributes, final Map<String, String> attributesFF) {
		final Map<String, String> matchAttributes = new HashMap<>();

		if (mappedAttributes != null && !mappedAttributes.isEmpty()) {
				for (String attr : mappedAttributes.keySet()) {
					if (attributesFF.containsKey(attr))
						matchAttributes.put(attr, attributesFF.get(attr));
			}
		}
		return matchAttributes;
	}

	protected Node<String> associate(final Node<String> root, final String value, final boolean isRoot) {
		Node<String> child = null;
		if(!root.hasChildren() && isRoot) {
			child = new Node<>(value);
			root.addChild(child);
			return child;
		} else {
			for (Node<String> node : root.getChildren()) {
				if(node.getData().equalsIgnoreCase(value))
					return node;
			}
			for (Node<String> node : root.getChildren()) {
				child = associate(node, value, false);
				if(child != null)
					return child;
			}
		}

		if(isRoot) {
			child = new Node<>(value);
			root.addChild(child);
		}
		return child;
	}

	protected String generateJson(final Node<String> node, final Map<String, String> attributes, final Node<String> root) {
		StringBuilder json = new StringBuilder();
		Node<String> child;
		String[] data = node.getData().split("\\.");

		if (node.getData().equalsIgnoreCase(root.getData()))
			json.append("{");

		json.append("\"");
		json.append(data[data.length - 1]);
		json.append("\"");
		json.append(":");
		if(node.hasChildren()) {
			json.append("{");
			for (int i = 0; i < node.getChildren().size(); i++) {
				child = node.getChildren().get(i);
				json.append(generateJson(child, attributes, root));
				if(node.getChildren().size() > 1 && i < node.getChildren().size() - 1)
					json.append(",");
			}
			json.append("}");
		} else {
			json.append("\"");
			json.append(attributes.get(node.getData()));
			json.append("\"");
		}

		if (node.getData().equalsIgnoreCase(root.getData()))
			json.append("}");

		return json.toString();
	}

}