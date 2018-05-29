package com.nifi.processors.generate;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;

import java.io.*;
import java.util.*;

@EventDriven
@SideEffectFree
@SupportsBatching
@Tags({"flowfile", "splittext", "generate fragments transaction"})
@InputRequirement(Requirement.INPUT_REQUIRED)
@CapabilityDescription("This processor will create a new FlowFile for each line that have in text file.")
@WritesAttributes({
		@WritesAttribute(attribute = "text.line.count", description = "The number of lines of text from the original FlowFile that were copied"),
		@WritesAttribute(attribute = "text.line.content", description = "The line of the text file that was extracted")})
public class GenerateFragmentsTransaction extends AbstractProcessor {

	public static final String FRAGMENT_IDENTIFIER = "fragment.identifier";
	public static final String FRAGMENT_COUNT = "fragment.count";
	public static final String FRAGMENT_INDEX = "fragment.index";

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("success")
			.build();

	public static final Relationship REL_ORIGINAL = new Relationship.Builder()
			.name("original")
			.description("The original FlowFile that was split into segments. If the FlowFile fails processing, nothing will be sent to this relationship")
			.build();

	private List<PropertyDescriptor> properties;
	private Set<Relationship> relationships;

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final List<PropertyDescriptor> properties = new ArrayList<>();
		this.properties = Collections.unmodifiableList(properties);

		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_ORIGINAL);
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
		final List<FlowFile> splits = new ArrayList<>();
		final FlowFile originalFlowFile = session.get();

		if (originalFlowFile.getSize() > 0) {
			final UUID uuid = UUID.randomUUID();
			final List<String> lines = new ArrayList<>();

			session.read(originalFlowFile, new InputStreamCallback() {
				@Override
				public void process(final InputStream in) throws IOException {
					final BufferedReader br = new BufferedReader(new InputStreamReader(in));
					String line = null;
					while ((line = br.readLine()) != null) {
						if (!line.trim().equals("")) {
							lines.add(line.trim());
						}
					}
				}
			});

			final Integer numberLines = lines.size();
			for (int i = 0; i < numberLines; i++) {
				final String currLine = lines.get(i).trim();
				final Integer countLine = new Integer(i + 1);
				FlowFile splitFile = session.create(originalFlowFile);
				splitFile = session.write(splitFile, new OutputStreamCallback() {
					@Override
					public void process(final OutputStream out) throws IOException {
						out.write(currLine.getBytes());
					}
				});
				splitFile = session.putAttribute(splitFile, FRAGMENT_COUNT, numberLines.toString());
				splitFile = session.putAttribute(splitFile, FRAGMENT_IDENTIFIER, uuid.toString());
				splitFile = session.putAttribute(splitFile, FRAGMENT_INDEX, countLine.toString());
				splits.add(splitFile);
			}

			if (!splits.isEmpty())
				session.transfer(splits, REL_SUCCESS);
		}

		session.transfer(originalFlowFile, REL_ORIGINAL);
	}

}