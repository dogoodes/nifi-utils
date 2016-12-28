package com.nifi.processors.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.StringUtils;
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
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.ContextHandler;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.ResourceHandler;

@EventDriven
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({ "server", "jetty", "rest", "dogood" })
@CapabilityDescription("")
@SeeAlso({})
@ReadsAttributes({ @ReadsAttribute(attribute = "", description = "") })
@WritesAttributes({ @WritesAttribute(attribute = "", description = "") })
public class StaticFileJetty extends AbstractProcessor {

	private volatile Server server;
	private AtomicBoolean initialized = new AtomicBoolean(false);

	private static final String PORT_OPEN = "port.open";

	public static final String TRUE = "True";
	public static final String FALSE = "False";

	private Set<Relationship> relationships;

	@Override
	public Set<Relationship> getRelationships() {
		return this.relationships;
	}

	public static final Relationship REL_SUCCESS = new Relationship.Builder()
			.name("success")
			.description("")
			.build();
	
	public static final Relationship REL_FAILURE = new Relationship.Builder()
			.name("failure")
			.description("")
			.build();

	private List<PropertyDescriptor> propDescriptors;

	@Override
	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
		return this.propDescriptors;
	}

	public static final PropertyDescriptor HOSTNAME = new PropertyDescriptor.Builder()
			.name("Hostname")
			.description("Hostname")
			.required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(false)
			.build();

	public static final PropertyDescriptor PORT = new PropertyDescriptor.Builder()
			.name("Listening Port")
			.description("Listening Port")
			.required(true)
			.addValidator(StandardValidators.createLongValidator(0L, 65535L, true))
			.expressionLanguageSupported(false)
			.defaultValue("80")
			.build();

	public static final PropertyDescriptor IDLE_TIMEOUT = new PropertyDescriptor.Builder()
			.name("Idle Timeout")
			.description("Idle Timeout")
			.required(true)
			.addValidator(StandardValidators.createLongValidator(0L, 65535L, true))
			.expressionLanguageSupported(false)
			.defaultValue("3000")
			.build();

	public static final PropertyDescriptor RESOURCE_BASE = new PropertyDescriptor.Builder()
			.name("Resource Base")
			.description("Resource Base")
			.required(true)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(false)
			.build();

	public static final PropertyDescriptor DIRECTORIES_LISTED = new PropertyDescriptor.Builder()
			.name("Directories Listed")
			.description("Directories Listed")
			.allowableValues(TRUE, FALSE)
			.defaultValue(TRUE)
			.required(true)
			.build();

	public static final PropertyDescriptor WELCOME_FILES = new PropertyDescriptor.Builder()
			.name("Welcome Files")
			.description("Welcome Files")
			.required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(false)
			.defaultValue("index.html")
			.build();

	public static final PropertyDescriptor CONTEXT_PATH = new PropertyDescriptor.Builder()
			.name("Context Path")
			.description("Context Path")
			.required(false)
			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
			.expressionLanguageSupported(false)
			.build();

	@Override
	protected void init(final ProcessorInitializationContext context) {
		final Set<Relationship> relationships = new HashSet<>();
		relationships.add(REL_SUCCESS);
		relationships.add(REL_FAILURE);
		this.relationships = Collections.unmodifiableSet(relationships);

		final List<PropertyDescriptor> pds = new ArrayList<>();
		pds.add(HOSTNAME);
		pds.add(PORT);
		pds.add(IDLE_TIMEOUT);
		pds.add(RESOURCE_BASE);
		pds.add(DIRECTORIES_LISTED);
		pds.add(WELCOME_FILES);
		pds.add(CONTEXT_PATH);
		this.propDescriptors = Collections.unmodifiableList(pds);
	}

	@Override
	public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
		FlowFile flowFile = session.create();

		try {
			if (!initialized.get()) {
				initializeServer(context);
			}
		} catch (Exception e) {
			context.yield();
			throw new ProcessException("Failed to initialize the server", e);
		}
		
		flowFile = session.putAttribute(flowFile, PORT_OPEN, getPort() + "");
		session.transfer(flowFile, REL_SUCCESS);

	}

	@OnScheduled
	public void createHttpServer(final ProcessContext context) {
		initialized.set(false);
	}

	private synchronized void initializeServer(final ProcessContext context) throws Exception {
		if (initialized.get()) {
			return;
		}
		final String host = context.getProperty(HOSTNAME).getValue();
		final int port = context.getProperty(PORT).asInteger();
		final int idleTimeout = context.getProperty(IDLE_TIMEOUT).asInteger();
		final String resourceBase = context.getProperty(RESOURCE_BASE).getValue();
		final String directoriesListed = context.getProperty(DIRECTORIES_LISTED).getValue();
		final String welcomeFiles = context.getProperty(WELCOME_FILES).getValue();
		final String contextPath = context.getProperty(CONTEXT_PATH).getValue();

		final Server server = new Server();
		final ServerConnector connector = new ServerConnector(server);

		if (StringUtils.isNotBlank(host))
			connector.setHost(host);
		connector.setPort(port);
		connector.setIdleTimeout(idleTimeout);
		server.setConnectors(new Connector[] { connector });

		ResourceHandler resourceHandler = new ResourceHandler();
		resourceHandler.setResourceBase(resourceBase);
		if (directoriesListed.equals(TRUE))
			resourceHandler.setDirectoriesListed(true);
		else
			resourceHandler.setDirectoriesListed(false);
		if (StringUtils.isNotBlank(welcomeFiles))
			resourceHandler.setWelcomeFiles(new String[] { welcomeFiles });

		ContextHandler contextHandler = new ContextHandler();
		if (StringUtils.isNotBlank(contextPath))
			contextHandler.setContextPath(contextPath);
		contextHandler.setHandler(resourceHandler);

		ContextHandlerCollection contexts = new ContextHandlerCollection();
		contexts.setHandlers(new Handler[] { contextHandler });

		server.setHandler(contexts);

		this.server = server;
		server.start();
		getLogger().info("Server started and listening on port " + getPort());
		initialized.set(true);
	}

	@OnStopped
	public void shutdownHttpServer() throws Exception {
		if (server != null) {
			getLogger().debug("Shutting down server");
			server.stop();
			server.destroy();
			server.join();
			getLogger().debug("Shut down {}", new Object[] { server });
		}
	}

	protected int getPort() {
		for (final Connector connector : server.getConnectors()) {
			if (connector instanceof ServerConnector) {
				return ((ServerConnector) connector).getLocalPort();
			}
		}
		throw new IllegalStateException("Server is not listening on any ports");
	}

}