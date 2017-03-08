///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package br.com.dogood.nifi.service;
//
//import com.mchange.v2.c3p0.ComboPooledDataSource;
//import org.apache.nifi.annotation.documentation.CapabilityDescription;
//import org.apache.nifi.annotation.documentation.Tags;
//import org.apache.nifi.annotation.lifecycle.OnDisabled;
//import org.apache.nifi.annotation.lifecycle.OnEnabled;
//import org.apache.nifi.components.PropertyDescriptor;
//import org.apache.nifi.controller.AbstractControllerService;
//import org.apache.nifi.controller.ConfigurationContext;
//import org.apache.nifi.dbcp.DBCPService;
//import org.apache.nifi.processor.exception.ProcessException;
//import org.apache.nifi.processor.util.StandardValidators;
//import org.apache.nifi.reporting.InitializationException;
//
//import java.beans.PropertyVetoException;
//import java.net.MalformedURLException;
//import java.net.URL;
//import java.net.URLClassLoader;
//import java.sql.Connection;
//import java.sql.Driver;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.concurrent.TimeUnit;
//
///**
// * Implementation of for Database Connection Pooling Service. Apache DBCP is
// * used for connection pooling functionality.
// *
// */
//@Tags({ "wave", "dbcp", "jdbc", "database", "connection", "pooling", "store" })
//@CapabilityDescription("WaveDBCPService checks if asked connections have broken pipe. It provides a Database Connection Pooling Service. Connections can be asked from pool and returned after usage.")
//public class WaveDBCPConnectionPool extends AbstractControllerService implements DBCPService {
//
//	public static final PropertyDescriptor JDBC_URL = new PropertyDescriptor.Builder()
//			.name("JDBC URL")
//			.description("The JDBC URL of the database from which Connections can and should be acquired. "
//							+ " Should resolve via java.sql.DriverManager to an appropriate JDBC Driver (which you can ensure will be loaded and available by setting driverClass), or if you wish to specify which driver to use directly (and avoid DriverManager resolution), you may specify driverClass in combination with forceUseNamedDriverClass. Unless you are supplying your own unpooled DataSource, a must always be provided and appropriate for the JDBC driver, however it is resolved. ")
//			.defaultValue(null)
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//			.required(true)
//			.build();
//
//	public static final PropertyDescriptor DRIVER_CLASS = new PropertyDescriptor.Builder()
//			.name("Driver Class")
//			.description("The fully-qualified class name of the JDBC driverClass that is expected to provide Connections. c3p0 will preload any class specified here to ensure that appropriate URLs may be resolved to an instance of the driver by java.sql.DriverManager. ")
//			.defaultValue(null).required(true)
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//			.build();
//
////	public static final PropertyDescriptor DB_DRIVER_JAR_URL = new PropertyDescriptor.Builder()
////			.name("Database Driver Jar Url")
////			.description(
////					"Optional database driver jar file path url. For example 'file:///var/tmp/mariadb-java-client-1.1.7.jar'")
////			.defaultValue(null).required(true)
////			.addValidator(StandardValidators.URL_VALIDATOR).build();
//
//	public static final PropertyDescriptor USER = new PropertyDescriptor.Builder()
//			.name("Database User")
//			.description("Database user name. For applications using ComboPooledDataSource or any c3p0-implemented unpooled DataSources — DriverManagerDataSource or the DataSource returned by DataSources.unpooledDataSource() — defines the username that will be used for the DataSource's default getConnection() method.")
//			.defaultValue(null)
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//			.build();
//
//	public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder()
//			.name("Password")
//			.description("The password for the database user. For applications using ComboPooledDataSource or any c3p0-implemented unpooled DataSources — DriverManagerDataSource or the DataSource returned by DataSources.unpooledDataSource( ... ) — defines the password that will be used for the DataSource's default getConnection() method.")
//			.defaultValue(null)
//			.required(false)
//			.sensitive(true)
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//			.build();
//
//	public static final PropertyDescriptor CHECKOUT_TIME_OUT = new PropertyDescriptor.Builder()
//			.name("Checkout Time Out")
//			.description("The number of milliseconds a client calling getConnection() will wait for a Connection to be checked-in or acquired when the pool is exhausted. "
//							+ "Zero means wait indefinitely. Setting any positive value will cause the getConnection() call to time-out and break with an SQLException after the specified number of milliseconds.")
//			.defaultValue("60000 millis")
//			.required(true)
//			.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
//			.sensitive(false)
//			.build();
//
//	public static final PropertyDescriptor MAX_POOL_SIZE = new PropertyDescriptor.Builder()
//			.name("Max Pool Size")
//			.description("Maximum number of Connections a pool will maintain at any given time. ")
//			.defaultValue("20")
//			.required(true)
//			.addValidator(StandardValidators.INTEGER_VALIDATOR)
//			.sensitive(false)
//			.build();
//
//	public static final PropertyDescriptor MIN_POOL_SIZE = new PropertyDescriptor.Builder()
//			.name("Min Pool Size")
//			.description(
//					"Minimum number of Connections a pool will maintain at any given time. ")
//			.defaultValue("3").required(true)
//			.addValidator(StandardValidators.INTEGER_VALIDATOR)
//			.sensitive(false).build();
//
//	public static final PropertyDescriptor MAX_CONNECTION_AGE = new PropertyDescriptor.Builder()
//			.name("Max Connection Age")
//			.description("Seconds, effectively a time to live. A Connection older than maxConnectionAge will be destroyed and purged from the pool. "
//							+ " This differs from maxIdleTime in that it refers to absolute age.  "
//							+ " Even a Connection which has not been much idle will be purged from the pool if it exceeds maxConnectionAge. "
//							+ " Zero means no maximum absolute age is enforced. ")
//			.defaultValue("0 millis")
//			.required(false)
//			.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
//			.sensitive(false)
//			.build();
//
//	public static final PropertyDescriptor MAX_IDLE_TIME = new PropertyDescriptor.Builder()
//			.name("Max Idle Time")
//			.description("Seconds a Connection can remain pooled but unused before being discarded. Zero means idle connections never expire. "
//							+ " Default time is 4000000 milliseconds, that is 1,11 hour. ")
//			.defaultValue("4000000 millis")
//			.required(false)
//			.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
//			.sensitive(false)
//			.build();
//
//	public static final PropertyDescriptor IDLE_CONNECTION_TEST_PERIOD = new PropertyDescriptor.Builder()
//			.name("Idle Connection Test Period")
//			.description("If this is a number greater than 0, c3p0 will test all idle, pooled but unchecked-out connections, every this number of seconds. Default:0 ")
//			.defaultValue("0 millis")
//			.required(false)
//			.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
//			.sensitive(false)
//			.build();
//
//	public static final PropertyDescriptor TEST_CONNECTION_ON_CHECKIN = new PropertyDescriptor.Builder()
//			.name("Test Connection on Checkin")
//			.description("If true, an operation will be performed asynchronously at every connection checkin to verify that the connection is valid. "
//							+ "Use in combination with idleConnectionTestPeriod for quite reliable, always asynchronous Connection testing. "
//							+ "Also, setting an automaticTestTable or preferredTestQuery will usually speed up all connection tests. ")
//			.defaultValue("false")
//			.required(false)
//			.addValidator(StandardValidators.BOOLEAN_VALIDATOR)
//			.sensitive(false)
//			.build();
//
//	public static final PropertyDescriptor TEST_CONNECTION_ON_CHECKOUT = new PropertyDescriptor.Builder()
//			.name("Test Connection on Checkout")
//			.description("If true, an operation will be performed at every connection checkout to verify that the connection is valid. "
//							+ "Be sure to set an efficient preferredTestQuery or automaticTestTable if you set this to true. "
//							+ "Performing the (expensive) default Connection test on every client checkout will harm client performance. "
//							+ "Testing Connections in checkout is the simplest and most reliable form of Connection testing, but for better performance, consider verifying connections periodically using idleConnectionTestPeriod. ")
//			.defaultValue("false")
//			.required(false)
//			.addValidator(StandardValidators.BOOLEAN_VALIDATOR)
//			.sensitive(false)
//			.build();
//
//	public static final PropertyDescriptor PREFERRED_TEST_QUERY = new PropertyDescriptor.Builder()
//			.name("Preferred Test Query")
//			.description("Defines the query that will be executed for all connection tests, if the default ConnectionTester (or some other implementation of QueryConnectionTester, or better yet FullQueryConnectionTester) is being used. "
//							+ "Defining a preferredTestQuery that will execute quickly in your database may dramatically speed up Connection tests. ")
//			.defaultValue(null)
//			.required(false)
//			.addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//			.build();
//
//	public static final PropertyDescriptor UNRETURNED_CONNECTION_TIMEOUT = new PropertyDescriptor.Builder()
//			.name("Unreturned Connection Timeout")
//			.description("Seconds. If set, if an application checks out but then fails to check-in [i.e. close()] a Connection within the specified period of time, the pool will unceremoniously destroy() the Connection. "
//							+ "This permits applications with occasional Connection leaks to survive, rather than eventually exhausting the Connection pool. And that's a shame. "
//							+ "Zero means no timeout, applications are expected to close() their own Connections. "
//							+ "Obviously, if a non-zero value is set, it should be to a value longer than any Connection should reasonably be checked-out. Otherwise, the pool will occasionally kill Connections in active use, which is bad. "
//							+ "This is basically a bad idea, but it's a commonly requested feature. Fix your $%!@% applications so they don't leak Connections! Use this temporarily in combination with debugUnreturnedConnectionStackTraces to figure out where Connections are being checked-out that don't make it back into the pool! ")
//			.defaultValue("0 millis")
//			.required(false)
//			.addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
//			.sensitive(false)
//			.build();
//
//	public static final PropertyDescriptor DEBUG_UNRETURNED_CONNECTION_STACK_TRACES = new PropertyDescriptor.Builder()
//			.name("Debug Unreturned Connection Stack Traces")
//			.description("If true, and if unreturnedConnectionTimeout is set to a positive value, then the pool will capture the stack trace (via an Exception) of all Connection checkouts, and the stack traces will be printed when unreturned checked-out Connections timeout. "
//							+ "This is intended to debug applications with Connection leaks, that is applications that occasionally fail to return Connections, leading to pool growth, and eventually exhaustion (when the pool hits maxPoolSize with all Connections checked-out and lost). "
//							+ "This parameter should only be set while debugging, as capturing the stack trace will slow down every Connection check-out. ")
//			.defaultValue("false")
//			.required(false)
//			.addValidator(StandardValidators.BOOLEAN_VALIDATOR)
//			.sensitive(false)
//			.build();
//
//	private static final List<PropertyDescriptor> properties;
//
//	static {
//		final List<PropertyDescriptor> props = new ArrayList<>();
//		props.add(JDBC_URL);
//		props.add(DRIVER_CLASS);
////		props.add(DB_DRIVER_JAR_URL);
//		props.add(USER);
//		props.add(PASSWORD);
//		props.add(CHECKOUT_TIME_OUT);
//		props.add(MAX_POOL_SIZE);
//		props.add(MIN_POOL_SIZE);
//		props.add(MAX_CONNECTION_AGE);
//		props.add(MAX_IDLE_TIME);
//		props.add(IDLE_CONNECTION_TEST_PERIOD);
//		props.add(TEST_CONNECTION_ON_CHECKIN);
//		props.add(TEST_CONNECTION_ON_CHECKOUT);
//		props.add(PREFERRED_TEST_QUERY);
//		props.add(UNRETURNED_CONNECTION_TIMEOUT);
//		props.add(DEBUG_UNRETURNED_CONNECTION_STACK_TRACES);
//
//		properties = Collections.unmodifiableList(props);
//	}
//
//	private ComboPooledDataSource dataSource;
//
//	@Override
//	protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
//		return properties;
//	}
//
//	/**
//	 * Configures connection pool by creating an instance of the
//	 * {@link ComboPooledDataSource} based on configuration provided with
//	 * {@link ConfigurationContext}.
//	 *
//	 * This operation makes no guarantees that the actual connection could be
//	 * made since the underlying system may still go off-line during normal
//	 * operation of the connection pool.
//	 *
//	 * @param context
//	 *            the configuration context
//	 * @throws InitializationException
//	 *             if unable to create a database connection
//	 * @throws PropertyVetoException
//	 */
//	@OnEnabled
//	public void onConfigured(final ConfigurationContext context)
//			throws InitializationException, PropertyVetoException {
//
//		// --------------Basic Configuration------------------------------
//		final String jdbcUrl = context.getProperty(JDBC_URL).getValue();
//		final String driverClass = context.getProperty(DRIVER_CLASS).getValue();
//		// Optional driver URL, when exist, this URL will be used to locate
//		// driver jar file location
////		final String dbDriverJarUrl = context.getProperty(DB_DRIVER_JAR_URL).getValue();
//		final String user = context.getProperty(USER).getValue();
//		final String password = context.getProperty(PASSWORD).getValue();
//		final Long checkoutTimeOut = context.getProperty(CHECKOUT_TIME_OUT).asTimePeriod(TimeUnit.MILLISECONDS);
//		final Integer maxPoolSize = context.getProperty(MAX_POOL_SIZE).asInteger();
//		final Integer minPoolSize = context.getProperty(MIN_POOL_SIZE).asInteger();
//
//		// ---------Test Idle Connection Configuration-----------------------
//		final Long maxConnectionAge = context.getProperty(MAX_CONNECTION_AGE).asTimePeriod(TimeUnit.MILLISECONDS);
//		final Long maxIdleTime = context.getProperty(MAX_IDLE_TIME).asTimePeriod(TimeUnit.MILLISECONDS);
//		final Long idleConnectionTestPeriod = context.getProperty(IDLE_CONNECTION_TEST_PERIOD).asTimePeriod(TimeUnit.MILLISECONDS);
//		final Boolean testConnectionOnCheckin = context.getProperty(TEST_CONNECTION_ON_CHECKIN).asBoolean();
//		final Boolean testConnectionOnCheckout = context.getProperty(TEST_CONNECTION_ON_CHECKOUT).asBoolean();
//		final String preferredTestQuery = context.getProperty(PREFERRED_TEST_QUERY).getValue();
//
//		// ----Configuring to Debug and Workaround Broken Client
//		// Applications----------------------
//		final Long unreturnedConnectionTimeout = context.getProperty(UNRETURNED_CONNECTION_TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS);
//		final Boolean debugUnreturnedConnectionStackTraces = context.getProperty(DEBUG_UNRETURNED_CONNECTION_STACK_TRACES).asBoolean();
//
//		dataSource = new ComboPooledDataSource();
//
//		// Basic Configuration
//		// dataSource.setDriverClass(getDriverClassLoader(jdbcUrl,
//		// driverClass).toString());
//		dataSource.setJdbcUrl(jdbcUrl);
//		dataSource.setDriverClass(driverClass);
//
//		dataSource.setUser(user);
//		dataSource.setPassword(password);
//		dataSource.setCheckoutTimeout((checkoutTimeOut).intValue());
//		dataSource.setMaxPoolSize(maxPoolSize);
//		dataSource.setMinPoolSize(minPoolSize);
//
//		// Test Idle Connections Configuration
//		dataSource.setMaxConnectionAge((maxConnectionAge).intValue());
//		dataSource.setMaxIdleTime((maxIdleTime).intValue());
//		dataSource.setIdleConnectionTestPeriod((idleConnectionTestPeriod)
//				.intValue());
//		dataSource.setTestConnectionOnCheckin(testConnectionOnCheckin);
//		dataSource.setTestConnectionOnCheckout(testConnectionOnCheckout);
//		dataSource.setPreferredTestQuery(preferredTestQuery);
//
//		// Configuring to Debug and Workaround Broken Client Applications
//		dataSource.setUnreturnedConnectionTimeout((unreturnedConnectionTimeout).intValue());
//		dataSource.setDebugUnreturnedConnectionStackTraces(debugUnreturnedConnectionStackTraces);
//
//	}
//
//	/**
//	 * using Thread.currentThread().getContextClassLoader(); will ensure that
//	 * you are using the ClassLoader for you NAR.
//	 *
//	 * @throws InitializationException
//	 *             if there is a problem obtaining the ClassLoader
//	 */
//	protected ClassLoader getDriverClassLoader(String urlString, String drvName)
//			throws InitializationException {
//		if (urlString != null && urlString.length() > 0) {
//			try {
//				final URL[] urls = new URL[] { new URL(urlString) };
//				final URLClassLoader ucl = new URLClassLoader(urls);
//
//				// Workaround which allows to use URLClassLoader for JDBC driver
//				// loading.
//				// (Because the DriverManager will refuse to use a driver not
//				// loaded by the system ClassLoader.)
//				final Class<?> clazz = Class.forName(drvName, true, ucl);
//				if (clazz == null) {
//					throw new InitializationException("Can't load Database Driver " + drvName);
//				}
//				final Driver driver = (Driver) clazz.newInstance();
//				DriverManager.registerDriver(new DriverShim(driver));
//
//				return ucl;
//			} catch (final MalformedURLException e) {
//				throw new InitializationException("Invalid Database Driver Jar Url", e);
//			} catch (final Exception e) {
//				throw new InitializationException("Can't load Database Driver", e);
//			}
//		} else {
//			// That will ensure that you are using the ClassLoader for you NAR.
//			return Thread.currentThread().getContextClassLoader();
//		}
//	}
//
//	/**
//	 * Shutdown pool, close all open connections.
//	 */
//	@OnDisabled
//	public void shutdown() throws SQLException {
//		dataSource.close();
//	}
//
//
//	public Connection getConnection() throws ProcessException {
//		try {
//			final Connection con = dataSource.getConnection();
//			return con;
//		} catch (final SQLException e) {
//			throw new ProcessException(e);
//		}
//	}
//
//	@Override
//	public String toString() {
//		return "WaveDBCPConnectionPool[id=" + getIdentifier() + "]";
//	}
//
//}
