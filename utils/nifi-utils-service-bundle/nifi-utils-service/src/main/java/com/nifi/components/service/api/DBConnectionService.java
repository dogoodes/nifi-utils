//package com.nifi.components.service.api;
//
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
//import java.util.Properties;
//
//@Tags({"dbconnection", "connection", "database",  "wave"})
//@CapabilityDescription("Provide a database connection")
//public class DBConnectionService extends AbstractControllerService implements DBCPService {
//
//	  public static final PropertyDescriptor DATABASE_URL = new PropertyDescriptor.Builder()
//              .name("Database Connection URL")
//		        .description("A database connection URL used to connect to a database. May contain database system name, host, port, database name and some parameters."
//		            + " The exact syntax of a database connection URL is specified by your DBMS.")
//		        .defaultValue(null)
//		        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//		        .required(true)
//		        .build();
//
//		    public static final PropertyDescriptor DB_DRIVERNAME = new PropertyDescriptor.Builder()
//		        .name("Database Driver Class Name")
//		        .description("Database driver class name")
//		        .defaultValue(null)
//		        .required(true)
//		        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//		        .build();
//
//		    public static final PropertyDescriptor DB_DRIVER_JAR_URL = new PropertyDescriptor.Builder()
//		        .name("Database Driver Jar Url")
//		        .description("Optional database driver jar file path url. For example 'file:///var/tmp/mariadb-java-client-1.1.7.jar'")
//		        .defaultValue(null)
//		        .required(false)
//		        .addValidator(StandardValidators.URL_VALIDATOR)
//		        .build();
//
//		    public static final PropertyDescriptor DB_USER = new PropertyDescriptor.Builder()
//		        .name("Database User")
//		        .description("Database user name")
//		        .defaultValue(null)
//		        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//		        .build();
//
//		    public static final PropertyDescriptor DB_PASSWORD = new PropertyDescriptor.Builder()
//		        .name("Password")
//		        .description("The password for the database user")
//		        .defaultValue(null)
//		        .required(false)
//		        .sensitive(true)
//		        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
//		        .build();
//
//	private static final List<PropertyDescriptor> properties;
//
//    static {
//        final List<PropertyDescriptor> props = new ArrayList<>();
//        props.add(DATABASE_URL);
//        props.add(DB_DRIVERNAME);
//        props.add(DB_DRIVER_JAR_URL);
//        props.add(DB_USER);
//        props.add(DB_PASSWORD);
//        properties = Collections.unmodifiableList(props);
//    }
//
//    private String drv = null;
//    private String user = null;
//    private String passw = null;
//    private String urlString = null;
//    private String dbUrl = null;
//
//
//    @Override
//    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
//        return properties;
//    }
//
//    @OnEnabled
//    public void onConfigured(final ConfigurationContext context) throws InitializationException {
//    	 this.drv = context.getProperty(DB_DRIVERNAME).getValue();
//         this.user = context.getProperty(DB_USER).getValue();
//         this.passw = context.getProperty(DB_PASSWORD).getValue();
//         this.urlString = context.getProperty(DB_DRIVER_JAR_URL).getValue();
//         this.dbUrl = context.getProperty(DATABASE_URL).getValue();
//    }
//
//    @OnDisabled
//    public void shutdown() {
//    	this.drv = null;
//        this.user = null;
//        this.passw = null;
//        this.urlString = null;
//        this.dbUrl = null;
//    }
//
//    @Override
//    public Connection getConnection() throws ProcessException {
//        try {
//        	ClassLoader driverManagerCL = getDriverClassLoader(urlString, drv);
//
//            final Connection con = createConnection(driverManagerCL);
//            return con;
//        } catch (final SQLException e) {
//            throw new ProcessException(e);
//        } catch(InitializationException e){
//        	throw new ProcessException(e);
//        }
//    }
//
//    /**
//     * Create a JDBC Connection
//     */
//    protected Connection createConnection(ClassLoader driverManagerCL) throws SQLException {
//        // Load the JDBC driver class
//        Class driverFromCCL = null;
//        if (this.drv != null) {
//            try {
//                try {
//                    if (driverManagerCL == null) {
//                        Class.forName(this.drv);
//                    } else {
//                        Class.forName(this.drv, true, driverManagerCL);
//                    }
//                } catch (ClassNotFoundException cnfe) {
//                    driverFromCCL = Thread.currentThread(
//                            ).getContextClassLoader().loadClass(
//                                    this.drv);
//                }
//            } catch (Throwable t) {
//                String message = "Cannot load JDBC driver class '" +
//                    this.drv + "'";
//                t.printStackTrace();
//                throw new SQLException(message, t);
//            }
//        }
//
//        // Create a JDBC driver instance
//        Driver driver = null;
//        try {
//            if (driverFromCCL == null) {
//                driver = DriverManager.getDriver(this.dbUrl);
//            } else {
//                // Usage of DriverManager is not possible, as it does not
//                // respect the ContextClassLoader
//                driver = (Driver) driverFromCCL.newInstance();
//                if (!driver.acceptsURL(this.urlString)) {
//                    throw new SQLException("No suitable driver", "08001");
//                }
//            }
//        } catch (Throwable t) {
//            String message = "Cannot create JDBC driver of class '" +
//                (this.drv != null ? this.drv : "") +
//                "' for connect URL '" + this.urlString + "'";
//            t.printStackTrace();
//            throw new SQLException(message, t);
//        }
//
//
//        // Set up the driver connection factory we will use
//
//        Properties connectionProperties = new Properties();
//        if (this.user != null) {
//            connectionProperties.put("user", this.user);
//        } else {
//            throw new SQLException("DBConnection configured without a 'username'");
//        }
//
//        if (this.passw != null) {
//            connectionProperties.put("password", this.passw);
//        } else {
//        	throw new SQLException("DBCP DataSource configured without a 'password'");
//        }
//        return driver.connect(this.dbUrl, connectionProperties);
//    }
//
//
//    /**
//     * using Thread.currentThread().getContextClassLoader(); will ensure that you are using the ClassLoader for you NAR.
//     *
//     * @throws InitializationException
//     *             if there is a problem obtaining the ClassLoader
//     */
//    protected ClassLoader getDriverClassLoader(String urlString, String drvName) throws InitializationException {
//        if (urlString != null && urlString.length() > 0) {
//            try {
//                final URL[] urls = new URL[] { new URL(urlString) };
//                final URLClassLoader ucl = new URLClassLoader(urls);
//
//                // Workaround which allows to use URLClassLoader for JDBC driver loading.
//                // (Because the DriverManager will refuse to use a driver not loaded by the system ClassLoader.)
//                final Class<?> clazz = Class.forName(drvName, true, ucl);
//                if (clazz == null) {
//                    throw new InitializationException("Can't load Database Driver " + drvName);
//                }
//                final Driver driver = (Driver) clazz.newInstance();
//                DriverManager.registerDriver(new DriverShim(driver));
//
//                return ucl;
//            } catch (final MalformedURLException e) {
//                throw new InitializationException("Invalid Database Driver Jar Url", e);
//            } catch (final Exception e) {
//                throw new InitializationException("Can't load Database Driver", e);
//            }
//        } else {
//            // That will ensure that you are using the ClassLoader for you NAR.
//            return Thread.currentThread().getContextClassLoader();
//        }
//    }
//
//    @Override
//    public String toString() {
//        return "DBConnectionPool[id=" + getIdentifier() + "]";
//    }
//
//
//}