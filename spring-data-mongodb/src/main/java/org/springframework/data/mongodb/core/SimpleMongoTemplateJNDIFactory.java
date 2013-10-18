package org.springframework.data.mongodb.core;

import java.util.Enumeration;
import java.util.Hashtable;

import javax.naming.Context;
import javax.naming.Name;
import javax.naming.NamingException;
import javax.naming.RefAddr;
import javax.naming.Reference;
import javax.naming.spi.ObjectFactory;

import org.springframework.context.annotation.Bean;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Component;

import com.mongodb.Mongo;

/**
 * Factory to create {@link MongoTemplate} instances from a JNDI Resource.
 * 
 * @author Gopalakrishnan P
 * @author Thirumaleshwar K
 */
public class SimpleMongoTemplateJNDIFactory implements ObjectFactory {

	private final static String PROP_PASSWORD = "password";
	private final static String PROP_HOST = "host";
	private final static String PROP_USERNAME = "username";
	private final static String PROP_PORT = "port";
	private final static String PROP_DBNAME = "db";

	private MongoTemplate mongoTemplate;
	private String dataBaseName;
	private String host;
	private String username ;
	private String password;
	private int port = 27017;


	public Object getObjectInstance(Object obj, Name name, Context nameCtx,
			Hashtable<?, ?> environment) throws Exception {

		Object result = null;

		validateProperty(obj, "Invalid JNDI object reference");

		Reference ref = (Reference) obj;
		Enumeration<RefAddr> props = ref.getAll();
		while (props.hasMoreElements()) {
			RefAddr addr = (RefAddr) props.nextElement();
			String propName = addr.getType();
			String propValue = (String) addr.getContent();
			if (propName.equals(PROP_DBNAME)) {
				dataBaseName = propValue;
			} else if (propName.equals(PROP_HOST)) {
				host = propValue;
			} else if (propName.equals(PROP_USERNAME)) {
				username = propValue;
			} else if (propName.equals(PROP_PASSWORD)) {
				password = propValue;
			} else if (propName.equals(PROP_PORT)) {
				try {
					port = Integer.parseInt(propValue);
				} catch (NumberFormatException e) {
					throw new NamingException("Invalid port value " + propValue);
				}
			}
		}

		// validate properties
		validateProperty(dataBaseName, "Invalid or empty mongo database name");
		validateProperty(host, "Invalid or empty mongo host");
		validateProperty(username, "Invalid or empty mongo username");
		validateProperty(password, "Invalid or empty mongo password");

		this.mongoTemplate = new MongoTemplate(new Mongo(host, port), dataBaseName,
				new UserCredentials(username, password));

		return this.mongoTemplate;
	}

	/**
	 * Validate internal String properties
	 * 
	 * @param property
	 * @param errorMessage
	 * @throws NamingException
	 */
	private void validateProperty(String property, String errorMessage)
			throws NamingException {
		if (property == null || property.trim().equals("")) {
			throw new NamingException(errorMessage);
		}
	}

	/**
	 * Validate internal Object properties
	 * 
	 * @param property
	 * @param errorMessage
	 * @throws NamingException
	 */
	private void validateProperty(Object property, String errorMessage)
			throws NamingException {
		if (property == null) {
			throw new NamingException(errorMessage);
		}
	}
}