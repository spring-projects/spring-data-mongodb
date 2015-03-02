/*
 * Copyright 2011-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.config;

import static org.springframework.data.config.ParsingUtils.*;

import java.util.Map;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.CustomEditorConfigurer;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.data.mongodb.core.MongoClientOptionsFactoryBean;
import org.springframework.data.mongodb.core.MongoOptionsFactoryBean;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * Utility methods for {@link BeanDefinitionParser} implementations for MongoDB.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
@SuppressWarnings("deprecation")
abstract class MongoParsingUtils {

	private MongoParsingUtils() {}

	/**
	 * Parses the mongo replica-set element.
	 * 
	 * @param parserContext the parser context
	 * @param element the mongo element
	 * @param mongoBuilder the bean definition builder to populate
	 * @return
	 */
	static void parseReplicaSet(Element element, BeanDefinitionBuilder mongoBuilder) {
		setPropertyValue(mongoBuilder, element, "replica-set", "replicaSetSeeds");
	}

	/**
	 * Parses the {@code mongo:options} sub-element. Populates the given attribute factory with the proper attributes.
	 * 
	 * @return true if parsing actually occured, {@literal false} otherwise
	 */
	static boolean parseMongoOptions(Element element, BeanDefinitionBuilder mongoBuilder) {

		Element optionsElement = DomUtils.getChildElementByTagName(element, "options");

		if (optionsElement == null) {
			return false;
		}

		BeanDefinitionBuilder optionsDefBuilder = BeanDefinitionBuilder
				.genericBeanDefinition(MongoOptionsFactoryBean.class);

		setPropertyValue(optionsDefBuilder, optionsElement, "connections-per-host", "connectionsPerHost");
		setPropertyValue(optionsDefBuilder, optionsElement, "threads-allowed-to-block-for-connection-multiplier",
				"threadsAllowedToBlockForConnectionMultiplier");
		setPropertyValue(optionsDefBuilder, optionsElement, "max-wait-time", "maxWaitTime");
		setPropertyValue(optionsDefBuilder, optionsElement, "connect-timeout", "connectTimeout");
		setPropertyValue(optionsDefBuilder, optionsElement, "socket-timeout", "socketTimeout");
		setPropertyValue(optionsDefBuilder, optionsElement, "socket-keep-alive", "socketKeepAlive");
		setPropertyValue(optionsDefBuilder, optionsElement, "auto-connect-retry", "autoConnectRetry");
		setPropertyValue(optionsDefBuilder, optionsElement, "max-auto-connect-retry-time", "maxAutoConnectRetryTime");
		setPropertyValue(optionsDefBuilder, optionsElement, "write-number", "writeNumber");
		setPropertyValue(optionsDefBuilder, optionsElement, "write-timeout", "writeTimeout");
		setPropertyValue(optionsDefBuilder, optionsElement, "write-fsync", "writeFsync");
		setPropertyValue(optionsDefBuilder, optionsElement, "slave-ok", "slaveOk");
		setPropertyValue(optionsDefBuilder, optionsElement, "ssl", "ssl");
		setPropertyReference(optionsDefBuilder, optionsElement, "ssl-socket-factory-ref", "sslSocketFactory");

		mongoBuilder.addPropertyValue("mongoOptions", optionsDefBuilder.getBeanDefinition());
		return true;
	}

	/**
	 * Parses the {@code mongo:client-options} sub-element. Populates the given attribute factory with the proper
	 * attributes.
	 * 
	 * @param element must not be {@literal null}.
	 * @param mongoClientBuilder must not be {@literal null}.
	 * @return
	 * @since 1.7
	 */
	public static boolean parseMongoClientOptions(Element element, BeanDefinitionBuilder mongoClientBuilder) {

		Element optionsElement = DomUtils.getChildElementByTagName(element, "client-options");

		if (optionsElement == null) {
			return false;
		}

		BeanDefinitionBuilder clientOptionsDefBuilder = BeanDefinitionBuilder
				.genericBeanDefinition(MongoClientOptionsFactoryBean.class);

		setPropertyValue(clientOptionsDefBuilder, optionsElement, "description", "description");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "min-connections-per-host", "minConnectionsPerHost");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "connections-per-host", "connectionsPerHost");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "threads-allowed-to-block-for-connection-multiplier",
				"threadsAllowedToBlockForConnectionMultiplier");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "max-wait-time", "maxWaitTime");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "max-connection-idle-time", "maxConnectionIdleTime");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "max-connection-life-time", "maxConnectionLifeTime");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "connect-timeout", "connectTimeout");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "socket-timeout", "socketTimeout");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "socket-keep-alive", "socketKeepAlive");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "read-preference", "readPreference");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "write-concern", "writeConcern");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "heartbeat-frequency", "heartbeatFrequency");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "min-heartbeat-frequency", "minHeartbeatFrequency");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "heartbeat-connect-timeout", "heartbeatConnectTimeout");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "heartbeat-socket-timeout", "heartbeatSocketTimeout");
		setPropertyValue(clientOptionsDefBuilder, optionsElement, "ssl", "ssl");
		setPropertyReference(clientOptionsDefBuilder, optionsElement, "ssl-socket-factory-ref", "sslSocketFactory");

		mongoClientBuilder.addPropertyValue("mongoClientOptions", clientOptionsDefBuilder.getBeanDefinition());

		return true;
	}

	/**
	 * Returns the {@link BeanDefinitionBuilder} to build a {@link BeanDefinition} for a
	 * {@link WriteConcernPropertyEditor}.
	 * 
	 * @return
	 */
	static BeanDefinitionBuilder getWriteConcernPropertyEditorBuilder() {

		Map<String, Class<?>> customEditors = new ManagedMap<String, Class<?>>();
		customEditors.put("com.mongodb.WriteConcern", WriteConcernPropertyEditor.class);

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(CustomEditorConfigurer.class);
		builder.addPropertyValue("customEditors", customEditors);

		return builder;
	}

	/**
	 * One should only register one bean definition but want to have the convenience of using
	 * AbstractSingleBeanDefinitionParser but have the side effect of registering a 'default' property editor with the
	 * container.
	 */
	static BeanDefinitionBuilder getServerAddressPropertyEditorBuilder() {

		Map<String, String> customEditors = new ManagedMap<String, String>();
		customEditors.put("com.mongodb.ServerAddress[]",
				"org.springframework.data.mongodb.config.ServerAddressPropertyEditor");

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(CustomEditorConfigurer.class);
		builder.addPropertyValue("customEditors", customEditors);
		return builder;
	}

	/**
	 * Returns the {@link BeanDefinitionBuilder} to build a {@link BeanDefinition} for a
	 * {@link ReadPreferencePropertyEditor}.
	 * 
	 * @return
	 * @since 1.7
	 */
	static BeanDefinitionBuilder getReadPreferencePropertyEditorBuilder() {

		Map<String, Class<?>> customEditors = new ManagedMap<String, Class<?>>();
		customEditors.put("com.mongodb.ReadPreference", ReadPreferencePropertyEditor.class);

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(CustomEditorConfigurer.class);
		builder.addPropertyValue("customEditors", customEditors);

		return builder;
	}

	/**
	 * Returns the {@link BeanDefinitionBuilder} to build a {@link BeanDefinition} for a
	 * {@link MongoCredentialPropertyEditor}.
	 * 
	 * @return
	 * @since 1.7
	 */
	static BeanDefinitionBuilder getMongoCredentialPropertyEditor() {

		Map<String, Class<?>> customEditors = new ManagedMap<String, Class<?>>();
		customEditors.put("com.mongodb.MongoCredential[]", MongoCredentialPropertyEditor.class);

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(CustomEditorConfigurer.class);
		builder.addPropertyValue("customEditors", customEditors);

		return builder;
	}
}
