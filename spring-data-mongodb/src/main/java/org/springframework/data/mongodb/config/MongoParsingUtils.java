/*
 * Copyright 2011-2013 the original author or authors.
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
import org.springframework.data.mongodb.core.MongoOptionsFactoryBean;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * Utility methods for {@link BeanDefinitionParser} implementations for MongoDB.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
abstract class MongoParsingUtils {

	private MongoParsingUtils() {

	}

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
	 * Parses the mongo:options sub-element. Populates the given attribute factory with the proper attributes.
	 * 
	 * @return true if parsing actually occured, false otherwise
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
		setPropertyValue(optionsDefBuilder, optionsElement, "write-number", "writeNumber");
		setPropertyValue(optionsDefBuilder, optionsElement, "write-timeout", "writeTimeout");
		setPropertyValue(optionsDefBuilder, optionsElement, "write-fsync", "writeFsync");
		setPropertyValue(optionsDefBuilder, optionsElement, "ssl", "ssl");
		setPropertyReference(optionsDefBuilder, optionsElement, "ssl-socket-factory-ref", "sslSocketFactory");

		mongoBuilder.addPropertyValue("mongoOptions", optionsDefBuilder.getBeanDefinition());
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
}
