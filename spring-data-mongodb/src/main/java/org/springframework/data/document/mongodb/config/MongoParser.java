/*
 * Copyright 2011 the original author or authors.
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

package org.springframework.data.document.mongodb.config;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.document.mongodb.MongoFactoryBean;
import org.springframework.data.document.mongodb.MongoOptionsFactoryBean;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * Parser for &lt;mongo;gt; definitions. If no name
 *
 * @author Mark Pollack
 */
public class MongoParser extends AbstractSingleBeanDefinitionParser {

	protected Class<?> getBeanClass(Element element) {
		return MongoFactoryBean.class;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {
		super.doParse(element, builder);

		setPropertyValue(element, builder, "port", "port");
		setPropertyValue(element, builder, "host", "host");

		parseOptions(parserContext, element, builder);

	}

	/**
	 * Parses the options sub-element. Populates the given attribute factory with the proper attributes.
	 *
	 * @param element
	 * @param attrBuilder
	 * @return true if parsing actually occured, false otherwise
	 */
	boolean parseOptions(ParserContext parserContext, Element element, BeanDefinitionBuilder mongoBuilder) {
		Element optionsElement = DomUtils.getChildElementByTagName(element, "options");
		if (optionsElement == null)
			return false;

		BeanDefinitionBuilder optionsDefBuilder = BeanDefinitionBuilder
				.genericBeanDefinition(MongoOptionsFactoryBean.class);

		setPropertyValue(optionsElement, optionsDefBuilder, "connections-per-host", "connectionsPerHost");
		setPropertyValue(optionsElement, optionsDefBuilder, "threads-allowed-to-block-for-connection-multiplier",
				"threadsAllowedToBlockForConnectionMultiplier");
		setPropertyValue(optionsElement, optionsDefBuilder, "max-wait-time", "maxWaitTime");
		setPropertyValue(optionsElement, optionsDefBuilder, "connect-timeout", "connectTimeout");
		setPropertyValue(optionsElement, optionsDefBuilder, "socket-timeout", "socketTimeout");
		setPropertyValue(optionsElement, optionsDefBuilder, "socket-keep-alive", "socketKeepAlive");		
		setPropertyValue(optionsElement, optionsDefBuilder, "auto-connect-retry", "autoConnectRetry");
    setPropertyValue(optionsElement, optionsDefBuilder, "write-number", "writeNumber");		
		setPropertyValue(optionsElement, optionsDefBuilder, "write-timeout", "writeTimeout");
		setPropertyValue(optionsElement, optionsDefBuilder, "write-fsync", "writeFsync");
		setPropertyValue(optionsElement, optionsDefBuilder, "slave-ok", "slaveOk");		
		
		

		mongoBuilder.addPropertyValue("mongoOptions", optionsDefBuilder.getBeanDefinition());
		return true;
	}

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {
		String name = super.resolveId(element, definition, parserContext);
		if (!StringUtils.hasText(name)) {
			name = "mongo";
		}
		return name;
	}

	private void setPropertyValue(Element element, BeanDefinitionBuilder builder, String attrName, String propertyName) {
		String attr = element.getAttribute(attrName);
		if (StringUtils.hasText(attr)) {
			builder.addPropertyValue(propertyName, attr);
		}
	}
	
}