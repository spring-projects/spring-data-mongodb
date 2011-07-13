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
package org.springframework.data.mongodb.config;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.parsing.CompositeComponentDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.mongodb.core.MongoAdmin;
import org.springframework.data.mongodb.monitor.*;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

public class MongoJmxParser implements BeanDefinitionParser {

	public BeanDefinition parse(Element element, ParserContext parserContext) {
		String name = element.getAttribute("mongo-ref");
		if (!StringUtils.hasText(name)) {
			name = "mongo";
		}
		registerJmxComponents(name, element, parserContext);
		return null;
	}

	protected void registerJmxComponents(String mongoRefName, Element element, ParserContext parserContext) {
		Object eleSource = parserContext.extractSource(element);

		CompositeComponentDefinition compositeDef = new CompositeComponentDefinition(element.getTagName(), eleSource);

		createBeanDefEntry(AssertMetrics.class, compositeDef, mongoRefName, eleSource, parserContext);
		createBeanDefEntry(BackgroundFlushingMetrics.class, compositeDef, mongoRefName, eleSource, parserContext);
		createBeanDefEntry(BtreeIndexCounters.class, compositeDef, mongoRefName, eleSource, parserContext);
		createBeanDefEntry(ConnectionMetrics.class, compositeDef, mongoRefName, eleSource, parserContext);
		createBeanDefEntry(GlobalLockMetrics.class, compositeDef, mongoRefName, eleSource, parserContext);
		createBeanDefEntry(MemoryMetrics.class, compositeDef, mongoRefName, eleSource, parserContext);
		createBeanDefEntry(OperationCounters.class, compositeDef, mongoRefName, eleSource, parserContext);
		createBeanDefEntry(ServerInfo.class, compositeDef, mongoRefName, eleSource, parserContext);
		createBeanDefEntry(MongoAdmin.class, compositeDef, mongoRefName, eleSource, parserContext);

		parserContext.registerComponent(compositeDef);

	}

	protected void createBeanDefEntry(Class<?> clazz, CompositeComponentDefinition compositeDef, String mongoRefName,
			Object eleSource, ParserContext parserContext) {
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(clazz);
		builder.getRawBeanDefinition().setSource(eleSource);
		builder.addConstructorArgReference(mongoRefName);
		BeanDefinition assertDef = builder.getBeanDefinition();
		String assertName = parserContext.getReaderContext().registerWithGeneratedName(assertDef);
		compositeDef.addNestedComponent(new BeanComponentDefinition(assertDef, assertName));
	}

}
