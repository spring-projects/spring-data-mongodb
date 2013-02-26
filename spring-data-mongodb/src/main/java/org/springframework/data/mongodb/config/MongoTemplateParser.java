/*
 * Copyright 2011-2012 the original author or authors.
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

import static org.springframework.data.config.ParsingUtils.setPropertyValue;
import static org.springframework.data.mongodb.config.MongoParsingUtils.getWriteConcernPropertyEditorBuilder;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.config.BeanComponentDefinitionBuilder;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * {@link BeanDefinitionParser} to parse {@code template} elements into {@link BeanDefinition}s.
 * 
 * @author Martin Baumgartner
 */
public class MongoTemplateParser extends AbstractBeanDefinitionParser {

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractBeanDefinitionParser#resolveId(org.w3c.dom.Element, org.springframework.beans.factory.support.AbstractBeanDefinition, org.springframework.beans.factory.xml.ParserContext)
	 */
	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : BeanNames.MONGO_TEMPLATE;
	}
	// Supports the MongoTemplate(MongoDbFactory mongoDbFactory) and MongoTemplate(MongoDbFactory mongoDbFactory, MongoConverter mongoConverter) Constructors
	// TODO Are other constructors useful? ( MongoTemplate(Mongo mongo, String databaseName) and MongoTemplate(Mongo mongo, String databaseName, UserCredentials userCredentials)
	@Override
	protected AbstractBeanDefinition parseInternal(Element element,
			ParserContext parserContext) {

		BeanComponentDefinitionBuilder helper = new BeanComponentDefinitionBuilder(element, parserContext);

		String converterRef = element.getAttribute("converter-ref");
		String dbFactoryRef = element.getAttribute("db-factory-ref");

		// Common setup
		BeanDefinitionBuilder mongoTemplateBuilder = BeanDefinitionBuilder.genericBeanDefinition(MongoTemplate.class);

		// Defaulting
		if (StringUtils.hasText(dbFactoryRef)) {
			mongoTemplateBuilder.addConstructorArgReference(dbFactoryRef);
		} 
		else {
			mongoTemplateBuilder.addConstructorArgReference(BeanNames.DB_FACTORY);
		}

		if (StringUtils.hasText(converterRef)) {
			mongoTemplateBuilder.addConstructorArgReference(converterRef);
		}

		setPropertyValue(mongoTemplateBuilder, element, "write-concern", "writeConcern");

		BeanDefinitionBuilder writeConcernPropertyEditorBuilder = getWriteConcernPropertyEditorBuilder();

		BeanComponentDefinition component = helper.getComponent(writeConcernPropertyEditorBuilder);
		parserContext.registerBeanComponent(component);

		return (AbstractBeanDefinition) helper.getComponentIdButFallback(mongoTemplateBuilder, BeanNames.MONGO_TEMPLATE)
				.getBeanDefinition();
	}

}