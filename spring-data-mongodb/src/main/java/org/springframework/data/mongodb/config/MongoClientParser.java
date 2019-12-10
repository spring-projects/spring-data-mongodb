/*
 * Copyright 2019 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
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
import org.springframework.data.config.BeanComponentDefinitionBuilder;
import org.springframework.data.config.ParsingUtils;
import org.springframework.data.mongodb.core.MongoClientFactoryBean;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * Parser for {@code mongo-client} definitions.
 *
 * @author Christoph Strobl
 * @since 1.7
 */
public class MongoClientParser implements BeanDefinitionParser {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.BeanDefinitionParser#parse(org.w3c.dom.Element, org.springframework.beans.factory.xml.ParserContext)
	 */
	public BeanDefinition parse(Element element, ParserContext parserContext) {

		Object source = parserContext.extractSource(element);
		String id = element.getAttribute("id");

		BeanComponentDefinitionBuilder helper = new BeanComponentDefinitionBuilder(element, parserContext);

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(MongoClientFactoryBean.class);

		ParsingUtils.setPropertyValue(builder, element, "port", "port");
		ParsingUtils.setPropertyValue(builder, element, "host", "host");
		ParsingUtils.setPropertyValue(builder, element, "credential", "credential");
		ParsingUtils.setPropertyValue(builder, element, "replica-set", "replicaSet");
		ParsingUtils.setPropertyValue(builder, element, "connection-string", "connectionString");

		MongoParsingUtils.parseMongoClientSettings(element, builder);

		String defaultedId = StringUtils.hasText(id) ? id : BeanNames.MONGO_BEAN_NAME;

		parserContext.pushContainingComponent(new CompositeComponentDefinition("Mongo", source));

		BeanComponentDefinition mongoComponent = helper.getComponent(builder, defaultedId);
		parserContext.registerBeanComponent(mongoComponent);

		BeanComponentDefinition connectionStringPropertyEditor = helper
				.getComponent(MongoParsingUtils.getConnectionStringPropertyEditorBuilder());
		parserContext.registerBeanComponent(connectionStringPropertyEditor);

		BeanComponentDefinition serverAddressPropertyEditor = helper
				.getComponent(MongoParsingUtils.getServerAddressPropertyEditorBuilder());
		parserContext.registerBeanComponent(serverAddressPropertyEditor);

		BeanComponentDefinition writeConcernEditor = helper
				.getComponent(MongoParsingUtils.getWriteConcernPropertyEditorBuilder());
		parserContext.registerBeanComponent(writeConcernEditor);

		BeanComponentDefinition readConcernEditor = helper
				.getComponent(MongoParsingUtils.getReadConcernPropertyEditorBuilder());
		parserContext.registerBeanComponent(readConcernEditor);

		BeanComponentDefinition readPreferenceEditor = helper
				.getComponent(MongoParsingUtils.getReadPreferencePropertyEditorBuilder());
		parserContext.registerBeanComponent(readPreferenceEditor);

		BeanComponentDefinition credentialsEditor = helper
				.getComponent(MongoParsingUtils.getMongoCredentialPropertyEditor());
		parserContext.registerBeanComponent(credentialsEditor);

		parserContext.popAndRegisterContainingComponent();

		return mongoComponent.getBeanDefinition();
	}
}
