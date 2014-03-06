/*
 * Copyright 2011-2014 the original author or authors.
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

import java.util.Map;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.CustomEditorConfigurer;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.parsing.CompositeComponentDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.config.BeanComponentDefinitionBuilder;
import org.springframework.data.config.ParsingUtils;
import org.springframework.data.mongodb.core.MongoFactoryBean;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * Parser for &lt;mongo;gt; definitions.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public class MongoParser implements BeanDefinitionParser {

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.BeanDefinitionParser#parse(org.w3c.dom.Element, org.springframework.beans.factory.xml.ParserContext)
	 */
	public BeanDefinition parse(Element element, ParserContext parserContext) {

		Object source = parserContext.extractSource(element);
		String id = element.getAttribute("id");

		BeanComponentDefinitionBuilder helper = new BeanComponentDefinitionBuilder(element, parserContext);

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(MongoFactoryBean.class);
		ParsingUtils.setPropertyValue(builder, element, "port", "port");
		ParsingUtils.setPropertyValue(builder, element, "host", "host");
		ParsingUtils.setPropertyValue(builder, element, "write-concern", "writeConcern");

		MongoParsingUtils.parseMongoOptions(element, builder);
		MongoParsingUtils.parseReplicaSet(element, builder);

		String defaultedId = StringUtils.hasText(id) ? id : BeanNames.MONGO_BEAN_NAME;

		parserContext.pushContainingComponent(new CompositeComponentDefinition("Mongo", source));

		BeanComponentDefinition mongoComponent = helper.getComponent(builder, defaultedId);
		parserContext.registerBeanComponent(mongoComponent);
		BeanComponentDefinition serverAddressPropertyEditor = helper.getComponent(registerServerAddressPropertyEditor());
		parserContext.registerBeanComponent(serverAddressPropertyEditor);
		BeanComponentDefinition writeConcernPropertyEditor = helper.getComponent(MongoParsingUtils
				.getWriteConcernPropertyEditorBuilder());
		parserContext.registerBeanComponent(writeConcernPropertyEditor);

		parserContext.popAndRegisterContainingComponent();

		return mongoComponent.getBeanDefinition();
	}

	/**
	 * One should only register one bean definition but want to have the convenience of using
	 * AbstractSingleBeanDefinitionParser but have the side effect of registering a 'default' property editor with the
	 * container.
	 */
	private BeanDefinitionBuilder registerServerAddressPropertyEditor() {

		Map<String, String> customEditors = new ManagedMap<String, String>();
		customEditors.put("com.mongodb.ServerAddress[]",
				"org.springframework.data.mongodb.config.ServerAddressPropertyEditor");

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(CustomEditorConfigurer.class);
		builder.addPropertyValue("customEditors", customEditors);
		return builder;
	}
}
