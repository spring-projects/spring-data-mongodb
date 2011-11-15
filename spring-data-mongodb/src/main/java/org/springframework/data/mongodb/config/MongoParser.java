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

import java.util.Map;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.CustomEditorConfigurer;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.AbstractSingleBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.mongodb.core.MongoFactoryBean;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * Parser for &lt;mongo;gt; definitions.
 * 
 * @author Mark Pollack
 */
public class MongoParser extends AbstractSingleBeanDefinitionParser {

	@Override
	protected Class<?> getBeanClass(Element element) {
		return MongoFactoryBean.class;
	}

	@Override
	protected void doParse(Element element, ParserContext parserContext, BeanDefinitionBuilder builder) {

		ParsingUtils.setPropertyValue(element, builder, "port", "port");
		ParsingUtils.setPropertyValue(element, builder, "host", "host");
		ParsingUtils.setPropertyValue(element, builder, "write-concern", "writeConcern");

		ParsingUtils.parseMongoOptions(element, builder);
		ParsingUtils.parseReplicaSet(element, builder);
				
		registerServerAddressPropertyEditor(parserContext.getRegistry());
		registerWriteConcernPropertyEditor(parserContext.getRegistry());

	}

	/**
	 * One should only register one bean definition but want to have the convenience of using AbstractSingleBeanDefinitionParser but have the side effect of
	 * registering a 'default' property editor with the container.  
	 * @param parserContext the ParserContext to 
	 */
	private void registerServerAddressPropertyEditor(BeanDefinitionRegistry registry) {
		BeanDefinitionBuilder customEditorConfigurer = BeanDefinitionBuilder.genericBeanDefinition(CustomEditorConfigurer.class);
		Map<String, String> customEditors = new ManagedMap<String, String>(); 
		customEditors.put("java.util.List", "org.springframework.data.mongodb.config.ServerAddressPropertyEditor");
		customEditorConfigurer.addPropertyValue("customEditors", customEditors);
		BeanDefinitionReaderUtils.registerWithGeneratedName(customEditorConfigurer.getBeanDefinition(),	registry);
	}
	
	private void registerWriteConcernPropertyEditor(BeanDefinitionRegistry registry) {
		BeanDefinitionBuilder customEditorConfigurer = BeanDefinitionBuilder.genericBeanDefinition(CustomEditorConfigurer.class);
		Map<String, String> customEditors = new ManagedMap<String, String>(); 
		customEditors.put("com.mongodb.WriteConcern", "org.springframework.data.mongodb.config.WriteConcernPropertyEditor");
		customEditorConfigurer.addPropertyValue("customEditors", customEditors);
		BeanDefinitionReaderUtils.registerWithGeneratedName(customEditorConfigurer.getBeanDefinition(),	registry);
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
}