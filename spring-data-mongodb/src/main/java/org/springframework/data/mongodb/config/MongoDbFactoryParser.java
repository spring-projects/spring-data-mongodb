/*
 * Copyright 2011 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.config;

import static org.springframework.data.mongodb.config.BeanNames.DB_FACTORY;
import static org.springframework.data.mongodb.config.ParsingUtils.getSourceBeanDefinition;

import java.util.Map;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.CustomEditorConfigurer;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.ManagedMap;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.core.MongoFactoryBean;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

import com.mongodb.Mongo;
import com.mongodb.MongoURI;

/**
 * {@link BeanDefinitionParser} to parse {@code db-factory} elements into {@link BeanDefinition}s.
 * 
 * @author Jon Brisbin
 * @author Oliver Gierke
 */
public class MongoDbFactoryParser extends AbstractBeanDefinitionParser {

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {
		String id = element.getAttribute("id");
		if (!StringUtils.hasText(id)) {
			id = DB_FACTORY;
		}
		return id;
	}

	@Override
	protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
		
		String uri = element.getAttribute("uri");
		String mongoRef = element.getAttribute("mongo-ref");
		String dbname = element.getAttribute("dbname");
		BeanDefinition userCredentials = getUserCredentialsBeanDefinition(element, parserContext);		

		// Common setup
		BeanDefinitionBuilder dbFactoryBuilder = BeanDefinitionBuilder.genericBeanDefinition(SimpleMongoDbFactory.class);
		ParsingUtils.setPropertyValue(element, dbFactoryBuilder, "write-concern", "writeConcern");
		
		if (StringUtils.hasText(uri)) {
			if(StringUtils.hasText(mongoRef) || StringUtils.hasText(dbname) || userCredentials != null) {
				parserContext.getReaderContext().error("Configure either Mongo URI or details individually!", parserContext.extractSource(element));
			}
			
			dbFactoryBuilder.addConstructorArgValue(getMongoUri(uri));
			return getSourceBeanDefinition(dbFactoryBuilder, parserContext, element);
		}
		
		// Defaulting
		mongoRef = StringUtils.hasText(mongoRef) ? mongoRef : registerMongoBeanDefinition(element, parserContext);
		dbname = StringUtils.hasText(dbname) ? dbname : "db";
		
		dbFactoryBuilder.addConstructorArgValue(new RuntimeBeanReference(mongoRef));
		dbFactoryBuilder.addConstructorArgValue(dbname);

		if (userCredentials != null) {
			dbFactoryBuilder.addConstructorArgValue(userCredentials);
		}
		
		//Register property editor to parse WriteConcern
		
		registerWriteConcernPropertyEditor(parserContext.getRegistry());

		return getSourceBeanDefinition(dbFactoryBuilder, parserContext, element);
	}

	/**
	 * Registers a default {@link BeanDefinition} of a {@link Mongo} instance and returns the name under which the
	 * {@link Mongo} instance was registered under.
	 * 
	 * @param element must not be {@literal null}.
	 * @param parserContext must not be {@literal null}.
	 * @return
	 */
	private String registerMongoBeanDefinition(Element element, ParserContext parserContext) {

		BeanDefinitionBuilder mongoBuilder = BeanDefinitionBuilder.genericBeanDefinition(MongoFactoryBean.class);
		ParsingUtils.setPropertyValue(element, mongoBuilder, "host");
		ParsingUtils.setPropertyValue(element, mongoBuilder, "port");

		return BeanDefinitionReaderUtils.registerWithGeneratedName(mongoBuilder.getBeanDefinition(),
				parserContext.getRegistry());
	}
	
	private void registerWriteConcernPropertyEditor(BeanDefinitionRegistry registry) {
		BeanDefinitionBuilder customEditorConfigurer = BeanDefinitionBuilder.genericBeanDefinition(CustomEditorConfigurer.class);
		Map<String, String> customEditors = new ManagedMap<String, String>(); 
		customEditors.put("com.mongodb.WriteConcern", "org.springframework.data.mongodb.config.WriteConcernPropertyEditor");
		customEditorConfigurer.addPropertyValue("customEditors", customEditors);
		BeanDefinitionReaderUtils.registerWithGeneratedName(customEditorConfigurer.getBeanDefinition(),	registry);
	}

	/**
	 * Returns a {@link BeanDefinition} for a {@link UserCredentials} object.
	 * 
	 * @param element
	 * @return the {@link BeanDefinition} or {@literal null} if neither username nor password given.
	 */
	private BeanDefinition getUserCredentialsBeanDefinition(Element element, ParserContext context) {

		String username = element.getAttribute("username");
		String password = element.getAttribute("password");

		if (!StringUtils.hasText(username) && !StringUtils.hasText(password)) {
			return null;
		}

		BeanDefinitionBuilder userCredentialsBuilder = BeanDefinitionBuilder.genericBeanDefinition(UserCredentials.class);
		userCredentialsBuilder.addConstructorArgValue(StringUtils.hasText(username) ? username : null);
		userCredentialsBuilder.addConstructorArgValue(StringUtils.hasText(password) ? password : null);

		return getSourceBeanDefinition(userCredentialsBuilder, context, element);
	}
	
	/**
	 * Creates a {@link BeanDefinition} for a {@link MongoURI}.
	 * 
	 * @param uri
	 * @return
	 */
	private BeanDefinition getMongoUri(String uri) {
		
		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(MongoURI.class);
		builder.addConstructorArgValue(uri);
		
		return builder.getBeanDefinition();
	}
}
