/*
 * Copyright (c) 2011 by the original author(s).
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

import static org.springframework.data.mongodb.config.BeanNames.*;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mongodb.core.MongoFactoryBean;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

import com.mongodb.Mongo;

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

		String mongoRef = element.getAttribute("mongo-ref");
		if (!StringUtils.hasText(mongoRef)) {
			mongoRef = registerMongoBeanDefinition(element, parserContext);
		}

		// Database name
		String dbname = element.getAttribute("dbname");
		if (!StringUtils.hasText(dbname)) {
			dbname = "db";
		}

		BeanDefinitionBuilder dbFactoryBuilder = BeanDefinitionBuilder.genericBeanDefinition(SimpleMongoDbFactory.class);
		dbFactoryBuilder.addConstructorArgValue(new RuntimeBeanReference(mongoRef));
		dbFactoryBuilder.addConstructorArgValue(dbname);

		BeanDefinition userCredentials = getUserCredentialsBeanDefinition(element);
		if (userCredentials != null) {
			dbFactoryBuilder.addConstructorArgValue(userCredentials);
		}

		ParsingUtils.setPropertyValue(element, dbFactoryBuilder, "write-concern", "writeConcern");

		return dbFactoryBuilder.getBeanDefinition();

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

	/**
	 * Returns a {@link BeanDefinition} for a {@link UserCredentials} object.
	 * 
	 * @param element
	 * @return the {@link BeanDefinition} or {@literal null} if neither username nor password given.
	 */
	private BeanDefinition getUserCredentialsBeanDefinition(Element element) {

		String username = element.getAttribute("username");
		String password = element.getAttribute("password");

		if (!StringUtils.hasText(username) && !StringUtils.hasText(password)) {
			return null;
		}

		BeanDefinitionBuilder userCredentialsBuilder = BeanDefinitionBuilder.genericBeanDefinition(UserCredentials.class);
		userCredentialsBuilder.addConstructorArgValue(StringUtils.hasText(username) ? username : null);
		userCredentialsBuilder.addConstructorArgValue(StringUtils.hasText(password) ? password : null);

		return userCredentialsBuilder.getBeanDefinition();
	}
}
