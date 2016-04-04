/*
 * Copyright 2011-2015 by the original author(s).
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

import static org.springframework.data.config.ParsingUtils.*;
import static org.springframework.data.mongodb.config.MongoParsingUtils.*;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.parsing.BeanComponentDefinition;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.BeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.config.BeanComponentDefinitionBuilder;
import org.springframework.data.mongodb.core.MongoClientFactoryBean;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

import com.mongodb.Mongo;
import com.mongodb.MongoClientURI;
import com.mongodb.MongoURI;

/**
 * {@link BeanDefinitionParser} to parse {@code db-factory} elements into {@link BeanDefinition}s.
 * 
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Viktor Khoroshko
 */
public class MongoDbFactoryParser extends AbstractBeanDefinitionParser {

	private static final Set<String> MONGO_URI_ALLOWED_ADDITIONAL_ATTRIBUTES;

	static {

		Set<String> mongoUriAllowedAdditionalAttributes = new HashSet<String>();
		mongoUriAllowedAdditionalAttributes.add("id");
		mongoUriAllowedAdditionalAttributes.add("write-concern");

		MONGO_URI_ALLOWED_ADDITIONAL_ATTRIBUTES = Collections.unmodifiableSet(mongoUriAllowedAdditionalAttributes);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractBeanDefinitionParser#resolveId(org.w3c.dom.Element, org.springframework.beans.factory.support.AbstractBeanDefinition, org.springframework.beans.factory.xml.ParserContext)
	 */
	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext)
			throws BeanDefinitionStoreException {

		String id = super.resolveId(element, definition, parserContext);
		return StringUtils.hasText(id) ? id : BeanNames.DB_FACTORY_BEAN_NAME;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.AbstractBeanDefinitionParser#parseInternal(org.w3c.dom.Element, org.springframework.beans.factory.xml.ParserContext)
	 */
	@Override
	protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {

		// Common setup
		BeanDefinitionBuilder dbFactoryBuilder = BeanDefinitionBuilder.genericBeanDefinition(SimpleMongoDbFactory.class);
		setPropertyValue(dbFactoryBuilder, element, "write-concern", "writeConcern");

		BeanDefinition mongoUri = getMongoUri(element, parserContext);

		if (mongoUri != null) {

			dbFactoryBuilder.addConstructorArgValue(mongoUri);
			return getSourceBeanDefinition(dbFactoryBuilder, parserContext, element);
		}

		BeanComponentDefinitionBuilder helper = new BeanComponentDefinitionBuilder(element, parserContext);

		String mongoRef = element.getAttribute("mongo-ref");
		String dbname = element.getAttribute("dbname");

		BeanDefinition userCredentials = getUserCredentialsBeanDefinition(element, parserContext);

		// Defaulting
		if (StringUtils.hasText(mongoRef)) {
			dbFactoryBuilder.addConstructorArgReference(mongoRef);
		} else {
			dbFactoryBuilder.addConstructorArgValue(registerMongoBeanDefinition(element, parserContext));
		}

		dbFactoryBuilder.addConstructorArgValue(StringUtils.hasText(dbname) ? dbname : "db");
		dbFactoryBuilder.addConstructorArgValue(userCredentials);
		dbFactoryBuilder.addConstructorArgValue(element.getAttribute("authentication-dbname"));

		BeanDefinitionBuilder writeConcernPropertyEditorBuilder = getWriteConcernPropertyEditorBuilder();

		BeanComponentDefinition component = helper.getComponent(writeConcernPropertyEditorBuilder);
		parserContext.registerBeanComponent(component);

		return (AbstractBeanDefinition) helper.getComponentIdButFallback(dbFactoryBuilder, BeanNames.DB_FACTORY_BEAN_NAME)
				.getBeanDefinition();
	}

	/**
	 * Registers a default {@link BeanDefinition} of a {@link Mongo} instance and returns the name under which the
	 * {@link Mongo} instance was registered under.
	 * 
	 * @param element must not be {@literal null}.
	 * @param parserContext must not be {@literal null}.
	 * @return
	 */
	private BeanDefinition registerMongoBeanDefinition(Element element, ParserContext parserContext) {

		BeanDefinitionBuilder mongoBuilder = BeanDefinitionBuilder.genericBeanDefinition(MongoClientFactoryBean.class);
		setPropertyValue(mongoBuilder, element, "host");
		setPropertyValue(mongoBuilder, element, "port");

		return getSourceBeanDefinition(mongoBuilder, parserContext, element);
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
	 * Creates a {@link BeanDefinition} for a {@link MongoURI} or {@link MongoClientURI} depending on configured
	 * attributes. <br />
	 * Errors when configured element contains {@literal uri} or {@literal client-uri} along with other attributes except
	 * {@literal write-concern} and/or {@literal id}.
	 * 
	 * @param element must not be {@literal null}.
	 * @param parserContext
	 * @return {@literal null} in case no client-/uri defined.
	 */
	private BeanDefinition getMongoUri(Element element, ParserContext parserContext) {

		boolean hasClientUri = element.hasAttribute("client-uri");

		if (!hasClientUri && !element.hasAttribute("uri")) {
			return null;
		}

		int allowedAttributesCount = 1;
		for (String attribute : MONGO_URI_ALLOWED_ADDITIONAL_ATTRIBUTES) {

			if (element.hasAttribute(attribute)) {
				allowedAttributesCount++;
			}
		}

		if (element.getAttributes().getLength() > allowedAttributesCount) {

			parserContext.getReaderContext().error(
					"Configure either " + (hasClientUri ? "Mongo Client URI" : "Mongo URI") + " or details individually!",
					parserContext.extractSource(element));
		}

		Class<?> type = hasClientUri ? MongoClientURI.class : MongoURI.class;
		String uri = hasClientUri ? element.getAttribute("client-uri") : element.getAttribute("uri");

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.genericBeanDefinition(type);
		builder.addConstructorArgValue(uri);

		return builder.getBeanDefinition();
	}
}
