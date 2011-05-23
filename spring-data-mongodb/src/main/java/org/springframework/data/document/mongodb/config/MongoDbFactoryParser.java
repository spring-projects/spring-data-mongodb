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

package org.springframework.data.document.mongodb.config;

import static org.springframework.data.document.mongodb.config.BeanNames.*;

import org.springframework.beans.factory.BeanDefinitionStoreException;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.xml.AbstractBeanDefinitionParser;
import org.springframework.beans.factory.xml.ParserContext;
import org.springframework.data.document.mongodb.MongoDbFactoryBean;
import org.springframework.data.document.mongodb.MongoFactoryBean;
import org.springframework.util.StringUtils;
import org.springframework.util.xml.DomUtils;
import org.w3c.dom.Element;

/**
 * @author Jon Brisbin <jbrisbin@vmware.com>
 */
public class MongoDbFactoryParser extends AbstractBeanDefinitionParser {

	@Override
	protected String resolveId(Element element, AbstractBeanDefinition definition, ParserContext parserContext) throws BeanDefinitionStoreException {
		String id = element.getAttribute("id");
		if (!StringUtils.hasText(id)) {
			id = DB_FACTORY;
		}
		return id;
	}

	@Override
	protected AbstractBeanDefinition parseInternal(Element element, ParserContext parserContext) {
		BeanDefinitionRegistry registry = parserContext.getRegistry();
		BeanDefinitionBuilder dbFactoryBuilder = BeanDefinitionBuilder.genericBeanDefinition(MongoDbFactoryBean.class);

		// Host/Port used in this and the mongoFactory
		String host = element.getAttribute("host");
		if (!StringUtils.hasText(host)) {
			host = "localhost";
		}
		dbFactoryBuilder.addPropertyValue("host", host);
		String port = element.getAttribute("port");
		if (!StringUtils.hasText(port)) {
			port = "27017";
		}
		dbFactoryBuilder.addPropertyValue("port", port);

		// Username/Password not always used (but is in CloudFoundry
		String username = element.getAttribute("username");
		if (StringUtils.hasText(username)) {
			dbFactoryBuilder.addPropertyValue("username", username);
		}
		String password = element.getAttribute("password");
		if (StringUtils.hasText(password)) {
			dbFactoryBuilder.addPropertyValue("password", password);
		}

		// Database name
		String dbname = element.getAttribute("dbname");
		if (!StringUtils.hasText(dbname)) {
			dbname = "db";
		}
		dbFactoryBuilder.addPropertyValue("databaseName", dbname);

		// Create or reference a MongoFactory instance.
		// Also respect embedded "mongo:mongo" definitions.
		String mongoRef = element.getAttribute("mongo-ref");
		if (!StringUtils.hasText(mongoRef)) {
			BeanDefinitionBuilder mongoBuilder = BeanDefinitionBuilder.genericBeanDefinition(MongoFactoryBean.class);
			Element mongoEl = DomUtils.getChildElementByTagName(element, "mongo");
			if (null != mongoEl) {
				String overrideHost = mongoEl.getAttribute("host");
				mongoBuilder.addPropertyValue("host", (StringUtils.hasText(overrideHost) ? overrideHost : host));
				String overridePort = mongoEl.getAttribute("port");
				mongoBuilder.addPropertyValue("port", (StringUtils.hasText(overridePort) ? overridePort : port));
				new MongoParser().parseOptions(parserContext, mongoEl, mongoBuilder);
			}
			else {
				mongoBuilder.addPropertyValue("host", host);
				mongoBuilder.addPropertyValue("port", port);
			}
			registry.registerBeanDefinition(MONGO, mongoBuilder.getBeanDefinition());
			mongoRef = MONGO;
		}
		dbFactoryBuilder.addPropertyValue("mongo", new RuntimeBeanReference(mongoRef));

		return dbFactoryBuilder.getRawBeanDefinition();
	}

}
