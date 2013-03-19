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

import org.springframework.beans.factory.xml.NamespaceHandlerSupport;
import org.springframework.data.mongodb.repository.config.MongoRepositoryConfigurationExtension;
import org.springframework.data.repository.config.RepositoryBeanDefinitionParser;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;

/**
 * {@link org.springframework.beans.factory.xml.NamespaceHandler} for Mongo DB configuration.
 * 
 * @author Oliver Gierke
 */
public class MongoNamespaceHandler extends NamespaceHandlerSupport {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.xml.NamespaceHandler#init()
	 */
	public void init() {

		RepositoryConfigurationExtension extension = new MongoRepositoryConfigurationExtension();
		RepositoryBeanDefinitionParser repositoryBeanDefinitionParser = new RepositoryBeanDefinitionParser(extension);

		registerBeanDefinitionParser("repositories", repositoryBeanDefinitionParser);
		registerBeanDefinitionParser("mapping-converter", new MappingMongoConverterParser());
		registerBeanDefinitionParser("mongo", new MongoParser());
		registerBeanDefinitionParser("db-factory", new MongoDbFactoryParser());
		registerBeanDefinitionParser("jmx", new MongoJmxParser());
		registerBeanDefinitionParser("auditing", new MongoAuditingBeanDefinitionParser());
		registerBeanDefinitionParser("template", new MongoTemplateParser());

	}
}
