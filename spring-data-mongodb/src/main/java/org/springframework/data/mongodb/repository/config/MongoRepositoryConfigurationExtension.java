/*
 * Copyright 2012-2021 the original author or authors.
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
package org.springframework.data.mongodb.repository.config;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.data.config.ParsingUtils;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactoryBean;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.XmlRepositoryConfigurationSource;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.w3c.dom.Element;

/**
 * {@link org.springframework.data.repository.config.RepositoryConfigurationExtension} for MongoDB.
 *
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class MongoRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport {

	private static final String MONGO_TEMPLATE_REF = "mongo-template-ref";
	private static final String CREATE_QUERY_INDEXES = "create-query-indexes";

	@Override
	public String getModuleName() {
		return "MongoDB";
	}

	@Override
	protected String getModulePrefix() {
		return "mongo";
	}

	public String getRepositoryFactoryBeanClassName() {
		return MongoRepositoryFactoryBean.class.getName();
	}

	@Override
	protected Collection<Class<? extends Annotation>> getIdentifyingAnnotations() {
		return Collections.singleton(Document.class);
	}

	@Override
	protected Collection<Class<?>> getIdentifyingTypes() {
		return Collections.singleton(MongoRepository.class);
	}

	@Override
	public void postProcess(BeanDefinitionBuilder builder, XmlRepositoryConfigurationSource config) {

		Element element = config.getElement();

		ParsingUtils.setPropertyReference(builder, element, MONGO_TEMPLATE_REF, "mongoOperations");
		ParsingUtils.setPropertyValue(builder, element, CREATE_QUERY_INDEXES, "createIndexesForQueryMethods");
	}

	@Override
	public void postProcess(BeanDefinitionBuilder builder, AnnotationRepositoryConfigurationSource config) {

		AnnotationAttributes attributes = config.getAttributes();

		builder.addPropertyReference("mongoOperations", attributes.getString("mongoTemplateRef"));
		builder.addPropertyValue("createIndexesForQueryMethods", attributes.getBoolean("createIndexesForQueryMethods"));
	}

	@Override
	protected boolean useRepositoryConfiguration(RepositoryMetadata metadata) {
		return !metadata.isReactiveRepository();
	}
}
