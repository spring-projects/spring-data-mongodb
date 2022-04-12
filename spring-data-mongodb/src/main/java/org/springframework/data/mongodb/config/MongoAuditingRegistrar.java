/*
 * Copyright 2013-2022 the original author or authors.
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

import java.lang.annotation.Annotation;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.Ordered;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport;
import org.springframework.data.auditing.config.AuditingConfiguration;
import org.springframework.data.config.ParsingUtils;
import org.springframework.data.mapping.context.PersistentEntities;
import org.springframework.data.mongodb.core.mapping.event.AuditingEntityCallback;
import org.springframework.util.Assert;

/**
 * {@link ImportBeanDefinitionRegistrar} to enable {@link EnableMongoAuditing} annotation.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Mark Paluch
 */
class MongoAuditingRegistrar extends AuditingBeanDefinitionRegistrarSupport implements Ordered {

	@Override
	protected Class<? extends Annotation> getAnnotation() {
		return EnableMongoAuditing.class;
	}

	@Override
	protected String getAuditingHandlerBeanName() {
		return "mongoAuditingHandler";
	}

	String persistentEntitiesBeanName;

	@Override
	public void registerBeanDefinitions(AnnotationMetadata annotationMetadata, BeanDefinitionRegistry registry) {

		Assert.notNull(annotationMetadata, "AnnotationMetadata must not be null");
		Assert.notNull(registry, "BeanDefinitionRegistry must not be null");

		if(persistentEntitiesBeanName == null) {
			if (registry instanceof DefaultListableBeanFactory beanFactory) {
				for (String bn : beanFactory.getBeanNamesForType(PersistentEntities.class)) {
					if (bn.startsWith("mongo")) {
						persistentEntitiesBeanName = bn;
					}
				}
			}
			if(persistentEntitiesBeanName == null) {

				persistentEntitiesBeanName = BeanDefinitionReaderUtils.uniqueBeanName("mongo.persistent-entities", registry);

				BeanDefinitionBuilder definition = BeanDefinitionBuilder.genericBeanDefinition(PersistentEntities.class)
						.setFactoryMethod("of")
						//.addConstructorArgValue(new RuntimeBeanReference(MongoMappingContext.class))
						.addConstructorArgReference("mongoMappingContext");
				registry.registerBeanDefinition(persistentEntitiesBeanName, definition.getBeanDefinition());
			}
		}

		super.registerBeanDefinitions(annotationMetadata, registry);
	}

	@Override
	protected BeanDefinitionBuilder getAuditHandlerBeanDefinitionBuilder(AuditingConfiguration configuration) {

		Assert.notNull(configuration, "AuditingConfiguration must not be null");

		BeanDefinitionBuilder builder = BeanDefinitionBuilder.rootBeanDefinition(IsNewAwareAuditingHandler.class);
		builder.addConstructorArgReference(persistentEntitiesBeanName);
		return configureDefaultAuditHandlerAttributes(configuration, builder);
	}

	@Override
	protected void registerAuditListenerBeanDefinition(BeanDefinition auditingHandlerDefinition,
			BeanDefinitionRegistry registry) {

		Assert.notNull(auditingHandlerDefinition, "BeanDefinition must not be null");
		Assert.notNull(registry, "BeanDefinitionRegistry must not be null");

		BeanDefinitionBuilder listenerBeanDefinitionBuilder = BeanDefinitionBuilder
				.rootBeanDefinition(AuditingEntityCallback.class);
		listenerBeanDefinitionBuilder
				.addConstructorArgValue(ParsingUtils.getObjectFactoryBeanDefinition(getAuditingHandlerBeanName(), registry));

		registerInfrastructureBeanWithId(listenerBeanDefinitionBuilder.getBeanDefinition(),
				AuditingEntityCallback.class.getName(), registry);
	}


	@Override
	public int getOrder() {
		return Ordered.LOWEST_PRECEDENCE;
	}
}
