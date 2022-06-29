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

import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionReaderUtils;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.context.annotation.ImportBeanDefinitionRegistrar;
import org.springframework.core.Ordered;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.auditing.config.AuditingBeanDefinitionRegistrarSupport;
import org.springframework.data.auditing.config.AuditingConfiguration;
import org.springframework.data.config.ParsingUtils;
import org.springframework.data.mapping.context.PersistentEntities;
import org.springframework.data.mongodb.core.mapping.event.AuditingEntityCallback;
import org.springframework.lang.Nullable;
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

	@Override
	protected void postProcess(BeanDefinitionBuilder builder, AuditingConfiguration configuration,
			BeanDefinitionRegistry registry) {
		potentiallyRegisterMongoPersistentEntities(builder, registry);
	}

	@Override
	protected BeanDefinitionBuilder getAuditHandlerBeanDefinitionBuilder(AuditingConfiguration configuration) {

		Assert.notNull(configuration, "AuditingConfiguration must not be null");

		return configureDefaultAuditHandlerAttributes(configuration,
				BeanDefinitionBuilder.rootBeanDefinition(IsNewAwareAuditingHandler.class));
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

	static void potentiallyRegisterMongoPersistentEntities(BeanDefinitionBuilder builder,
			BeanDefinitionRegistry registry) {

		String persistentEntitiesBeanName = MongoAuditingRegistrar.detectPersistentEntitiesBeanName(registry);

		if (persistentEntitiesBeanName == null) {

			persistentEntitiesBeanName = BeanDefinitionReaderUtils.uniqueBeanName("mongoPersistentEntities", registry);

			// TODO: https://github.com/spring-projects/spring-framework/issues/28728
			BeanDefinitionBuilder definition = BeanDefinitionBuilder.genericBeanDefinition(PersistentEntities.class) //
					.setFactoryMethod("of") //
					.addConstructorArgReference("mongoMappingContext");

			registry.registerBeanDefinition(persistentEntitiesBeanName, definition.getBeanDefinition());
		}

		builder.addConstructorArgReference(persistentEntitiesBeanName);
	}

	@Nullable
	private static String detectPersistentEntitiesBeanName(BeanDefinitionRegistry registry) {

		if (registry instanceof ListableBeanFactory beanFactory) {
			for (String bn : beanFactory.getBeanNamesForType(PersistentEntities.class)) {
				if (bn.startsWith("mongo")) {
					return bn;
				}
			}
		}

		return null;
	}
}
