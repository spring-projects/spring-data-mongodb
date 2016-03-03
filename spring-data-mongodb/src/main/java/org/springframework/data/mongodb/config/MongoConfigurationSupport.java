/*
 * Copyright 2016 the original author or authors.
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.authentication.UserCredentials;
import org.springframework.data.mapping.context.MappingContextIsNewStrategyFactory;
import org.springframework.data.mapping.model.CamelCaseAbbreviatingFieldNamingStrategy;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.CustomConversions;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.support.CachingIsNewStrategyFactory;
import org.springframework.data.support.IsNewStrategyFactory;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;

/**
 * Base class for Spring Data MongoDB to be extended for JavaConfiguration usage.
 * 
 * @author Mark Paluch
 * @since 2.0
 */
public abstract class MongoConfigurationSupport {

	/**
	 * Return the name of the database to connect to.
	 * 
	 * @return must not be {@literal null}.
	 */
	protected abstract String getDatabaseName();

	/**
	 * Returns the base packages to scan for MongoDB mapped entities at startup. Will return the package name of the
	 * configuration class' (the concrete class, not this one here) by default. So if you have a
	 * {@code com.acme.AppConfig} extending {@link MongoConfigurationSupport} the base package will be considered
	 * {@code com.acme} unless the method is overridden to implement alternate behavior.
	 * 
	 * @return the base packages to scan for mapped {@link Document} classes or an empty collection to not enable scanning
	 *         for entities.
	 * @since 1.10
	 */
	protected Collection<String> getMappingBasePackages() {

		Package mappingBasePackage = getClass().getPackage();
		return Collections.singleton(mappingBasePackage == null ? null : mappingBasePackage.getName());
	}

	/**
	 * Creates a {@link MongoMappingContext} equipped with entity classes scanned from the mapping base package.
	 * 
	 * @see #getMappingBasePackage()
	 * @return
	 * @throws ClassNotFoundException
	 */
	@Bean
	public MongoMappingContext mongoMappingContext() throws ClassNotFoundException {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setInitialEntitySet(getInitialEntitySet());
		mappingContext.setSimpleTypeHolder(customConversions().getSimpleTypeHolder());
		mappingContext.setFieldNamingStrategy(fieldNamingStrategy());

		return mappingContext;
	}

	/**
	 * Returns a {@link MappingContextIsNewStrategyFactory} wrapped into a {@link CachingIsNewStrategyFactory}.
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 */
	@Bean
	public IsNewStrategyFactory isNewStrategyFactory() throws ClassNotFoundException {
		return new CachingIsNewStrategyFactory(new MappingContextIsNewStrategyFactory(mongoMappingContext()));
	}

	/**
	 * Register custom {@link Converter}s in a {@link CustomConversions} object if required. These
	 * {@link CustomConversions} will be registered with the {@link #mappingMongoConverter()} and
	 * {@link #mongoMappingContext()}. Returns an empty {@link CustomConversions} instance by default.
	 * 
	 * @return must not be {@literal null}.
	 */
	@Bean
	public CustomConversions customConversions() {
		return new CustomConversions(Collections.emptyList());
	}

	/**
	 * Scans the mapping base package for classes annotated with {@link Document}. By default, it scans for entities in
	 * all packages returned by {@link #getMappingBasePackages()}.
	 * 
	 * @see #getMappingBasePackages()
	 * @return
	 * @throws ClassNotFoundException
	 */
	protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {

		Set<Class<?>> initialEntitySet = new HashSet<Class<?>>();

		for (String basePackage : getMappingBasePackages()) {
			initialEntitySet.addAll(scanForEntities(basePackage));
		}

		return initialEntitySet;
	}

	/**
	 * Scans the given base package for entities, i.e. MongoDB specific types annotated with {@link Document} and
	 * {@link Persistent}.
	 * 
	 * @param basePackage must not be {@literal null}.
	 * @return
	 * @throws ClassNotFoundException
	 * @since 1.10
	 */
	protected Set<Class<?>> scanForEntities(String basePackage) throws ClassNotFoundException {

		if (!StringUtils.hasText(basePackage)) {
			return Collections.emptySet();
		}

		Set<Class<?>> initialEntitySet = new HashSet<Class<?>>();

		if (StringUtils.hasText(basePackage)) {

			ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(
					false);
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Document.class));
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));

			for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {

				initialEntitySet
						.add(ClassUtils.forName(candidate.getBeanClassName(), MongoConfigurationSupport.class.getClassLoader()));
			}
		}

		return initialEntitySet;
	}

	/**
	 * Configures whether to abbreviate field names for domain objects by configuring a
	 * {@link CamelCaseAbbreviatingFieldNamingStrategy} on the {@link MongoMappingContext} instance created. For advanced
	 * customization needs, consider overriding {@link #mappingMongoConverter()}.
	 * 
	 * @return
	 */
	protected boolean abbreviateFieldNames() {
		return false;
	}

	/**
	 * Configures a {@link FieldNamingStrategy} on the {@link MongoMappingContext} instance created.
	 * 
	 * @return
	 * @since 1.5
	 */
	protected FieldNamingStrategy fieldNamingStrategy() {
		return abbreviateFieldNames() ? new CamelCaseAbbreviatingFieldNamingStrategy()
				: PropertyNameFieldNamingStrategy.INSTANCE;
	}
}
