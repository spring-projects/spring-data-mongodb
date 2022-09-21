/*
 * Copyright 2016-2023 the original author or authors.
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

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.bson.UuidRepresentation;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.core.convert.converter.Converter;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.model.CamelCaseAbbreviatingFieldNamingStrategy;
import org.springframework.data.mapping.model.FieldNamingStrategy;
import org.springframework.data.mapping.model.PropertyNameFieldNamingStrategy;
import org.springframework.data.mongodb.MongoManagedTypes;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions.MongoConverterConfigurationAdapter;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoClientSettings.Builder;

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
	 * @see #getMappingBasePackages()
	 * @return
	 */
	@Bean
	public MongoMappingContext mongoMappingContext(MongoCustomConversions customConversions,
			MongoManagedTypes mongoManagedTypes) {

		MongoMappingContext mappingContext = new MongoMappingContext();
		mappingContext.setManagedTypes(mongoManagedTypes);
		mappingContext.setSimpleTypeHolder(customConversions.getSimpleTypeHolder());
		mappingContext.setFieldNamingStrategy(fieldNamingStrategy());
		mappingContext.setAutoIndexCreation(autoIndexCreation());

		return mappingContext;
	}

	/**
	 * @return new instance of {@link MongoManagedTypes}.
	 * @throws ClassNotFoundException
	 * @since 4.0
	 */
	@Bean
	public MongoManagedTypes mongoManagedTypes() throws ClassNotFoundException {
		return MongoManagedTypes.fromIterable(getInitialEntitySet());
	}

	/**
	 * Register custom {@link Converter}s in a {@link CustomConversions} object if required. These
	 * {@link CustomConversions} will be registered with the
	 * {@link org.springframework.data.mongodb.core.convert.MappingMongoConverter} and {@link MongoMappingContext}.
	 * Returns an empty {@link MongoCustomConversions} instance by default.
	 * <p>
	 * <strong>NOTE:</strong> Use {@link #configureConverters(MongoConverterConfigurationAdapter)} to configure MongoDB
	 * native simple types and register custom {@link Converter converters}.
	 *
	 * @return must not be {@literal null}.
	 */
	@Bean
	public MongoCustomConversions customConversions() {
		return MongoCustomConversions.create(this::configureConverters);
	}

	/**
	 * Configuration hook for {@link MongoCustomConversions} creation.
	 *
	 * @param converterConfigurationAdapter never {@literal null}.
	 * @since 2.3
	 * @see MongoConverterConfigurationAdapter#useNativeDriverJavaTimeCodecs()
	 * @see MongoConverterConfigurationAdapter#useSpringDataJavaTimeCodecs()
	 */
	protected void configureConverters(MongoConverterConfigurationAdapter converterConfigurationAdapter) {

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
	 * Scans the given base package for entities, i.e. MongoDB specific types annotated with {@link Document}.
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

			for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {

				initialEntitySet
						.add(ClassUtils.forName(candidate.getBeanClassName(), MongoConfigurationSupport.class.getClassLoader()));
			}
		}

		return initialEntitySet;
	}

	/**
	 * Configures whether to abbreviate field names for domain objects by configuring a
	 * {@link CamelCaseAbbreviatingFieldNamingStrategy} on the {@link MongoMappingContext} instance created.
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

	/**
	 * Configure whether to automatically create indices for domain types by deriving the
	 * {@link org.springframework.data.mongodb.core.index.IndexDefinition} from the entity or not.
	 *
	 * @return {@literal false} by default. <br />
	 *         <strong>INFO:</strong> As of 3.x the default is set to {@literal false}; In 2.x it was {@literal true}.
	 * @since 2.2
	 */
	protected boolean autoIndexCreation() {
		return false;
	}

	/**
	 * Return the {@link MongoClientSettings} used to create the actual {@literal MongoClient}. <br />
	 * Override either this method, or use {@link #configureClientSettings(Builder)} to alter the setup.
	 *
	 * @return never {@literal null}.
	 * @since 3.0
	 */
	protected MongoClientSettings mongoClientSettings() {

		MongoClientSettings.Builder builder = MongoClientSettings.builder();
		builder.uuidRepresentation(UuidRepresentation.JAVA_LEGACY);
		configureClientSettings(builder);
		return builder.build();
	}

	/**
	 * Configure {@link MongoClientSettings} via its {@link Builder} API.
	 *
	 * @param builder never {@literal null}.
	 * @since 3.0
	 */
	protected void configureClientSettings(MongoClientSettings.Builder builder) {
		// customization hook
	}
}
