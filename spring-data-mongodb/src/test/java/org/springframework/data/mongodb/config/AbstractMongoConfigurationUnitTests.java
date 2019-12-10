/*
 * Copyright 2012-2019 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import example.first.First;
import example.second.Second;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.junit.Test;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoTypeMapper;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.spel.EvaluationContextProvider;
import org.springframework.data.spel.ExtensionAwareEvaluationContextProvider;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * Unit tests for {@link AbstractMongoClientConfiguration}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public class AbstractMongoConfigurationUnitTests {

	@Test // DATAMONGO-496
	public void usesConfigClassPackageAsBaseMappingPackage() throws ClassNotFoundException {

		AbstractMongoClientConfiguration configuration = new SampleMongoConfiguration();
		assertThat(configuration.getMappingBasePackage()).isEqualTo(SampleMongoConfiguration.class.getPackage().getName());
		assertThat(configuration.getInitialEntitySet()).hasSize(2);
		assertThat(configuration.getInitialEntitySet()).contains(Entity.class);
	}

	@Test // DATAMONGO-496
	public void doesNotScanPackageIfMappingPackageIsNull() throws ClassNotFoundException {
		assertScanningDisabled(null);
	}

	@Test // DATAMONGO-496
	public void doesNotScanPackageIfMappingPackageIsEmpty() throws ClassNotFoundException {

		assertScanningDisabled("");
		assertScanningDisabled(" ");
	}

	@Test // DATAMONGO-569
	public void containsMongoDbFactoryButNoMongoBean() {

		AbstractApplicationContext context = new AnnotationConfigApplicationContext(SampleMongoConfiguration.class);

		assertThat(context.getBean(MongoDbFactory.class)).isNotNull();
		assertThatExceptionOfType(NoSuchBeanDefinitionException.class).isThrownBy(() -> context.getBean(MongoClient.class));

		context.close();
	}

	@Test
	public void returnsUninitializedMappingContext() throws Exception {

		SampleMongoConfiguration configuration = new SampleMongoConfiguration();
		MongoMappingContext context = configuration.mongoMappingContext();

		assertThat(context.getPersistentEntities()).isEmpty();
		context.initialize();
		assertThat(context.getPersistentEntities()).isNotEmpty();
	}

	@Test // DATAMONGO-717
	public void lifecycleCallbacksAreInvokedInAppropriateOrder() {

		AbstractApplicationContext context = new AnnotationConfigApplicationContext(SampleMongoConfiguration.class);
		MongoMappingContext mappingContext = context.getBean(MongoMappingContext.class);
		BasicMongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Entity.class);
		EvaluationContextProvider provider = (EvaluationContextProvider) ReflectionTestUtils.getField(entity,
				"evaluationContextProvider");

		assertThat(provider).isInstanceOf(ExtensionAwareEvaluationContextProvider.class);
		context.close();
	}

	@Test // DATAMONGO-725
	public void shouldBeAbleToConfigureCustomTypeMapperViaJavaConfig() {

		AbstractApplicationContext context = new AnnotationConfigApplicationContext(SampleMongoConfiguration.class);
		MongoTypeMapper typeMapper = context.getBean(CustomMongoTypeMapper.class);
		MappingMongoConverter mmc = context.getBean(MappingMongoConverter.class);

		assertThat(mmc).isNotNull();
		assertThat(mmc.getTypeMapper()).isEqualTo(typeMapper);
		context.close();
	}

	@Test // DATAMONGO-1470
	@SuppressWarnings("unchecked")
	public void allowsMultipleEntityBasePackages() throws ClassNotFoundException {

		ConfigurationWithMultipleBasePackages config = new ConfigurationWithMultipleBasePackages();
		Set<Class<?>> entities = config.getInitialEntitySet();

		assertThat(entities).hasSize(2);
		assertThat(entities).contains(First.class, Second.class);
	}

	private static void assertScanningDisabled(final String value) throws ClassNotFoundException {

		AbstractMongoClientConfiguration configuration = new SampleMongoConfiguration() {
			@Override
			protected Collection<String> getMappingBasePackages() {
				return Collections.singleton(value);
			}
		};

		assertThat(configuration.getMappingBasePackages()).contains(value);
		assertThat(configuration.getInitialEntitySet()).hasSize(0);
	}

	@Configuration
	static class SampleMongoConfiguration extends AbstractMongoClientConfiguration {

		@Override
		protected String getDatabaseName() {
			return "database";
		}

		@Override
		public MongoClient mongoClient() {
			return MongoClients.create();
		}

		@Bean
		@Override
		public MappingMongoConverter mappingMongoConverter() throws Exception {

			MappingMongoConverter converter = super.mappingMongoConverter();
			converter.setTypeMapper(typeMapper());

			return converter;
		}

		@Bean
		public MongoTypeMapper typeMapper() {
			return new CustomMongoTypeMapper();
		}
	}

	static class ConfigurationWithMultipleBasePackages extends AbstractMongoClientConfiguration {

		@Override
		protected String getDatabaseName() {
			return "test";
		}

		@Override
		public MongoClient mongoClient() {
			return MongoClients.create();
		}

		@Override
		protected Collection<String> getMappingBasePackages() {
			return Arrays.asList("example.first", "example.second");
		}
	}

	@Document
	static class Entity {}
}
