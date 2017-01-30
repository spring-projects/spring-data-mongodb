/*
 * Copyright 2016-2017 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoTypeMapper;
import org.springframework.data.mongodb.core.mapping.BasicMongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.expression.spel.support.StandardEvaluationContext;
import org.springframework.test.util.ReflectionTestUtils;

import com.mongodb.Mongo;
import com.mongodb.reactivestreams.client.MongoClient;
import com.mongodb.reactivestreams.client.MongoClients;

import example.first.First;
import example.second.Second;

/**
 * Unit tests for {@link AbstractReactiveMongoConfiguration}.
 *
 * @author Mark Paluch
 */
public class AbstractReactiveMongoConfigurationUnitTests {

	@Rule public ExpectedException exception = ExpectedException.none();

	@Test // DATAMONGO-1444
	public void usesConfigClassPackageAsBaseMappingPackage() throws ClassNotFoundException {

		AbstractReactiveMongoConfiguration configuration = new SampleMongoConfiguration();
		assertThat(configuration.getMappingBasePackages(), hasItem(SampleMongoConfiguration.class.getPackage().getName()));
		assertThat(configuration.getInitialEntitySet(), hasSize(2));
		assertThat(configuration.getInitialEntitySet(), hasItem(Entity.class));
	}

	@Test // DATAMONGO-1444
	public void doesNotScanPackageIfMappingPackageIsNull() throws ClassNotFoundException {
		assertScanningDisabled(null);
	}

	@Test // DATAMONGO-1444
	public void doesNotScanPackageIfMappingPackageIsEmpty() throws ClassNotFoundException {

		assertScanningDisabled("");
		assertScanningDisabled(" ");
	}

	@Test // DATAMONGO-1444
	public void containsMongoDbFactoryButNoMongoBean() {

		AbstractApplicationContext context = new AnnotationConfigApplicationContext(SampleMongoConfiguration.class);

		assertThat(context.getBean(SimpleReactiveMongoDatabaseFactory.class), is(notNullValue()));

		exception.expect(NoSuchBeanDefinitionException.class);
		context.getBean(Mongo.class);
		context.close();
	}

	@Test // DATAMONGO-1444
	public void returnsUninitializedMappingContext() throws Exception {

		SampleMongoConfiguration configuration = new SampleMongoConfiguration();
		MongoMappingContext context = configuration.mongoMappingContext();

		assertThat(context.getPersistentEntities(), is(emptyIterable()));
		context.initialize();
		assertThat(context.getPersistentEntities(), is(not(emptyIterable())));
	}

	@Test // DATAMONGO-1444
	public void lifecycleCallbacksAreInvokedInAppropriateOrder() {

		AbstractApplicationContext context = new AnnotationConfigApplicationContext(SampleMongoConfiguration.class);
		MongoMappingContext mappingContext = context.getBean(MongoMappingContext.class);
		BasicMongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(Entity.class);
		StandardEvaluationContext spElContext = (StandardEvaluationContext) ReflectionTestUtils.getField(entity, "context");

		assertThat(spElContext.getBeanResolver(), is(notNullValue()));
		context.close();
	}

	@Test // DATAMONGO-1444
	public void shouldBeAbleToConfigureCustomTypeMapperViaJavaConfig() {

		AbstractApplicationContext context = new AnnotationConfigApplicationContext(SampleMongoConfiguration.class);
		MongoTypeMapper typeMapper = context.getBean(CustomMongoTypeMapper.class);
		MappingMongoConverter mmc = context.getBean(MappingMongoConverter.class);

		assertThat(mmc, is(notNullValue()));
		assertThat(mmc.getTypeMapper(), is(typeMapper));
		context.close();
	}

	@Test // DATAMONGO-1444
	@SuppressWarnings("unchecked")
	public void allowsMultipleEntityBasePackages() throws ClassNotFoundException {

		ConfigurationWithMultipleBasePackages config = new ConfigurationWithMultipleBasePackages();
		Set<Class<?>> entities = config.getInitialEntitySet();

		assertThat(entities, hasSize(2));
		assertThat(entities, hasItems(First.class, Second.class));
	}

	private static void assertScanningDisabled(final String value) throws ClassNotFoundException {

		AbstractReactiveMongoConfiguration configuration = new SampleMongoConfiguration() {
			@Override
			protected Collection<String> getMappingBasePackages() {
				return Collections.singleton(value);
			}
		};

		assertThat(configuration.getMappingBasePackages(), hasItem(value));
		assertThat(configuration.getInitialEntitySet(), hasSize(0));
	}

	@Configuration
	static class SampleMongoConfiguration extends AbstractReactiveMongoConfiguration {

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

	static class ConfigurationWithMultipleBasePackages extends AbstractReactiveMongoConfiguration {

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
