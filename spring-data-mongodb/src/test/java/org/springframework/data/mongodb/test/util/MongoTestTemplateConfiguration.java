/*
 * Copyright 2020-2021 the original author or authors.
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
package org.springframework.data.mongodb.test.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.convert.converter.Converter;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.convert.CustomConversions;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.event.AuditingEventListener;
import org.springframework.data.mongodb.core.mapping.event.MongoMappingEvent;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 * @since 3.0
 */
public class MongoTestTemplateConfiguration {

	private final DatabaseFactoryConfigurer dbFactoryConfig = new DatabaseFactoryConfigurer();
	private final MappingContextConfigurer mappingContextConfigurer = new MappingContextConfigurer();
	private final MongoConverterConfigurer mongoConverterConfigurer = new MongoConverterConfigurer();
	private final AuditingConfigurer auditingConfigurer = new AuditingConfigurer();
	private final ApplicationContextConfigurer applicationContextConfigurer = new ApplicationContextConfigurer();

	private MongoMappingContext mappingContext;
	private MappingMongoConverter converter;
	private ApplicationContext context;

	private com.mongodb.client.MongoClient syncClient;
	private com.mongodb.reactivestreams.client.MongoClient reactiveClient;
	private MongoDatabaseFactory syncFactory;
	private SimpleReactiveMongoDatabaseFactory reactiveFactory;

	MongoConverter mongoConverter() {

		if (converter == null) {

			converter = new MappingMongoConverter(new DefaultDbRefResolver(databaseFactory()), mappingContext());

			if (mongoConverterConfigurer.customConversions != null) {
				converter.setCustomConversions(mongoConverterConfigurer.customConversions);
			}
			converter.afterPropertiesSet();
		}

		return converter;
	}

	List<ApplicationListener<?>> getApplicationEventListener() {

		ArrayList<ApplicationListener<?>> listeners = new ArrayList<>(applicationContextConfigurer.listeners);
		if (auditingConfigurer.hasAuditingHandler()) {
			listeners.add(new AuditingEventListener(() -> auditingConfigurer.auditingHandlers(mappingContext())));
		}
		return listeners;
	}

	@Nullable
	ApplicationContext getApplicationContext() {

		if (applicationContextConfigurer.applicationContext == null) {
			return null;
		}

		if (context != null) {
			return context;
		}

		context = applicationContextConfigurer.applicationContext;

		if (context instanceof ConfigurableApplicationContext) {

			ConfigurableApplicationContext configurableApplicationContext = (ConfigurableApplicationContext) this.context;
			getApplicationEventListener().forEach(configurableApplicationContext::addApplicationListener);

			configurableApplicationContext.refresh();
		}
		return context;
	}

	MongoMappingContext mappingContext() {

		if (mappingContext == null) {

			mappingContext = new MongoMappingContext();
			mappingContext.setInitialEntitySet(mappingContextConfigurer.initialEntitySet());
			mappingContext.setAutoIndexCreation(mappingContextConfigurer.autocreateIndex);
			if(mongoConverterConfigurer.customConversions != null) {
				mappingContext.setSimpleTypeHolder(mongoConverterConfigurer.customConversions.getSimpleTypeHolder());
			}
			mappingContext.afterPropertiesSet();
		}

		return mappingContext;
	}

	MongoDatabaseFactory databaseFactory() {

		if (syncFactory == null) {
			syncFactory = new SimpleMongoClientDatabaseFactory(syncClient(), defaultDatabase());
		}

		return syncFactory;
	}

	ReactiveMongoDatabaseFactory reactiveDatabaseFactory() {

		if (reactiveFactory == null) {
			reactiveFactory = new SimpleReactiveMongoDatabaseFactory(reactiveClient(), defaultDatabase());
		}

		return reactiveFactory;
	}

	public MongoTestTemplateConfiguration configureDatabaseFactory(Consumer<DatabaseFactoryConfigurer> dbFactory) {

		dbFactory.accept(dbFactoryConfig);
		return this;
	}

	public MongoTestTemplateConfiguration configureMappingContext(
			Consumer<MappingContextConfigurer> mappingContextConfigurerConsumer) {
		mappingContextConfigurerConsumer.accept(mappingContextConfigurer);
		return this;
	}

	public MongoTestTemplateConfiguration configureApplicationContext(
			Consumer<ApplicationContextConfigurer> applicationContextConfigurerConsumer) {

		applicationContextConfigurerConsumer.accept(applicationContextConfigurer);
		return this;
	}

	public MongoTestTemplateConfiguration configureAuditing(Consumer<AuditingConfigurer> auditingConfigurerConsumer) {

		auditingConfigurerConsumer.accept(auditingConfigurer);
		return this;
	}

	public MongoTestTemplateConfiguration configureConversion(
			Consumer<MongoConverterConfigurer> mongoConverterConfigurerConsumer) {

		mongoConverterConfigurerConsumer.accept(mongoConverterConfigurer);
		return this;
	}

	com.mongodb.client.MongoClient syncClient() {

		if (syncClient == null) {
			syncClient = dbFactoryConfig.syncClient != null ? dbFactoryConfig.syncClient : MongoTestUtils.client();
		}

		return syncClient;
	}

	com.mongodb.reactivestreams.client.MongoClient reactiveClient() {

		if (reactiveClient == null) {
			reactiveClient = dbFactoryConfig.reactiveClient != null ? dbFactoryConfig.reactiveClient
					: MongoTestUtils.reactiveClient();
		}

		return reactiveClient;
	}

	String defaultDatabase() {
		return dbFactoryConfig.defaultDatabase != null ? dbFactoryConfig.defaultDatabase : "test";
	}

	public static class DatabaseFactoryConfigurer {

		com.mongodb.client.MongoClient syncClient;
		com.mongodb.reactivestreams.client.MongoClient reactiveClient;
		String defaultDatabase;

		public void client(com.mongodb.client.MongoClient client) {
			this.syncClient = client;
		}

		public void client(com.mongodb.reactivestreams.client.MongoClient client) {
			this.reactiveClient = client;
		}

		public void defaultDb(String defaultDatabase) {
			this.defaultDatabase = defaultDatabase;
		}
	}

	public static class MongoConverterConfigurer {

		CustomConversions customConversions;

		public void customConversions(CustomConversions customConversions) {
			this.customConversions = customConversions;
		}

		public void customConverters(Converter<?, ?>... converters) {
			customConversions(new MongoCustomConversions(Arrays.asList(converters)));
		}
	}

	public static class MappingContextConfigurer {

		Set<Class<?>> intitalEntitySet;
		boolean autocreateIndex = false;

		public void autocreateIndex(boolean autocreateIndex) {
			this.autocreateIndex = autocreateIndex;
		}

		public void intitalEntitySet(Set<Class<?>> intitalEntitySet) {
			this.intitalEntitySet = intitalEntitySet;
		}

		public void intitalEntitySet(Class<?>... initialEntitySet) {
			this.intitalEntitySet = new HashSet<>(Arrays.asList(initialEntitySet));
		}

		Set<Class<?>> initialEntitySet() {
			return intitalEntitySet != null ? intitalEntitySet : Collections.emptySet();
		}
	}

	public static class AuditingConfigurer {

		Function<MappingContext, IsNewAwareAuditingHandler> auditingHandlerFunction;

		public void auditingHandler(Function<MappingContext, IsNewAwareAuditingHandler> auditingHandlerFunction) {
			this.auditingHandlerFunction = auditingHandlerFunction;
		}

		IsNewAwareAuditingHandler auditingHandlers(MongoMappingContext mongoMappingContext) {
			return auditingHandlerFunction.apply(mongoMappingContext);
		}

		boolean hasAuditingHandler() {
			return auditingHandlerFunction != null;
		}
	}

	public static class ApplicationContextConfigurer {

		List<ApplicationListener<MongoMappingEvent<?>>> listeners = new ArrayList<>();
		ApplicationContext applicationContext;

		public void applicationContext(ApplicationContext context) {
			this.applicationContext = context;
		}

		public void addEventListener(ApplicationListener<MongoMappingEvent<?>> listener) {
			this.listeners.add(listener);
		}
	}
}
