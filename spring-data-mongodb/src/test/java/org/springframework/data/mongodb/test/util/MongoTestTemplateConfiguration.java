/*
 * Copyright 2020-2023 the original author or authors.
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
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.springframework.beans.BeansException;
import org.springframework.beans.factory.ObjectFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationListener;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.auditing.IsNewAwareAuditingHandler;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.SimpleReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.core.mapping.event.AuditingEntityCallback;
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

			if (dbFactoryConfig.syncClient != null || syncClient != null) {
				converter = new MappingMongoConverter(new DefaultDbRefResolver(databaseFactory()), mappingContext());
			} else {
				converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, mappingContext());
			}

			if (mongoConverterConfigurer.customConversions != null) {
				converter.setCustomConversions(mongoConverterConfigurer.customConversions);
			}
			if (auditingConfigurer.hasAuditingHandler()) {
				converter.setEntityCallbacks(getEntityCallbacks());
			}
			converter.afterPropertiesSet();
		}

		return converter;
	}

	EntityCallbacks getEntityCallbacks() {

		EntityCallbacks callbacks = null;
		if (getApplicationContext() != null) {
			callbacks = EntityCallbacks.create(getApplicationContext());
		}
		if (!auditingConfigurer.hasAuditingHandler()) {
			return callbacks;
		}
		if (callbacks == null) {
			callbacks = EntityCallbacks.create();
		}

		callbacks.addEntityCallback(new AuditingEntityCallback(new ObjectFactory<IsNewAwareAuditingHandler>() {
			@Override
			public IsNewAwareAuditingHandler getObject() throws BeansException {
				return auditingConfigurer.auditingHandlerFunction.apply(converter.getMappingContext());
			}
		}));
		return callbacks;

	}

	List<ApplicationListener<?>> getApplicationEventListener() {
		return new ArrayList<>(applicationContextConfigurer.listeners);
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
			mappingContext = new MongoTestMappingContext(mappingContextConfigurer).customConversions(mongoConverterConfigurer)
					.init();
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
