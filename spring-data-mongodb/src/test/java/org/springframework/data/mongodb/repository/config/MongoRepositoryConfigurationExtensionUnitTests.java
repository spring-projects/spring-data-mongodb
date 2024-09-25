/*
 * Copyright 2014-2024 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.util.Collection;

import org.junit.jupiter.api.Test;

import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ResourceLoader;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.core.type.StandardAnnotationMetadata;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfiguration;
import org.springframework.data.repository.config.RepositoryConfigurationSource;

/**
 * Unit tests for {@link MongoRepositoryConfigurationExtension}.
 *
 * @author Oliver Gierke
 * @since 1.6
 */
public class MongoRepositoryConfigurationExtensionUnitTests {

	StandardAnnotationMetadata metadata = new StandardAnnotationMetadata(Config.class, true);
	ResourceLoader loader = new PathMatchingResourcePatternResolver();
	Environment environment = new StandardEnvironment();
	BeanDefinitionRegistry registry = new DefaultListableBeanFactory();

	RepositoryConfigurationSource configurationSource = new AnnotationRepositoryConfigurationSource(metadata,
			EnableMongoRepositories.class, loader, environment, registry);

	@Test // DATAMONGO-1009
	public void isStrictMatchIfDomainTypeIsAnnotatedWithDocument() {

		MongoRepositoryConfigurationExtension extension = new MongoRepositoryConfigurationExtension();
		assertHasRepo(SampleRepository.class, extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	@Test // DATAMONGO-1009
	public void isStrictMatchIfRepositoryExtendsStoreSpecificBase() {

		MongoRepositoryConfigurationExtension extension = new MongoRepositoryConfigurationExtension();
		assertHasRepo(StoreRepository.class, extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	@Test // DATAMONGO-1009
	public void isNotStrictMatchIfDomainTypeIsNotAnnotatedWithDocument() {

		MongoRepositoryConfigurationExtension extension = new MongoRepositoryConfigurationExtension();
		assertDoesNotHaveRepo(UnannotatedRepository.class,
				extension.getRepositoryConfigurations(configurationSource, loader, true));
	}

	private static void assertHasRepo(Class<?> repositoryInterface,
			Collection<RepositoryConfiguration<RepositoryConfigurationSource>> configs) {

		for (RepositoryConfiguration<?> config : configs) {
			if (config.getRepositoryInterface().equals(repositoryInterface.getName())) {
				return;
			}
		}

		fail("Expected to find config for repository interface ".concat(repositoryInterface.getName()).concat(" but got ")
				.concat(configs.toString()));
	}

	private static void assertDoesNotHaveRepo(Class<?> repositoryInterface,
			Collection<RepositoryConfiguration<RepositoryConfigurationSource>> configs) {

		for (RepositoryConfiguration<?> config : configs) {
			if (config.getRepositoryInterface().equals(repositoryInterface.getName())) {
				fail("Expected not to find config for repository interface ".concat(repositoryInterface.getName()));
			}
		}
	}

	@EnableMongoRepositories(considerNestedRepositories = true)
	static class Config {

	}

	@Document
	static class Sample {}

	interface SampleRepository extends Repository<Sample, Long> {}

	interface UnannotatedRepository extends Repository<Object, Long> {}

	interface StoreRepository extends MongoRepository<Object, Long> {}
}
