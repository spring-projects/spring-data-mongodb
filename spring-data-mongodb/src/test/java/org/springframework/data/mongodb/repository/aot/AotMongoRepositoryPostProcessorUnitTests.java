/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.lang.annotation.Annotation;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import org.jmolecules.ddd.annotation.Entity;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junitpioneer.jupiter.SetSystemProperty;

import org.springframework.aot.AotDetector;
import org.springframework.aot.generate.ClassNameGenerator;
import org.springframework.aot.generate.DefaultGenerationContext;
import org.springframework.aot.generate.GenerationContext;
import org.springframework.aot.generate.InMemoryGeneratedFiles;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.data.annotation.Id;
import org.springframework.data.aot.AotContext;
import org.springframework.data.aot.AotTypeConfiguration;
import org.springframework.data.mongodb.repository.support.SimpleMongoRepository;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.config.AotRepositoryContext;
import org.springframework.data.repository.config.AotRepositoryInformation;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.data.repository.core.RepositoryInformation;
import org.springframework.data.repository.core.support.AbstractRepositoryMetadata;
import org.springframework.javapoet.ClassName;

/**
 * Unit tests for {@link AotMongoRepositoryPostProcessor}.
 *
 * @author Mark Paluch
 */
class AotMongoRepositoryPostProcessorUnitTests {

	@Test // GH-4893
	@SetSystemProperty(key = AotDetector.AOT_ENABLED, value = "true")
	void repositoryProcessorShouldEnableAotRepositoriesByDefaultWhenAotIsEnabled() {

		GenerationContext ctx = createGenerationContext();
		GenericApplicationContext context = new GenericApplicationContext();

		MongoRepositoryContributor contributor = createContributorWithPersonTypes(context, ctx);

		assertThat(contributor).isNotNull();
	}

	@Test // GH-4893
	@ClearSystemProperty(key = AotContext.GENERATED_REPOSITORIES_ENABLED)
	void shouldEnableAotRepositoriesByDefault() {

		GenerationContext ctx = createGenerationContext();
		GenericApplicationContext context = new GenericApplicationContext();

		MongoRepositoryContributor contributor = createContributorWithPersonTypes(context, ctx);

		assertThat(contributor).isNotNull();
	}

	@Test // GH-4893
	@SetSystemProperty(key = AotContext.GENERATED_REPOSITORIES_ENABLED, value = "false")
	void shouldDisableAotRepositoriesWhenGeneratedRepositoriesIsFalse() {

		GenerationContext ctx = createGenerationContext();
		GenericApplicationContext context = new GenericApplicationContext();

		MongoRepositoryContributor contributor = createContributorWithPersonTypes(context, ctx);

		assertThat(contributor).isNull();
	}

	@Test // GH-3899
	@SetSystemProperty(key = "spring.aot.mongodb.repositories.enabled", value = "false")
	void shouldDisableAotRepositoriesWhenJpaGeneratedRepositoriesIsFalse() {

		GenerationContext ctx = createGenerationContext();
		GenericApplicationContext context = new GenericApplicationContext();

		MongoRepositoryContributor contributor = createContributorWithPersonTypes(context, ctx);

		assertThat(contributor).isNull();
	}

	private GenerationContext createGenerationContext() {
		return new DefaultGenerationContext(new ClassNameGenerator(ClassName.OBJECT), new InMemoryGeneratedFiles());
	}

	private MongoRepositoryContributor createContributorWithPersonTypes(GenericApplicationContext context,
			GenerationContext ctx) {

		return new AotMongoRepositoryPostProcessor().contribute(new DummyAotRepositoryContext(context) {
			@Override
			public Set<Class<?>> getResolvedTypes() {
				return Collections.singleton(Person.class);
			}
		}, ctx);
	}

	@Entity
	static class Person {
		@Id Long id;
	}

	interface PersonRepository extends Repository<Person, Long> {}

	static class DummyAotRepositoryContext implements AotRepositoryContext {

		private final @Nullable AbstractApplicationContext applicationContext;

		DummyAotRepositoryContext(@Nullable AbstractApplicationContext applicationContext) {
			this.applicationContext = applicationContext;
		}

		@Override
		public String getBeanName() {
			return "jpaRepository";
		}

		@Override
		public String getModuleName() {
			return "JPA";
		}

		@Override
		public RepositoryConfigurationSource getConfigurationSource() {
			return mock(RepositoryConfigurationSource.class);
		}

		@Override
		public Set<String> getBasePackages() {
			return Collections.singleton(this.getClass().getPackageName());
		}

		@Override
		public Set<Class<? extends Annotation>> getIdentifyingAnnotations() {
			return Collections.singleton(Entity.class);
		}

		@Override
		public RepositoryInformation getRepositoryInformation() {
			return new AotRepositoryInformation(AbstractRepositoryMetadata.getMetadata(PersonRepository.class),
					SimpleMongoRepository.class, List.of());
		}

		@Override
		public Set<MergedAnnotation<Annotation>> getResolvedAnnotations() {
			return Set.of();
		}

		@Override
		public Set<Class<?>> getResolvedTypes() {
			return Set.of();
		}

		@Override
		public Set<Class<?>> getUserDomainTypes() {
			return Set.of();
		}

		@Override
		public ConfigurableListableBeanFactory getBeanFactory() {
			return applicationContext != null ? applicationContext.getBeanFactory() : null;
		}

		@Override
		public Environment getEnvironment() {
			return applicationContext == null ? new StandardEnvironment() : applicationContext.getEnvironment();
		}

		@Override
		public TypeIntrospector introspectType(String typeName) {
			return null;
		}

		@Override
		public IntrospectedBeanDefinition introspectBeanDefinition(String beanName) {
			return null;
		}

		@Override
		public void typeConfiguration(Class<?> type, Consumer<AotTypeConfiguration> configurationConsumer) {

		}

		@Override
		public Collection<AotTypeConfiguration> typeConfigurations() {
			return List.of();
		}
	}

}
