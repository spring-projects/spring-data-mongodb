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
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.jmolecules.ddd.annotation.Entity;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.ClearSystemProperty;
import org.junitpioneer.jupiter.SetSystemProperty;

import org.springframework.aot.AotDetector;
import org.springframework.aot.generate.ClassNameGenerator;
import org.springframework.aot.generate.DefaultGenerationContext;
import org.springframework.aot.generate.GenerationContext;
import org.springframework.aot.generate.InMemoryGeneratedFiles;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.annotation.MergedAnnotation;
import org.springframework.data.annotation.Id;
import org.springframework.data.aot.AotContext;
import org.springframework.data.mongodb.repository.ReactivePersonRepository;
import org.springframework.data.mongodb.repository.support.SimpleMongoRepository;
import org.springframework.data.mongodb.repository.support.SimpleReactiveMongoRepository;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.config.AotRepositoryContextSupport;
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

		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();

		MongoRepositoryContributor contributor = createContributorWithPersonTypes(context);

		assertThat(contributor).isNotNull();
	}

	@Test // GH-4893
	@ClearSystemProperty(key = AotContext.GENERATED_REPOSITORIES_ENABLED)
	void shouldEnableAotRepositoriesByDefault() {

		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();

		MongoRepositoryContributor contributor = createContributorWithPersonTypes(context);

		assertThat(contributor).isNotNull();
	}

	@Test // GH-4893
	@SetSystemProperty(key = AotContext.GENERATED_REPOSITORIES_ENABLED, value = "false")
	void shouldDisableAotRepositoriesWhenGeneratedRepositoriesIsFalse() {

		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();

		MongoRepositoryContributor contributor = createContributorWithPersonTypes(context);

		assertThat(contributor).isNull();
	}

	@Test // GH-3899
	@SetSystemProperty(key = "spring.aot.mongodb.repositories.enabled", value = "false")
	void shouldDisableAotRepositoriesWhenJpaGeneratedRepositoriesIsFalse() {

		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();

		MongoRepositoryContributor contributor = createContributorWithPersonTypes(context);

		assertThat(contributor).isNull();
	}

	@Test  // GH-5068
	void shouldNotAttemptToContributeCodeForReactiveRepository(){

		GenericApplicationContext context = new GenericApplicationContext();
		context.refresh();

		MongoRepositoryContributor contributor = createContributorForReactiveRepo(context);

		assertThat(contributor).isNull();
	}

	private GenerationContext createGenerationContext() {
		return new DefaultGenerationContext(new ClassNameGenerator(ClassName.OBJECT), new InMemoryGeneratedFiles());
	}

	private MongoRepositoryContributor createContributorForReactiveRepo(GenericApplicationContext context) {
		return createContributorWithPersonTypes(context, true);
	}

	private MongoRepositoryContributor createContributorWithPersonTypes(GenericApplicationContext context) {
		return createContributorWithPersonTypes(context, false);
	}

	private MongoRepositoryContributor createContributorWithPersonTypes(GenericApplicationContext context, boolean reactive) {

		return new AotMongoRepositoryPostProcessor().contributeAotRepository(new DummyAotRepositoryContext(context, reactive) {
			@Override
			public Set<Class<?>> getResolvedTypes() {
				return Collections.singleton(Person.class);
			}
		});
	}

	@Entity
	static class Person {
		@Id Long id;
	}

	interface PersonRepository extends Repository<Person, Long> {}

	static class DummyAotRepositoryContext extends AotRepositoryContextSupport {

		boolean reactive;

		DummyAotRepositoryContext(AbstractApplicationContext applicationContext, boolean reactive) {
			super(AotContext.from(applicationContext, applicationContext.getEnvironment()));
			this.reactive = reactive;
		}

		@Override
		public String getModuleName() {
			return "MongoDB";
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
			if(reactive) {
				return new AotRepositoryInformation(AbstractRepositoryMetadata.getMetadata(ReactivePersonRepository.class),
					SimpleReactiveMongoRepository.class, List.of());
			}
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

	}

}
