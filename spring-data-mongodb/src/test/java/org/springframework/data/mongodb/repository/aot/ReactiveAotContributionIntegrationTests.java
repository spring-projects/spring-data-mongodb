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

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.assertThatJson;
import static org.mockito.Mockito.mock;

import example.aot.User;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.springframework.aot.generate.GeneratedFiles;
import org.springframework.aot.test.generate.TestGenerationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.aot.ApplicationContextAotGenerator;
import org.springframework.core.io.InputStreamResource;
import org.springframework.core.io.InputStreamSource;
import org.springframework.data.aot.AotContext;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.repository.config.EnableReactiveMongoRepositories;
import org.springframework.data.querydsl.ReactiveQuerydslPredicateExecutor;
import org.springframework.data.repository.CrudRepository;
import org.springframework.mock.env.MockPropertySource;

import com.mongodb.client.MongoClient;

/**
 * Integration tests for AOT processing of reactive repositories.
 *
 * @author Mark Paluch
 */
class ReactiveAotContributionIntegrationTests {

	@EnableReactiveMongoRepositories(considerNestedRepositories = true, includeFilters = {
			@ComponentScan.Filter(type = FilterType.ASSIGNABLE_TYPE, value = ReactiveQuerydslUserRepository.class) })
	static class AotConfiguration extends AbstractMongoClientConfiguration {

		@Override
		public MongoClient mongoClient() {
			return mock(MongoClient.class);
		}

		@Override
		protected String getDatabaseName() {
			return "";
		}
	}

	interface ReactiveQuerydslUserRepository
			extends CrudRepository<User, String>, ReactiveQuerydslPredicateExecutor<User> {

		Flux<User> findUserNoArgumentsBy();

		Mono<User> findOneByUsername(String username);

	}

	@Test // GH-4964
	void shouldGenerateMetadataForBaseRepositoryAndQuerydslFragment() throws IOException {

		TestGenerationContext generationContext = generate(AotConfiguration.class);

		InputStreamSource metadata = generationContext.getGeneratedFiles().getGeneratedFile(GeneratedFiles.Kind.RESOURCE,
				ReactiveQuerydslUserRepository.class.getName().replace('.', '/') + ".json");

		InputStreamResource isr = new InputStreamResource(metadata);
		String json = isr.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).isObject() //
				.containsEntry("name", ReactiveQuerydslUserRepository.class.getName()) //
				.containsEntry("module", "MongoDB") //
				.containsEntry("type", "REACTIVE");

		assertThatJson(json).inPath("$.methods[?(@.name == 'findBy')].fragment").isArray().first().isObject()
				.containsEntry("interface", "org.springframework.data.querydsl.ReactiveQuerydslPredicateExecutor")
				.containsEntry("fragment",
						"org.springframework.data.mongodb.repository.support.ReactiveQuerydslMongoPredicateExecutor");

		assertThatJson(json).inPath("$.methods[?(@.name == 'existsById')].fragment").isArray().first().isObject()
				.containsEntry("fragment", "org.springframework.data.mongodb.repository.support.SimpleReactiveMongoRepository");
	}

	private static TestGenerationContext generate(Class<?>... configurationClasses) {

		AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();
		context.getEnvironment().getPropertySources()
				.addFirst(new MockPropertySource().withProperty(AotContext.GENERATED_REPOSITORIES_ENABLED, "true"));
		context.register(configurationClasses);

		ApplicationContextAotGenerator generator = new ApplicationContextAotGenerator();

		TestGenerationContext generationContext = new TestGenerationContext();
		generator.processAheadOfTime(context, generationContext);
		generationContext.writeGeneratedContent();
		return generationContext;
	}

}
