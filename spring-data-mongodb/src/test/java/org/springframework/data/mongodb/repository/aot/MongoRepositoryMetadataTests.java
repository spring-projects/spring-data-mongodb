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

import static net.javacrumbs.jsonunit.assertj.JsonAssertions.*;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;

import example.aot.UserRepository;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.test.util.MongoClientExtension;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

/**
 * Integration tests for the {@link UserRepository} JSON metadata via {@link MongoRepositoryContributor}.
 *
 * @author Mark Paluch
 */
@ExtendWith(MongoClientExtension.class)
@SpringJUnitConfig(classes = MongoRepositoryMetadataTests.MongoRepositoryContributorConfiguration.class)
class MongoRepositoryMetadataTests {

	@Configuration
	static class MongoRepositoryContributorConfiguration extends AotFragmentTestConfigurationSupport {

		public MongoRepositoryContributorConfiguration() {
			super(UserRepository.class);
		}

		@Bean
		MongoOperations mongoOperations() {
			return mock(MongoOperations.class);
		}

	}

	@Autowired AbstractApplicationContext context;

	@Test // GH-4964
	void shouldDocumentBase() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).isObject() //
				.containsEntry("name", UserRepository.class.getName()) //
				.containsEntry("module", "MongoDB") //
				.containsEntry("type", "IMPERATIVE");
	}

	@Test // GH-4964
	void shouldDocumentDerivedQuery() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'countUsersByLastname')].query").isArray().element(0).isObject()
				.containsEntry("filter", "{'lastname':?0}");
	}

	@Test // GH-4964
	void shouldDocumentSortedQuery() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'findByLastnameStartingWithOrderByUsername')].query") //
				.isArray().element(0).isObject() //
				.containsEntry("filter", "{'lastname':{'$regex':/^\\Q?0\\E/}}")
				.containsEntry("sort", "{'username':{'$numberInt':'1'}}");
	}

	@Test // GH-4964
	void shouldDocumentPagedQuery() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'findPageOfUsersByLastnameStartingWith')].query").isArray()
				.element(0).isObject().containsEntry("filter", "{'lastname':{'$regex':/^\\Q?0\\E/}}");
	}

	@Test // GH-4964
	@Disabled("No support for expressions yet")
	void shouldDocumentQueryWithExpression() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'findValueExpressionNamedByEmailAddress')].query").isArray()
				.first().isObject().containsEntry("query", "select u from User u where u.emailAddress = :__$synthetic$__1");
	}

	@Test // GH-4964
	void shouldDocumentAggregation() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'findAllLastnames')].query").isArray().element(0).isObject()
				.containsEntry("pipeline",
						List.of("{ '$match' : { 'last_name' : { '$ne' : null } } }", "{ '$project': { '_id' : '$last_name' } }"));
	}

	@Test // GH-4964
	void shouldDocumentPipelineUpdate() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'findAndIncrementVisitsViaPipelineByLastname')].query").isArray()
				.element(0).isObject().containsEntry("filter", "{'lastname':?0}").containsEntry("update-pipeline",
						List.of("{ '$set' : { 'visits' : { '$ifNull' : [ {'$add' : [ '$visits', ?1 ] }, ?1 ] } } }"));
	}

	@Test // GH-4964
	void shouldDocumentBaseFragment() throws IOException {

		Resource resource = getResource();

		assertThat(resource).isNotNull();
		assertThat(resource.exists()).isTrue();

		String json = resource.getContentAsString(StandardCharsets.UTF_8);

		assertThatJson(json).inPath("$.methods[?(@.name == 'existsById')].fragment").isArray().first().isObject()
				.containsEntry("fragment", "org.springframework.data.mongodb.repository.support.SimpleMongoRepository");
	}

	private Resource getResource() {

		String location = UserRepository.class.getPackageName().replace('.', '/') + "/"
				+ UserRepository.class.getSimpleName() + ".json";
		return new UrlResource(context.getBeanFactory().getBeanClassLoader().getResource(location));
	}

}
