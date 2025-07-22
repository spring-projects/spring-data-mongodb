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
package org.springframework.data.mongodb.repository;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.regex.Pattern;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.domain.Limit;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.ScrollPosition;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.domain.Window;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.TestMongoConfiguration;
import org.springframework.data.mongodb.repository.aot.AotFragmentTestConfigurationSupport;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;
import org.springframework.data.mongodb.test.util.DirtiesStateExtension.DirtiesState;
import org.springframework.data.repository.core.support.RepositoryComposition;
import org.springframework.test.context.ContextConfiguration;

/**
 * Integration tests for {@link PersonRepository} with mounted AOT-generated repository methods.
 *
 * @author Mark Paluch
 */
@ContextConfiguration(classes = AotPersonRepositoryIntegrationTests.Config.class)
// @Disabled("Several mismatches, some class-loader visibility issues and some behavioral differences remain to be
// fixed")
class AotPersonRepositoryIntegrationTests extends AbstractPersonRepositoryIntegrationTests {

	@Configuration
	@ImportResource("classpath:/org/springframework/data/mongodb/repository/PersonRepositoryIntegrationTests-infrastructure.xml")
	static class Config extends TestMongoConfiguration {

		@Bean
		static AotFragmentTestConfigurationSupport aot() {
			return new AotFragmentTestConfigurationSupport(PersonRepository.class, false);
		}

		@Bean
		PersonRepository personRepository(MongoOperations mongoOperations, ApplicationContext context) {

			MongoRepositoryFactory factory = new MongoRepositoryFactory(mongoOperations);
			factory.setBeanFactory(context);
			factory.setBeanClassLoader(context.getClassLoader());
			factory.setEnvironment(context.getEnvironment());

			Object aotFragment = context.getBean("fragment");

			return factory.getRepository(PersonRepository.class, RepositoryComposition.RepositoryFragments.just(aotFragment));
		}

	}

	@Test // GH-4397
	@Override
	void executesFinderCorrectlyWithSortAndLimit() {

		List<Person> page = repository.findByLastnameLike(Pattern.compile(".*a.*"),
				Sort.by(Direction.ASC, "lastname", "firstname"), Limit.of(2));

		assertThat(page).containsExactly(carter, stefan);
	}

	@Test
	@Override
	void findsPersonsByFirstnameLike() {

		List<Person> result = repository.findByFirstnameLike(Pattern.compile("Bo.*"));
		assertThat(result).hasSize(1).contains(boyd);
	}

	@Test // DATAMONGO-1424
	@Override
	void findsPersonsByFirstnameNotLike() {

		List<Person> result = repository.findByFirstnameNotLike(Pattern.compile("Bo.*"));
		assertThat(result).hasSize((int) (repository.count() - 1));
		assertThat(result).doesNotContain(boyd);
	}

	@Test // GH-4308
	@Override
	void appliesScrollPositionCorrectly() {

		Window<Person> page = repository.findTop2ByLastnameLikeOrderByLastnameAscFirstnameAsc(Pattern.compile(".*a.*"),
				ScrollPosition.keyset());

		assertThat(page.isLast()).isFalse();
		assertThat(page.size()).isEqualTo(2);
		assertThat(page).contains(carter);
	}

	@Test // GH-4397
	@Override
	void appliesLimitToScrollingCorrectly() {

		Window<Person> page = repository.findByLastnameLikeOrderByLastnameAscFirstnameAsc(Pattern.compile(".*a.*"),
				ScrollPosition.keyset(), Limit.of(2));

		assertThat(page.isLast()).isFalse();
		assertThat(page.size()).isEqualTo(2);
		assertThat(page).contains(carter);
	}

	@Test // GH-4308
	@Disabled
	void appliesScrollPositionWithProjectionCorrectly() {

		Window<PersonSummaryDto> page = repository.findCursorProjectionByLastnameLike(Pattern.compile(".*a.*"),
				PageRequest.of(0, 2, Sort.by(Direction.ASC, "lastname", "firstname")));

		assertThat(page.isLast()).isFalse();
		assertThat(page.size()).isEqualTo(2);

		assertThat(page).element(0).isEqualTo(new PersonSummaryDto(carter.getFirstname(), carter.getLastname()));
	}

	@Test // DATADOC-236
	@Override
	void appliesStaticAndDynamicSorting() {
		List<Person> result = repository.findByFirstnameLikeOrderByLastnameAsc(Pattern.compile(".*e.*"), Sort.by("age"));
		assertThat(result).hasSize(5);
		assertThat(result.get(0)).isEqualTo(carter);
		assertThat(result.get(1)).isEqualTo(stefan);
		assertThat(result.get(2)).isEqualTo(oliver);
		assertThat(result.get(3)).isEqualTo(dave);
		assertThat(result.get(4)).isEqualTo(leroi);
	}

	@Test // DATAMONGO-1608
	@Disabled
	void findByFirstnameLikeWithNull() {
		super.findByFirstnameLikeWithNull();
	}

	@Test // GH-3395
	@Disabled
	void caseInSensitiveInClauseQuotesExpressions() {
		super.caseInSensitiveInClauseQuotesExpressions();
	}

	@Test // GH-4839
	@Disabled
	void annotatedAggregationWithAggregationResultAsClosedInterfaceProjection() {
		super.annotatedAggregationWithAggregationResultAsClosedInterfaceProjection();
	}

	@Test // DATAMONGO-1608
	@Disabled
	void findByFirstNameIgnoreCaseWithNull() {
		super.findByFirstNameIgnoreCaseWithNull();
	}

	@Test // GH-3395
	@Disabled
	void caseSensitiveInClauseIgnoresExpressions() {
		super.caseSensitiveInClauseIgnoresExpressions();
	}

	@Test // GH-3656
	@DirtiesState
	@Disabled
	void resultProjectionWithOptionalIsExecutedCorrectly() {
		super.resultProjectionWithOptionalIsExecutedCorrectly();
	}

	@Test
	@Override
	void executesPagedFinderCorrectly() {

		Page<Person> page = repository.findByLastnameLike(Pattern.compile(".*a.*"),
				PageRequest.of(0, 2, Direction.ASC, "lastname", "firstname"));
		assertThat(page.isFirst()).isTrue();
		assertThat(page.isLast()).isFalse();
		assertThat(page.getNumberOfElements()).isEqualTo(2);
		assertThat(page).contains(carter, stefan);
	}

	@Test // DATAMONGO-990
	@Disabled
	void shouldFindByFirstnameAndCurrentUserWithCustomQuery() {
		super.shouldFindByFirstnameAndCurrentUserWithCustomQuery();
	}

	@Test // GH-3395, GH-4404
	@Disabled
	void caseInSensitiveInClause() {
		super.caseInSensitiveInClause();
	}

}
