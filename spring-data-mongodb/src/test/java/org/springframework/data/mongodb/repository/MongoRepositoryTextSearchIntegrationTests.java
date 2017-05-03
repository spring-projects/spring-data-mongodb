/*
 * Copyright 2014-2017 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import static org.hamcrest.collection.IsCollectionWithSize.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsEqual.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.TextIndexDefinition.TextIndexDefinitionBuilder;
import org.springframework.data.mongodb.core.index.TextIndexed;
import org.springframework.data.mongodb.core.mapping.TextScore;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;
import org.springframework.data.mongodb.test.util.MongoVersionRule;
import org.springframework.data.util.Version;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;

import com.mongodb.MongoClient;

/**
 * Integration tests for text searches on repository.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Mark Paluch
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration
public class MongoRepositoryTextSearchIntegrationTests {

	public static @ClassRule MongoVersionRule versionRule = MongoVersionRule.atLeast(new Version(2, 6, 0));

	private static final FullTextDocument PASSENGER_57 = new FullTextDocument("1", "Passenger 57",
			"Passenger 57 is an action film that stars Wesley Snipes and Bruce Payne.");
	private static final FullTextDocument DEMOLITION_MAN = new FullTextDocument("2", "Demolition Man",
			"Demolition Man is a science fiction action comedy film staring Wesley Snipes and Sylvester Stallone.");
	private static final FullTextDocument DROP_ZONE = new FullTextDocument("3", "Drop Zone",
			"Drop Zone is an action film featuring Wesley Snipes and Gary Busey.");

	@Autowired MongoTemplate template;
	FullTextRepository repo;

	@Before
	public void setUp() {

		template.indexOps(FullTextDocument.class)
				.ensureIndex(new TextIndexDefinitionBuilder().onField("title").onField("content").build());
		this.repo = new MongoRepositoryFactory(this.template).getRepository(FullTextRepository.class);
	}

	@After
	public void tearDown() {
		template.dropCollection(FullTextDocument.class);
	}

	@Test // DATAMONGO-973
	public void findAllByTextCriteriaShouldReturnMatchingDocuments() {

		initRepoWithDefaultDocuments();

		List<FullTextDocument> result = repo.findAllBy(TextCriteria.forDefaultLanguage().matchingAny("stallone", "payne"));

		assertThat(result, hasSize(2));
		assertThat(result, hasItems(PASSENGER_57, DEMOLITION_MAN));
	}

	@Test // DATAMONGO-973
	public void derivedFinderWithTextCriteriaReturnsCorrectResult() {

		initRepoWithDefaultDocuments();
		FullTextDocument blade = new FullTextDocument("4", "Blade",
				"Blade is a 1998 American vampire-superhero-vigilante action film starring Wesley Snipes and Stephen Dorff, loosely based on the Marvel Comics character Blade");
		blade.nonTextIndexProperty = "foo";
		repo.save(blade);

		List<FullTextDocument> result = repo.findByNonTextIndexProperty("foo",
				TextCriteria.forDefaultLanguage().matching("snipes"));

		assertThat(result, hasSize(1));
		assertThat(result, hasItems(blade));
	}

	@Test // DATAMONGO-973
	public void findByWithPaginationWorksCorrectlyWhenUsingTextCriteria() {

		initRepoWithDefaultDocuments();

		Page<FullTextDocument> page = repo.findAllBy(TextCriteria.forDefaultLanguage().matching("film"),
				PageRequest.of(1, 1, Direction.ASC, "id"));

		assertThat(page.hasNext(), is(true));
		assertThat(page.hasPrevious(), is(true));
		assertThat(page.getTotalElements(), is(3L));
		assertThat(page.getContent().get(0), equalTo(DEMOLITION_MAN));
	}

	@Test // DATAMONGO-973
	public void findAllByTextCriteriaWithSortWorksCorrectly() {

		initRepoWithDefaultDocuments();
		FullTextDocument snipes = new FullTextDocument("4", "Snipes", "Wesley Trent Snipes is an actor and film producer.");
		repo.save(snipes);

		List<FullTextDocument> result = repo.findAllBy(TextCriteria.forDefaultLanguage().matching("snipes"),
				Sort.by("score"));

		assertThat(result.size(), is(4));
		assertThat(result.get(0), equalTo(snipes));
	}

	@Test // DATAMONGO-973
	public void findByWithSortByScoreViaPageRequestTriggersSortingCorrectly() {

		initRepoWithDefaultDocuments();
		FullTextDocument snipes = new FullTextDocument("4", "Snipes", "Wesley Trent Snipes is an actor and film producer.");
		repo.save(snipes);

		Page<FullTextDocument> page = repo.findAllBy(TextCriteria.forDefaultLanguage().matching("snipes"),
				PageRequest.of(0, 10, Direction.ASC, "score"));

		assertThat(page.getTotalElements(), is(4L));
		assertThat(page.getContent().get(0), equalTo(snipes));
	}

	@Test // DATAMONGO-973
	public void findByWithSortViaPageRequestIgnoresTextScoreWhenSortedByOtherProperty() {

		initRepoWithDefaultDocuments();
		FullTextDocument snipes = new FullTextDocument("4", "Snipes", "Wesley Trent Snipes is an actor and film producer.");
		repo.save(snipes);

		Page<FullTextDocument> page = repo.findAllBy(TextCriteria.forDefaultLanguage().matching("snipes"),
				PageRequest.of(0, 10, Direction.ASC, "id"));

		assertThat(page.getTotalElements(), is(4L));
		assertThat(page.getContent().get(0), equalTo(PASSENGER_57));
	}

	@Test // DATAMONGO-973
	public void derivedSortForTextScorePropertyWorksCorrectly() {

		initRepoWithDefaultDocuments();
		FullTextDocument snipes = new FullTextDocument("4", "Snipes", "Wesley Trent Snipes is an actor and film producer.");
		repo.save(snipes);

		List<FullTextDocument> result = repo
				.findByNonTextIndexPropertyIsNullOrderByScoreDesc(TextCriteria.forDefaultLanguage().matching("snipes"));
		assertThat(result.get(0), equalTo(snipes));
	}

	@Test // DATAMONGO-973
	public void derivedFinderMethodWithoutFullTextShouldNoCauseTroubleWhenHavingEntityWithTextScoreProperty() {

		initRepoWithDefaultDocuments();
		List<FullTextDocument> result = repo.findByTitle(DROP_ZONE.getTitle());
		assertThat(result.get(0), equalTo(DROP_ZONE));
		assertThat(result.get(0).score, equalTo(0.0F));
	}

	private void initRepoWithDefaultDocuments() {
		repo.saveAll(Arrays.asList(PASSENGER_57, DEMOLITION_MAN, DROP_ZONE));
	}

	@org.springframework.context.annotation.Configuration
	public static class Configuration extends AbstractMongoConfiguration {

		@Override
		protected String getDatabaseName() {
			return ClassUtils.getShortNameAsProperty(MongoRepositoryTextSearchIntegrationTests.class);
		}

		@Override
		public MongoClient mongoClient() {
			return new MongoClient();
		}

	}

	static class FullTextDocument {

		private @Id String id;
		private @TextIndexed String title;
		private @TextIndexed String content;
		String nonTextIndexProperty;
		@TextScore Float score;

		public FullTextDocument() {

		}

		public FullTextDocument(String id, String title, String content) {

			this.id = id;
			this.title = title;
			this.content = content;
		}

		public String getId() {
			return id;
		}

		public void setId(String id) {
			this.id = id;
		}

		public String getTitle() {
			return title;
		}

		public void setTitle(String title) {
			this.title = title;
		}

		public String getContent() {
			return content;
		}

		public void setContent(String content) {
			this.content = content;
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(this.id);
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof FullTextDocument)) {
				return false;
			}
			FullTextDocument other = (FullTextDocument) obj;
			return ObjectUtils.nullSafeEquals(this.id, other.id);
		}

	}

	static interface FullTextRepository extends MongoRepository<FullTextDocument, String> {

		List<FullTextDocument> findByNonTextIndexProperty(String nonTextIndexProperty, TextCriteria criteria);

		List<FullTextDocument> findByNonTextIndexPropertyIsNullOrderByScoreDesc(TextCriteria criteria);

		List<FullTextDocument> findByTitle(String title);

		List<FullTextDocument> findAllBy(TextCriteria textCriteria);

		List<FullTextDocument> findAllBy(TextCriteria textCriteria, Sort sort);

		Page<FullTextDocument> findAllBy(TextCriteria textCriteria, Pageable pageable);
	}
}
