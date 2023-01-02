/*
 * Copyright 2014-2023 the original author or authors.
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

import static org.assertj.core.api.Assertions.*;

import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.index.TextIndexDefinition.TextIndexDefinitionBuilder;
import org.springframework.data.mongodb.core.index.TextIndexed;
import org.springframework.data.mongodb.core.mapping.TextScore;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;
import org.springframework.lang.Nullable;
import org.springframework.util.ObjectUtils;

/**
 * Integration tests for text searches on repository.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Mark Paluch
 */
@ExtendWith(MongoTemplateExtension.class)
class MongoRepositoryTextSearchIntegrationTests {

	private static final FullTextDocument PASSENGER_57 = new FullTextDocument("1", "Passenger 57",
			"Passenger 57 is an action film that stars Wesley Snipes and Bruce Payne.");
	private static final FullTextDocument DEMOLITION_MAN = new FullTextDocument("2", "Demolition Man",
			"Demolition Man is a science fiction action comedy film staring Wesley Snipes and Sylvester Stallone.");
	private static final FullTextDocument DROP_ZONE = new FullTextDocument("3", "Drop Zone",
			"Drop Zone is an action film featuring Wesley Snipes and Gary Busey.");

	@Template(initialEntitySet = FullTextDocument.class) //
	private static MongoTestTemplate template;

	private FullTextRepository repo = new MongoRepositoryFactory(this.template).getRepository(FullTextRepository.class);

	@BeforeEach
	void setUp() {

		template.indexOps(FullTextDocument.class)
				.ensureIndex(new TextIndexDefinitionBuilder().onField("title").onField("content").build());
	}

	@AfterEach
	void tearDown() {
		template.flush();
	}

	@Test // DATAMONGO-973
	void findAllByTextCriteriaShouldReturnMatchingDocuments() {

		initRepoWithDefaultDocuments();

		List<FullTextDocument> result = repo.findAllBy(TextCriteria.forDefaultLanguage().matchingAny("stallone", "payne"));

		assertThat(result).hasSize(2);
		assertThat(result).contains(PASSENGER_57, DEMOLITION_MAN);
	}

	@Test // DATAMONGO-973
	void derivedFinderWithTextCriteriaReturnsCorrectResult() {

		initRepoWithDefaultDocuments();
		FullTextDocument blade = new FullTextDocument("4", "Blade",
				"Blade is a 1998-2018 American vampire-superhero-vigilante action film starring Wesley Snipes and Stephen Dorff, loosely based on the Marvel Comics character Blade");
		blade.nonTextIndexProperty = "foo";
		repo.save(blade);

		List<FullTextDocument> result = repo.findByNonTextIndexProperty("foo",
				TextCriteria.forDefaultLanguage().matching("snipes"));

		assertThat(result).hasSize(1);
		assertThat(result).contains(blade);
	}

	@Test // DATAMONGO-973
	void findByWithPaginationWorksCorrectlyWhenUsingTextCriteria() {

		initRepoWithDefaultDocuments();

		Page<FullTextDocument> page = repo.findAllBy(TextCriteria.forDefaultLanguage().matching("film"),
				PageRequest.of(1, 1, Direction.ASC, "id"));

		assertThat(page.hasNext()).isTrue();
		assertThat(page.hasPrevious()).isTrue();
		assertThat(page.getTotalElements()).isEqualTo(3L);
		assertThat(page.getContent().get(0)).isEqualTo(DEMOLITION_MAN);
	}

	@Test // DATAMONGO-973
	void findAllByTextCriteriaWithSortWorksCorrectly() {

		initRepoWithDefaultDocuments();
		FullTextDocument snipes = new FullTextDocument("4", "Snipes", "Wesley Trent Snipes is an actor and film producer.");
		repo.save(snipes);

		List<FullTextDocument> result = repo.findAllBy(TextCriteria.forDefaultLanguage().matching("snipes"),
				Sort.by("score"));

		assertThat(result.size()).isEqualTo(4);
		assertThat(result.get(0)).isEqualTo(snipes);
	}

	@Test // DATAMONGO-973
	void findByWithSortByScoreViaPageRequestTriggersSortingCorrectly() {

		initRepoWithDefaultDocuments();
		FullTextDocument snipes = new FullTextDocument("4", "Snipes", "Wesley Trent Snipes is an actor and film producer.");
		repo.save(snipes);

		Page<FullTextDocument> page = repo.findAllBy(TextCriteria.forDefaultLanguage().matching("snipes"),
				PageRequest.of(0, 10, Direction.ASC, "score"));

		assertThat(page.getTotalElements()).isEqualTo(4L);
		assertThat(page.getContent().get(0)).isEqualTo(snipes);
	}

	@Test // DATAMONGO-973
	void findByWithSortViaPageRequestIgnoresTextScoreWhenSortedByOtherProperty() {

		initRepoWithDefaultDocuments();
		FullTextDocument snipes = new FullTextDocument("4", "Snipes", "Wesley Trent Snipes is an actor and film producer.");
		repo.save(snipes);

		Page<FullTextDocument> page = repo.findAllBy(TextCriteria.forDefaultLanguage().matching("snipes"),
				PageRequest.of(0, 10, Direction.ASC, "id"));

		assertThat(page.getTotalElements()).isEqualTo(4L);
		assertThat(page.getContent().get(0)).isEqualTo(PASSENGER_57);
	}

	@Test // DATAMONGO-973
	void derivedSortForTextScorePropertyWorksCorrectly() {

		initRepoWithDefaultDocuments();
		FullTextDocument snipes = new FullTextDocument("4", "Snipes", "Wesley Trent Snipes is an actor and film producer.");
		repo.save(snipes);

		List<FullTextDocument> result = repo
				.findByNonTextIndexPropertyIsNullOrderByScoreDesc(TextCriteria.forDefaultLanguage().matching("snipes"));
		assertThat(result.get(0)).isEqualTo(snipes);
	}

	@Test // DATAMONGO-973, DATAMONGO-2516
	void derivedFinderMethodWithoutFullTextShouldNoCauseTroubleWhenHavingEntityWithTextScoreProperty() {

		initRepoWithDefaultDocuments();
		List<FullTextDocument> result = repo.findByTitle(DROP_ZONE.getTitle());

		assertThat(result.get(0)).isEqualTo(DROP_ZONE);
		assertThat(result.get(0).score).isNull();
	}

	private void initRepoWithDefaultDocuments() {
		repo.saveAll(Arrays.asList(PASSENGER_57, DEMOLITION_MAN, DROP_ZONE));
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
		public boolean equals(@Nullable Object obj) {
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
