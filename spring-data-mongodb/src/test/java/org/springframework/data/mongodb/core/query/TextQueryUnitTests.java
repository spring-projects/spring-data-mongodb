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
package org.springframework.data.mongodb.core.query;

import static org.springframework.data.mongodb.test.util.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;

/**
 * Unit tests for {@link TextQuery}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class TextQueryUnitTests {

	private static final String QUERY = "bake coffee cake";
	private static final String LANGUAGE_SPANISH = "spanish";

	@Test // DATAMONGO-850
	public void shouldCreateQueryObjectCorrectly() {
		assertThat(new TextQuery(QUERY).getQueryObject()).containsEntry("$text.$search", QUERY);
	}

	@Test // DATAMONGO-850
	public void shouldIncludeLanguageInQueryObjectWhenNotNull() {
		assertThat(new TextQuery(QUERY, LANGUAGE_SPANISH).getQueryObject()).containsEntry("$text.$search", QUERY)
				.containsEntry("$text.$language", LANGUAGE_SPANISH);
	}

	@Test // DATAMONGO-850
	public void shouldIncludeScoreFieldCorrectly() {

		TextQuery textQuery = new TextQuery(QUERY).includeScore();
		assertThat(textQuery.getQueryObject()).containsEntry("$text.$search", QUERY);
		assertThat(textQuery.getFieldsObject()).containsKey("score");
	}

	@Test // DATAMONGO-850
	public void shouldNotOverrideExistingProjections() {

		TextQuery query = new TextQuery(TextCriteria.forDefaultLanguage().matching(QUERY)).includeScore();
		query.fields().include("foo");

		assertThat(query.getQueryObject()).containsEntry("$text.$search", QUERY);
		assertThat(query.getFieldsObject()).containsKeys("score", "foo");
	}

	@Test // DATAMONGO-850
	public void shouldIncludeSortingByScoreCorrectly() {

		TextQuery textQuery = new TextQuery(QUERY).sortByScore();

		assertThat(textQuery.getQueryObject()).containsEntry("$text.$search", QUERY);
		assertThat(textQuery.getFieldsObject()).containsKey("score");
		assertThat(textQuery.getSortObject()).containsKey("score");
	}

	@Test // DATAMONGO-850
	public void shouldNotOverrideExistingSort() {

		TextQuery query = new TextQuery(QUERY);
		query.with(Sort.by(Direction.DESC, "foo"));
		query.sortByScore();

		assertThat(query.getQueryObject()).containsEntry("$text.$search", QUERY);
		assertThat(query.getFieldsObject()).containsKeys("score");
		assertThat(query.getSortObject()).containsEntry("foo", -1).containsKey("score");
	}

	@Test // DATAMONGO-850
	public void shouldUseCustomFieldnameForScoring() {

		TextQuery query = new TextQuery(QUERY).includeScore("customFieldForScore").sortByScore();

		assertThat(query.getQueryObject()).containsEntry("$text.$search", QUERY);
		assertThat(query.getFieldsObject()).containsKeys("customFieldForScore");
		assertThat(query.getSortObject()).containsKey("customFieldForScore");
	}

	@Test // GH-3896
	public void retainsSortOrderWhenUsingScore() {

		TextQuery query = new TextQuery(QUERY);
		query.with(Sort.by(Direction.DESC, "one"));
		query.sortByScore();
		query.with(Sort.by(Direction.DESC, "two"));

		assertThat(query.getSortObject().keySet().stream()).containsExactly("one", "score", "two");

		query = new TextQuery(QUERY);
		query.with(Sort.by(Direction.DESC, "one"));
		query.sortByScore();

		assertThat(query.getSortObject().keySet().stream()).containsExactly("one", "score");

		query = new TextQuery(QUERY);
		query.sortByScore();
		query.with(Sort.by(Direction.DESC, "one"));
		query.with(Sort.by(Direction.DESC, "two"));

		assertThat(query.getSortObject().keySet().stream()).containsExactly("score", "one", "two");
	}

}
