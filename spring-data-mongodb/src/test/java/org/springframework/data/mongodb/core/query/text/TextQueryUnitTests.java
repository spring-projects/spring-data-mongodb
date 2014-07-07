/*
 * Copyright 2014 the original author or authors.
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
package org.springframework.data.mongodb.core.query.text;

import static org.junit.Assert.*;
import static org.springframework.data.mongodb.core.query.text.IsTextQuery.*;

import org.junit.Test;
import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.query.Query;

/**
 * @author Christoph Strobl
 */
public class TextQueryUnitTests {

	private static final String QUERY = "bake coffee cake";
	private static final String LANGUAGE_SPANISH = "spanish";

	/**
	 * @see DATAMONGO-850
	 */
	@Test
	public void shouldCreateQueryObjectCorrectly() {
		assertThat(new TextQuery(QUERY), isTextQuery().searchingFor(QUERY));
	}

	/**
	 * @see DATAMONGO-850
	 */
	@Test
	public void shouldIncludeLanguageInQueryObjectWhenNotNull() {
		assertThat(new TextQuery(QUERY, LANGUAGE_SPANISH), isTextQuery().searchingFor(QUERY).inLanguage(LANGUAGE_SPANISH));
	}

	/**
	 * @see DATAMONGO-850
	 */
	@Test
	public void shouldIncludeScoreFieldCorrectly() {
		assertThat(new TextQuery(QUERY).includeScore(), isTextQuery().searchingFor(QUERY).returningScore());
	}

	/**
	 * @see DATAMONGO-850
	 */
	@Test
	public void shouldNotOverrideExistingProjections() {

		TextQuery query = new TextQuery(TextCriteria.forDefaultLanguage().matching(QUERY)).includeScore();
		query.fields().include("foo");

		assertThat(query, isTextQuery().searchingFor(QUERY).returningScore().includingField("foo"));
	}

	/**
	 * @see DATAMONGO-850
	 */
	@Test
	public void shouldIncludeSortingByScoreCorrectly() {
		assertThat(new TextQuery(QUERY).sortByScore(), isTextQuery().searchingFor(QUERY).returningScore().sortingByScore());
	}

	/**
	 * @see DATAMONGO-850
	 */
	@Test
	public void shouldNotOverrideExistingSort() {

		TextQuery query = new TextQuery(QUERY);
		query.with(new Sort(Direction.DESC, "foo"));
		query.sortByScore();

		assertThat(query,
				isTextQuery().searchingFor(QUERY).returningScore().sortingByScore().sortingBy("foo", Direction.DESC));
	}

	/**
	 * @see DATAMONGO-850
	 */
	@Test
	public void shouldUseCustomFieldnameForScoring() {
		TextQuery query = new TextQuery(QUERY).includeScore("customFieldForScore").sortByScore();

		assertThat(query, isTextQuery().searchingFor(QUERY).returningScoreAs("customFieldForScore").sortingByScore());
	}

}
