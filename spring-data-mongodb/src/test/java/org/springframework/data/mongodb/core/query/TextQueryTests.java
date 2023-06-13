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

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.core.query.Criteria.*;

import java.util.List;

import org.bson.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.mongodb.core.index.IndexDefinition;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.Language;
import org.springframework.data.mongodb.core.mapping.TextScore;
import org.springframework.data.mongodb.core.query.TextQueryTests.FullTextDoc.FullTextDocBuilder;
import org.springframework.data.mongodb.test.util.MongoTemplateExtension;
import org.springframework.data.mongodb.test.util.MongoTestTemplate;
import org.springframework.data.mongodb.test.util.Template;
import org.springframework.lang.Nullable;

/**
 * @author Christoph Strobl
 * @author Mark Paluch
 */
@ExtendWith(MongoTemplateExtension.class)
public class TextQueryTests {

	private static final FullTextDoc BAKE = new FullTextDocBuilder().headline("bake").build();
	private static final FullTextDoc COFFEE = new FullTextDocBuilder().subHeadline("coffee").build();
	private static final FullTextDoc CAKE = new FullTextDocBuilder().body("cake").build();
	private static final FullTextDoc NOT_TO_BE_FOUND = new FullTextDocBuilder().headline("o_O").build();
	private static final FullTextDoc SPANISH_MILK = new FullTextDocBuilder().headline("leche").lanugage("spanish")
			.build();
	private static final FullTextDoc FRENCH_MILK = new FullTextDocBuilder().headline("leche").lanugage("french").build();
	private static final FullTextDoc MILK_AND_SUGAR = new FullTextDocBuilder().headline("milk and sugar").build();

	@Template(initialEntitySet = FullTextDoc.class) //
	static MongoTestTemplate template;

	@BeforeEach
	public void setUp() {

		template.flush();

		IndexOperations indexOps = template.indexOps(FullTextDoc.class);
		indexOps.dropAllIndexes();

		indexOps.ensureIndex(new IndexDefinition() {

			@Override
			public Document getIndexOptions() {
				Document options = new Document();
				options.put("weights", weights());
				options.put("name", "TextQueryTests_TextIndex");
				options.put("language_override", "lang");
				options.put("default_language", "english");
				return options;
			}

			@Override
			public Document getIndexKeys() {
				Document keys = new Document();
				keys.put("headline", "text");
				keys.put("subheadline", "text");
				keys.put("body", "text");
				return keys;
			}

			private Document weights() {
				Document weights = new Document();
				weights.put("headline", 10);
				weights.put("subheadline", 5);
				weights.put("body", 1);
				return weights;
			}
		});
	}

	@Test // DATAMONGO-850
	public void shouldOnlyFindDocumentsMatchingAnyWordOfGivenQuery() {

		initWithDefaultDocuments();

		List<FullTextDoc> result = template.find(new TextQuery("bake coffee cake"), FullTextDoc.class);
		assertThat(result).hasSize(3);
		assertThat(result).contains(BAKE, COFFEE, CAKE);
	}

	@Test // DATAMONGO-850
	public void shouldNotFindDocumentsWhenQueryDoesNotMatchAnyDocumentInIndex() {

		initWithDefaultDocuments();

		List<FullTextDoc> result = template.find(new TextQuery("tasmanian devil"), FullTextDoc.class);
		assertThat(result).hasSize(0);
	}

	@Test // DATAMONGO-850
	public void shouldApplySortByScoreCorrectly() {

		initWithDefaultDocuments();
		FullTextDoc coffee2 = new FullTextDocBuilder().headline("coffee").build();
		template.insert(coffee2);

		List<FullTextDoc> result = template.find(new TextQuery("bake coffee cake").sortByScore(), FullTextDoc.class);
		assertThat(result).hasSize(4);
		assertThat(result.get(0)).isIn(BAKE, coffee2);
		assertThat(result.get(1)).isIn(BAKE, coffee2);
		assertThat(result.get(2)).isEqualTo(COFFEE);
		assertThat(result.get(3)).isEqualTo(CAKE);
	}

	@Test // DATAMONGO-850
	public void shouldFindTextInAnyLanguage() {

		initWithDefaultDocuments();
		List<FullTextDoc> result = template.find(new TextQuery("leche"), FullTextDoc.class);
		assertThat(result).hasSize(2);
		assertThat(result).contains(SPANISH_MILK, FRENCH_MILK);
	}

	@Test // DATAMONGO-850
	public void shouldOnlyFindTextInSpecificLanguage() {

		initWithDefaultDocuments();
		List<FullTextDoc> result = template.find(new TextQuery("leche").addCriteria(where("language").is("spanish")),
				FullTextDoc.class);
		assertThat(result).hasSize(1);
		assertThat(result.get(0)).isEqualTo(SPANISH_MILK);
	}

	@Test // DATAMONGO-850
	public void shouldNotFindDocumentsWithNegatedTerms() {

		initWithDefaultDocuments();

		List<FullTextDoc> result = template.find(new TextQuery("bake coffee -cake"), FullTextDoc.class);
		assertThat(result).hasSize(2);
		assertThat(result).contains(BAKE, COFFEE);
	}

	@Test // DATAMONGO-976
	public void shouldInlcudeScoreCorreclty() {

		initWithDefaultDocuments();

		List<FullTextDoc> result = template.find(new TextQuery("bake coffee -cake").includeScore().sortByScore(),
				FullTextDoc.class);

		assertThat(result).hasSize(2);
		for (FullTextDoc scoredDoc : result) {
			assertThat(scoredDoc.score > 0F).isTrue();
		}
	}

	@Test // DATAMONGO-850
	public void shouldApplyPhraseCorrectly() {

		initWithDefaultDocuments();

		TextQuery query = TextQuery.queryText(TextCriteria.forDefaultLanguage().matchingPhrase("milk and sugar"));
		List<FullTextDoc> result = template.find(query, FullTextDoc.class);

		assertThat(result).hasSize(1);
		assertThat(result).containsExactly(MILK_AND_SUGAR);
	}

	@Test // DATAMONGO-850
	public void shouldReturnEmptyListWhenNoDocumentsMatchGivenPhrase() {

		initWithDefaultDocuments();

		TextQuery query = TextQuery.queryText(TextCriteria.forDefaultLanguage().matchingPhrase("milk no sugar"));
		List<FullTextDoc> result = template.find(query, FullTextDoc.class);

		assertThat(result).isEmpty();
	}

	@Test // DATAMONGO-850
	public void shouldApplyPaginationCorrectly() {

		initWithDefaultDocuments();

		// page 1
		List<FullTextDoc> result = template.find(new TextQuery("bake coffee cake").sortByScore().with(PageRequest.of(0, 2)),
				FullTextDoc.class);
		assertThat(result).hasSize(2);
		assertThat(result).containsExactly(BAKE, COFFEE);

		// page 2
		result = template.find(new TextQuery("bake coffee cake").sortByScore().with(PageRequest.of(1, 2)),
				FullTextDoc.class);
		assertThat(result).hasSize(1);
		assertThat(result).containsExactly(CAKE);
	}

	private void initWithDefaultDocuments() {
		this.template.save(BAKE);
		this.template.save(COFFEE);
		this.template.save(CAKE);
		this.template.save(NOT_TO_BE_FOUND);
		this.template.save(SPANISH_MILK);
		this.template.save(FRENCH_MILK);
		this.template.save(MILK_AND_SUGAR);
	}

	@org.springframework.data.mongodb.core.mapping.Document(collection = "fullTextDoc")
	static class FullTextDoc {

		@Id String id;

		private @Language @Field("lang") String language;

		private String headline;
		private String subheadline;
		private String body;

		private @TextScore Float score;

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((body == null) ? 0 : body.hashCode());
			result = prime * result + ((headline == null) ? 0 : headline.hashCode());
			result = prime * result + ((id == null) ? 0 : id.hashCode());
			result = prime * result + ((language == null) ? 0 : language.hashCode());
			result = prime * result + ((subheadline == null) ? 0 : subheadline.hashCode());
			return result;
		}

		@Override
		public boolean equals(@Nullable Object obj) {
			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof FullTextDoc)) {
				return false;
			}
			FullTextDoc other = (FullTextDoc) obj;
			if (body == null) {
				if (other.body != null) {
					return false;
				}
			} else if (!body.equals(other.body)) {
				return false;
			}
			if (headline == null) {
				if (other.headline != null) {
					return false;
				}
			} else if (!headline.equals(other.headline)) {
				return false;
			}
			if (id == null) {
				if (other.id != null) {
					return false;
				}
			} else if (!id.equals(other.id)) {
				return false;
			}
			if (language == null) {
				if (other.language != null) {
					return false;
				}
			} else if (!language.equals(other.language)) {
				return false;
			}
			if (subheadline == null) {
				if (other.subheadline != null) {
					return false;
				}
			} else if (!subheadline.equals(other.subheadline)) {
				return false;
			}
			return true;
		}

		public String toString() {
			return "TextQueryTests.FullTextDoc(id=" + this.id + ", language=" + this.language + ", headline=" + this.headline + ", subheadline=" + this.subheadline + ", body=" + this.body + ", score=" + this.score + ")";
		}

		static class FullTextDocBuilder {

			private FullTextDoc instance;

			public FullTextDocBuilder() {
				this.instance = new FullTextDoc();
			}

			public FullTextDocBuilder headline(String headline) {
				this.instance.headline = headline;
				return this;
			}

			public FullTextDocBuilder subHeadline(String subHeadline) {
				this.instance.subheadline = subHeadline;
				return this;
			}

			public FullTextDocBuilder body(String body) {
				this.instance.body = body;
				return this;
			}

			public FullTextDocBuilder lanugage(String language) {
				this.instance.language = language;
				return this;
			}

			public FullTextDoc build() {
				return this.instance;
			}
		}

	}
}
