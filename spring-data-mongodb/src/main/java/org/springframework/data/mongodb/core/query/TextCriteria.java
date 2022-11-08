/*
 * Copyright 2014-2022 the original author or authors.
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

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Implementation of {@link CriteriaDefinition} to be used for full text search.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @author Mark Paluch
 * @since 1.6
 */
public class TextCriteria implements CriteriaDefinition {

	private final List<Term> terms;
	private final @Nullable String language;
	private @Nullable Boolean caseSensitive;
	private @Nullable Boolean diacriticSensitive;

	/**
	 * Creates a new {@link TextCriteria}.
	 *
	 * @see #forDefaultLanguage()
	 * @see #forLanguage(String)
	 */
	public TextCriteria() {
		this(null);
	}

	private TextCriteria(@Nullable String language) {

		this.language = language;
		this.terms = new ArrayList<>();
	}

	/**
	 * Returns a new {@link TextCriteria} for the default language.
	 *
	 * @return
	 */
	public static TextCriteria forDefaultLanguage() {
		return new TextCriteria();
	}

	/**
	 * For a full list of supported languages see the mongodb reference manual for
	 * <a href="https://docs.mongodb.org/manual/reference/text-search-languages/">Text Search Languages</a>.
	 *
	 * @param language
	 * @return
	 */
	public static TextCriteria forLanguage(String language) {

		Assert.hasText(language, "Language must not be null or empty");
		return new TextCriteria(language);
	}

	/**
	 * Configures the {@link TextCriteria} to match any of the given words.
	 *
	 * @param words the words to match.
	 * @return
	 */
	public TextCriteria matchingAny(String... words) {

		for (String word : words) {
			matching(word);
		}

		return this;
	}

	/**
	 * Adds given {@link Term} to criteria.
	 *
	 * @param term must not be {@literal null}.
	 */
	public TextCriteria matching(Term term) {

		Assert.notNull(term, "Term to add must not be null");

		this.terms.add(term);
		return this;
	}

	/**
	 * @param term
	 * @return
	 */
	public TextCriteria matching(String term) {

		if (StringUtils.hasText(term)) {
			matching(new Term(term));
		}
		return this;
	}

	/**
	 * @param term
	 * @return
	 */
	public TextCriteria notMatching(String term) {

		if (StringUtils.hasText(term)) {
			matching(new Term(term, Term.Type.WORD).negate());
		}
		return this;
	}

	/**
	 * @param words
	 * @return
	 */
	public TextCriteria notMatchingAny(String... words) {

		for (String word : words) {
			notMatching(word);
		}
		return this;
	}

	/**
	 * Given value will treated as a single phrase.
	 *
	 * @param phrase
	 * @return
	 */
	public TextCriteria notMatchingPhrase(String phrase) {

		if (StringUtils.hasText(phrase)) {
			matching(new Term(phrase, Term.Type.PHRASE).negate());
		}
		return this;
	}

	/**
	 * Given value will treated as a single phrase.
	 *
	 * @param phrase
	 * @return
	 */
	public TextCriteria matchingPhrase(String phrase) {

		if (StringUtils.hasText(phrase)) {
			matching(new Term(phrase, Term.Type.PHRASE));
		}
		return this;
	}

	/**
	 * Optionally enable or disable case sensitive search.
	 *
	 * @param caseSensitive boolean flag to enable/disable.
	 * @return never {@literal null}.
	 * @since 1.10
	 */
	public TextCriteria caseSensitive(boolean caseSensitive) {

		this.caseSensitive = caseSensitive;
		return this;
	}

	/**
	 * Optionally enable or disable diacritic sensitive search against version 3 text indexes.
	 *
	 * @param diacriticSensitive boolean flag to enable/disable.
	 * @return never {@literal null}.
	 * @since 1.10
	 */
	public TextCriteria diacriticSensitive(boolean diacriticSensitive) {

		this.diacriticSensitive = diacriticSensitive;
		return this;
	}

	@Override
	public String getKey() {
		return "$text";
	}

	@Override
	public Document getCriteriaObject() {

		Document document = new Document();

		if (StringUtils.hasText(language)) {
			document.put("$language", language);
		}

		if (!terms.isEmpty()) {
			document.put("$search", join(terms));
		}

		if (caseSensitive != null) {
			document.put("$caseSensitive", caseSensitive);
		}

		if (diacriticSensitive != null) {
			document.put("$diacriticSensitive", diacriticSensitive);
		}

		return new Document("$text", document);
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o) {
			return true;
		}
		if (!(o instanceof TextCriteria)) {
			return false;
		}

		TextCriteria that = (TextCriteria) o;

		return ObjectUtils.nullSafeEquals(terms, that.terms) && ObjectUtils.nullSafeEquals(language, that.language)
				&& ObjectUtils.nullSafeEquals(caseSensitive, that.caseSensitive)
				&& ObjectUtils.nullSafeEquals(diacriticSensitive, that.diacriticSensitive);
	}

	@Override
	public int hashCode() {

		int result = 17;

		result += ObjectUtils.nullSafeHashCode(terms);
		result += ObjectUtils.nullSafeHashCode(language);
		result += ObjectUtils.nullSafeHashCode(caseSensitive);
		result += ObjectUtils.nullSafeHashCode(diacriticSensitive);

		return result;
	}

	private String join(Iterable<Term> terms) {

		List<String> result = new ArrayList<>();

		for (Term term : terms) {
			if (term != null) {
				result.add(term.getFormatted());
			}
		}

		return StringUtils.collectionToDelimitedString(result, " ");
	}
}
