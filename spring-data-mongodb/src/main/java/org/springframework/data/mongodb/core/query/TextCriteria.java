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
package org.springframework.data.mongodb.core.query;

import java.util.ArrayList;
import java.util.List;

import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * Implementation of {@link CriteriaDefinition} to be used for full text search.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.6
 */
public class TextCriteria implements CriteriaDefinition {

	private final List<Term> terms;
	private String language;

	/**
	 * Creates a new {@link TextCriteria}.
	 *
	 * @see #forDefaultLanguage()
	 * @see #forLanguage(String)
	 */
	public TextCriteria() {
		this(null);
	}

	private TextCriteria(String language) {

		this.language = language;
		this.terms = new ArrayList<Term>();
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
	 * For a full list of supported languages see the mongodb reference manual for <a
	 * href="http://docs.mongodb.org/manual/reference/text-search-languages/">Text Search Languages</a>.
	 *
	 * @param language
	 * @return
	 */
	public static TextCriteria forLanguage(String language) {

		Assert.hasText(language, "Language must not be null or empty!");
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

		Assert.notNull(term, "Term to add must not be null.");

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

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.query.CriteriaDefinition#getKey()
	 */
	@Override
	public String getKey() {
		return "$text";
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.query.CriteriaDefinition#getCriteriaObject()
	 */
	@Override
	public DBObject getCriteriaObject() {

		BasicDBObjectBuilder builder = new BasicDBObjectBuilder();

		if (StringUtils.hasText(language)) {
			builder.add("$language", language);
		}

		if (!terms.isEmpty()) {
			builder.add("$search", join(terms));
		}

		return new BasicDBObject("$text", builder.get());
	}

	private String join(Iterable<Term> terms) {

		List<String> result = new ArrayList<String>();

		for (Term term : terms) {
			if (term != null) {
				result.add(term.getFormatted());
			}
		}

		return StringUtils.collectionToDelimitedString(result, " ");
	}
}
