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

import java.util.ArrayList;
import java.util.List;

import org.springframework.core.convert.converter.Converter;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.text.Term;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * Implementation of {@link CriteriaDefinition} to be used for full text search.
 * 
 * @author Christoph Strobl
 * @since 1.6
 */
public class TextCriteria implements CriteriaDefinition {

	private String language;
	private List<Term> terms;

	public TextCriteria() {
		this.terms = new ArrayList<Term>();
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

		if (!CollectionUtils.isEmpty(terms)) {
			builder.add("$search", join(terms));
		}

		return new BasicDBObject("$text", builder.get());
	}

	/**
	 * @param words
	 * @return
	 */
	public TextCriteria matchingAny(String... words) {

		for (String word : words) {
			matching(word);
		}
		return this;
	}

	/**
	 * Add given {@link Term} to criteria.
	 * 
	 * @param term must not be null.
	 */
	public TextCriteria matching(Term term) {

		Assert.notNull(term, "Term to add must not be null.");
		this.terms.add(term);
		return this;
	}

	private void notMatching(Term term) {
		matching(term.negate());
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
			notMatching(new Term(term, Term.Type.WORD));
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
			notMatching(new Term(phrase, Term.Type.PHRASE));
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
	 * @return
	 */
	public static TextCriteria forDefaultLanguage() {
		return new TextCriteriaBuilder().build();
	}

	/**
	 * For a full list of supported languages see the mongdodb reference manual for <a
	 * href="http://docs.mongodb.org/manual/reference/text-search-languages/">Text Search Languages</a>.
	 * 
	 * @param language
	 * @return
	 */
	public static TextCriteria forLanguage(String language) {
		return new TextCriteriaBuilder().withLanguage(language).build();
	}

	private String join(Iterable<Term> iterable) {

		return collectionToDelimitedString(iterable, new Converter<Term, String>() {

			@Override
			public String convert(Term source) {

				if (source == null) {
					return null;
				}
				return source.getFormatted();
			}
		});
	}

	private <T> String collectionToDelimitedString(Iterable<T> iterable, Converter<T, String> converter) {

		List<String> values = new ArrayList<String>();
		for (T item : iterable) {
			values.add(converter.convert(item));
		}
		return StringUtils.collectionToDelimitedString(values, " ");
	}

	public static class TextCriteriaBuilder {

		private TextCriteria instance;

		public TextCriteriaBuilder() {
			this.instance = new TextCriteria();
		}

		public TextCriteriaBuilder withLanguage(String language) {
			this.instance.language = language;
			return this;
		}

		public TextCriteria build() {
			return this.instance;
		}

	}

	@Override
	public String getKey() {
		return "$text";
	}

}
