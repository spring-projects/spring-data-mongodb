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
package org.springframework.data.mongodb.core.index;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.bson.Document;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * {@link IndexDefinition} to span multiple keys for text search.
 * 
 * @author Christoph Strobl
 * @since 1.6
 */
public class TextIndexDefinition implements IndexDefinition {

	private @Nullable String name;
	private Set<TextIndexedFieldSpec> fieldSpecs;
	private @Nullable String defaultLanguage;
	private @Nullable String languageOverride;
	private @Nullable IndexFilter filter;

	TextIndexDefinition() {
		fieldSpecs = new LinkedHashSet<TextIndexedFieldSpec>();
	}

	/**
	 * Creates a {@link TextIndexDefinition} for all fields in the document.
	 * 
	 * @return
	 */
	public static TextIndexDefinition forAllFields() {
		return new TextIndexDefinitionBuilder().onAllFields().build();
	}

	/**
	 * Get {@link TextIndexDefinitionBuilder} to create {@link TextIndexDefinition}.
	 * 
	 * @return
	 */
	public static TextIndexDefinitionBuilder builder() {
		return new TextIndexDefinitionBuilder();
	}

	/**
	 * @param fieldSpec
	 */
	public void addFieldSpec(TextIndexedFieldSpec fieldSpec) {
		this.fieldSpecs.add(fieldSpec);
	}

	/**
	 * @param fieldSpecs
	 */
	public void addFieldSpecs(Collection<TextIndexedFieldSpec> fieldSpecs) {
		this.fieldSpecs.addAll(fieldSpecs);
	}

	/**
	 * Returns if the {@link TextIndexDefinition} has fields assigned.
	 * 
	 * @return
	 */
	public boolean hasFieldSpec() {
		return !fieldSpecs.isEmpty();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.index.IndexDefinition#getIndexKeys()
	 */
	@Override
	public Document getIndexKeys() {

		Document keys = new Document();
		for (TextIndexedFieldSpec fieldSpec : fieldSpecs) {
			keys.put(fieldSpec.fieldname, "text");
		}

		return keys;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.index.IndexDefinition#getIndexOptions()
	 */
	@Override
	public Document getIndexOptions() {

		Document options = new Document();
		if (StringUtils.hasText(name)) {
			options.put("name", name);
		}
		if (StringUtils.hasText(defaultLanguage)) {
			options.put("default_language", defaultLanguage);
		}

		Document weightsDocument = new Document();
		for (TextIndexedFieldSpec fieldSpec : fieldSpecs) {
			if (fieldSpec.isWeighted()) {
				weightsDocument.put(fieldSpec.getFieldname(), fieldSpec.getWeight());
			}
		}

		if (!weightsDocument.isEmpty()) {
			options.put("weights", weightsDocument);
		}
		if (StringUtils.hasText(languageOverride)) {
			options.put("language_override", languageOverride);
		}

		if (filter != null) {
			options.put("partialFilterExpression", filter.getFilterObject());
		}

		return options;
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.6
	 */
	public static class TextIndexedFieldSpec {

		private final String fieldname;
		private final @Nullable Float weight;

		/**
		 * Create new {@link TextIndexedFieldSpec} for given fieldname without any weight.
		 * 
		 * @param fieldname
		 */
		public TextIndexedFieldSpec(String fieldname) {
			this(fieldname, 1.0F);
		}

		/**
		 * Create new {@link TextIndexedFieldSpec} for given fieldname and weight.
		 * 
		 * @param fieldname
		 * @param weight
		 */
		public TextIndexedFieldSpec(String fieldname, @Nullable Float weight) {

			Assert.hasText(fieldname, "Text index field cannot be blank.");
			this.fieldname = fieldname;
			this.weight = weight != null ? weight : 1.0F;
		}

		/**
		 * Get the fieldname associated with the {@link TextIndexedFieldSpec}.
		 * 
		 * @return
		 */
		public String getFieldname() {
			return fieldname;
		}

		/**
		 * Get the weight associated with the {@link TextIndexedFieldSpec}.
		 * 
		 * @return
		 */
		@Nullable
		public Float getWeight() {
			return weight;
		}

		/**
		 * @return true if {@link #weight} has a value that is a valid number.
		 */
		public boolean isWeighted() {
			return this.weight != null && this.weight.compareTo(1.0F) != 0;
		}

		@Override
		public int hashCode() {
			return ObjectUtils.nullSafeHashCode(fieldname);
		}

		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof TextIndexedFieldSpec)) {
				return false;
			}

			TextIndexedFieldSpec other = (TextIndexedFieldSpec) obj;

			return ObjectUtils.nullSafeEquals(this.fieldname, other.fieldname);
		}

	}

	/**
	 * {@link TextIndexDefinitionBuilder} helps defining options for creating {@link TextIndexDefinition}.
	 * 
	 * @author Christoph Strobl
	 * @since 1.6
	 */
	public static class TextIndexDefinitionBuilder {

		private TextIndexDefinition instance;
		private static final TextIndexedFieldSpec ALL_FIELDS = new TextIndexedFieldSpec("$**");

		public TextIndexDefinitionBuilder() {
			this.instance = new TextIndexDefinition();
		}

		/**
		 * Define the name to be used when creating the index in the store.
		 * 
		 * @param name
		 * @return
		 */
		public TextIndexDefinitionBuilder named(String name) {
			this.instance.name = name;
			return this;
		}

		/**
		 * Define the index to span all fields using wilcard. <br/>
		 * <strong>NOTE</strong> {@link TextIndexDefinition} cannot contain any other fields when defined with wildcard.
		 * 
		 * @return
		 */
		public TextIndexDefinitionBuilder onAllFields() {

			if (!instance.fieldSpecs.isEmpty()) {
				throw new InvalidDataAccessApiUsageException("Cannot add wildcard fieldspect to non empty.");
			}

			this.instance.fieldSpecs.add(ALL_FIELDS);
			return this;
		}

		/**
		 * Include given fields with default weight.
		 * 
		 * @param fieldnames
		 * @return
		 */
		public TextIndexDefinitionBuilder onFields(String... fieldnames) {

			for (String fieldname : fieldnames) {
				onField(fieldname);
			}
			return this;
		}

		/**
		 * Include given field with default weight.
		 * 
		 * @param fieldname
		 * @return
		 */
		public TextIndexDefinitionBuilder onField(String fieldname) {
			return onField(fieldname, 1F);
		}

		/**
		 * Include given field with weight.
		 * 
		 * @param fieldname
		 * @return
		 */
		public TextIndexDefinitionBuilder onField(String fieldname, Float weight) {

			if (this.instance.fieldSpecs.contains(ALL_FIELDS)) {
				throw new InvalidDataAccessApiUsageException(
						String.format("Cannot add %s to field spec for all fields.", fieldname));
			}

			this.instance.fieldSpecs.add(new TextIndexedFieldSpec(fieldname, weight));
			return this;
		}

		/**
		 * Define the default language to be used when indexing documents.
		 * 
		 * @param language
		 * @return
		 * @see <a href=
		 *      "https://docs.mongodb.org/manual/tutorial/specify-language-for-text-index/#specify-default-language-text-index">https://docs.mongodb.org/manual/tutorial/specify-language-for-text-index/#specify-default-language-text-index</a>
		 */
		public TextIndexDefinitionBuilder withDefaultLanguage(String language) {

			this.instance.defaultLanguage = language;
			return this;
		}

		/**
		 * Define field for language override.
		 * 
		 * @param fieldname
		 * @return
		 */
		public TextIndexDefinitionBuilder withLanguageOverride(String fieldname) {

			if (StringUtils.hasText(this.instance.languageOverride)) {
				throw new InvalidDataAccessApiUsageException(
						String.format("Cannot set language override on %s as it is already defined on %s.", fieldname,
								this.instance.languageOverride));
			}

			this.instance.languageOverride = fieldname;
			return this;
		}

		/**
		 * Only index the documents that meet the specified {@link IndexFilter filter expression}.
		 *
		 * @param filter can be {@literal null}.
		 * @return
		 * @see <a href=
		 *      "https://docs.mongodb.com/manual/core/index-partial/">https://docs.mongodb.com/manual/core/index-partial/</a>
		 * @since 1.10
		 */
		public TextIndexDefinitionBuilder partial(IndexFilter filter) {

			this.instance.filter = filter;
			return this;
		}

		public TextIndexDefinition build() {
			return this.instance;
		}

	}

}
