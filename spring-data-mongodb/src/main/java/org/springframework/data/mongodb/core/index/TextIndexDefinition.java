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
package org.springframework.data.mongodb.core.index;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * {@link IndexDefinition} to span multiple keys for text search.
 * 
 * @author Christoph Strobl
 * @since 1.6
 */
public class TextIndexDefinition implements IndexDefinition {

	private String name;
	private Set<TextIndexedFieldSpec> fieldSpecs;
	private String defaultLanguage;
	private String languageOverride;

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
	public DBObject getIndexKeys() {

		DBObject keys = new BasicDBObject();
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
	public DBObject getIndexOptions() {

		DBObject options = new BasicDBObject();
		if (StringUtils.hasText(name)) {
			options.put("name", name);
		}
		if (StringUtils.hasText(defaultLanguage)) {
			options.put("default_language", defaultLanguage);
		}

		BasicDBObject weightsDbo = new BasicDBObject();
		for (TextIndexedFieldSpec fieldSpec : fieldSpecs) {
			if (fieldSpec.isWeighted()) {
				weightsDbo.put(fieldSpec.getFieldname(), fieldSpec.getWeight());
			}
		}

		if (!weightsDbo.isEmpty()) {
			options.put("weights", weightsDbo);
		}
		if (StringUtils.hasText(languageOverride)) {
			options.put("language_override", languageOverride);
		}

		return options;
	}

	/**
	 * @author Christoph Strobl
	 * @since 1.6
	 */
	public static class TextIndexedFieldSpec {

		private final String fieldname;
		private final Float weight;

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
		public TextIndexedFieldSpec(String fieldname, Float weight) {

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
				throw new InvalidDataAccessApiUsageException(String.format("Cannot add %s to field spec for all fields.",
						fieldname));
			}

			this.instance.fieldSpecs.add(new TextIndexedFieldSpec(fieldname, weight));
			return this;
		}

		/**
		 * Define the default language to be used when indexing documents.
		 * 
		 * @param language
		 * @see http://docs.mongodb.org/manual/tutorial/specify-language-for-text-index/#specify-default-language-text-index
		 * @return
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
				throw new InvalidDataAccessApiUsageException(String.format(
						"Cannot set language override on %s as it is already defined on %s.", fieldname,
						this.instance.languageOverride));
			}

			this.instance.languageOverride = fieldname;
			return this;
		}

		public TextIndexDefinition build() {
			return this.instance;
		}

	}

}
