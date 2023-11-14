/*
 * Copyright 2002-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.index;

import static org.springframework.data.domain.Sort.Direction.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bson.Document;
import org.springframework.data.mongodb.util.BsonUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;
import org.springframework.util.ObjectUtils;

/**
 * Index information for a MongoDB index.
 *
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class IndexInfo {

	private static final Double ONE = 1.0;
	private static final Double MINUS_ONE = (double) -1;
	private static final Collection<String> TWO_D_IDENTIFIERS = Arrays.asList("2d", "2dsphere");

	private final List<IndexField> indexFields;

	private final String name;
	private final boolean unique;
	private final boolean sparse;
	private final String language;
	private final boolean hidden;
	private @Nullable Duration expireAfter;
	private @Nullable String partialFilterExpression;
	private @Nullable Document collation;
	private @Nullable Document wildcardProjection;

	public IndexInfo(List<IndexField> indexFields, String name, boolean unique, boolean sparse, String language) {

		this.indexFields = Collections.unmodifiableList(indexFields);
		this.name = name;
		this.unique = unique;
		this.sparse = sparse;
		this.language = language;
		this.hidden = false;
	}

	public IndexInfo(List<IndexField> indexFields, String name, boolean unique, boolean sparse, String language,
			boolean hidden) {

		this.indexFields = Collections.unmodifiableList(indexFields);
		this.name = name;
		this.unique = unique;
		this.sparse = sparse;
		this.language = language;
		this.hidden = hidden;
	}

	/**
	 * Creates new {@link IndexInfo} parsing required properties from the given {@literal sourceDocument}.
	 *
	 * @param sourceDocument never {@literal null}.
	 * @return new instance of {@link IndexInfo}.
	 * @since 1.10
	 */
	public static IndexInfo indexInfoOf(Document sourceDocument) {

		Document keyDbObject = (Document) sourceDocument.get("key");
		int numberOfElements = keyDbObject.keySet().size();

		List<IndexField> indexFields = new ArrayList<IndexField>(numberOfElements);

		for (String key : keyDbObject.keySet()) {

			Object value = keyDbObject.get(key);

			if (TWO_D_IDENTIFIERS.contains(value)) {

				indexFields.add(IndexField.geo(key));

			} else if ("text".equals(value)) {

				Document weights = (Document) sourceDocument.get("weights");

				for (String fieldName : weights.keySet()) {
					indexFields.add(IndexField.text(fieldName, Float.valueOf(weights.get(fieldName).toString())));
				}

			} else {

				if (ObjectUtils.nullSafeEquals("hashed", value)) {
					indexFields.add(IndexField.hashed(key));
				} else if (key.endsWith("$**")) {
					indexFields.add(IndexField.wildcard(key));
				} else {

					Double keyValue = Double.valueOf(value.toString());

					if (ONE.equals(keyValue)) {
						indexFields.add(IndexField.create(key, ASC));
					} else if (MINUS_ONE.equals(keyValue)) {
						indexFields.add(IndexField.create(key, DESC));
					}
				}
			}
		}

		String name = sourceDocument.get("name").toString();

		boolean unique = sourceDocument.get("unique", false);
		boolean sparse = sourceDocument.get("sparse", false);
		boolean hidden = sourceDocument.getBoolean("hidden", false);
		String language = sourceDocument.containsKey("default_language") ? sourceDocument.getString("default_language")
				: "";

		String partialFilter = extractPartialFilterString(sourceDocument);

		IndexInfo info = new IndexInfo(indexFields, name, unique, sparse, language, hidden);
		info.partialFilterExpression = partialFilter;
		info.collation = sourceDocument.get("collation", Document.class);

		if (sourceDocument.containsKey("expireAfterSeconds")) {

			Number expireAfterSeconds = sourceDocument.get("expireAfterSeconds", Number.class);
			info.expireAfter = Duration.ofSeconds(NumberUtils.convertNumberToTargetClass(expireAfterSeconds, Long.class));
		}

		if (sourceDocument.containsKey("wildcardProjection")) {
			info.wildcardProjection = sourceDocument.get("wildcardProjection", Document.class);
		}

		return info;
	}

	/**
	 * @param sourceDocument never {@literal null}.
	 * @return the {@link String} representation of the partial filter {@link Document}.
	 * @since 2.1.11
	 */
	@Nullable
	private static String extractPartialFilterString(Document sourceDocument) {

		if (!sourceDocument.containsKey("partialFilterExpression")) {
			return null;
		}

		return BsonUtils.toJson(sourceDocument.get("partialFilterExpression", Document.class));
	}

	/**
	 * Returns the individual index fields of the index.
	 *
	 * @return
	 */
	public List<IndexField> getIndexFields() {
		return this.indexFields;
	}

	/**
	 * Returns whether the index is covering exactly the fields given independently of the order.
	 *
	 * @param keys must not be {@literal null}.
	 * @return
	 */
	public boolean isIndexForFields(Collection<String> keys) {

		Assert.notNull(keys, "Collection of keys must not be null");

		return this.indexFields.stream().map(IndexField::getKey).collect(Collectors.toSet()).containsAll(keys);
	}

	public String getName() {
		return name;
	}

	public boolean isUnique() {
		return unique;
	}

	public boolean isSparse() {
		return sparse;
	}

	/**
	 * @return
	 * @since 1.6
	 */
	public String getLanguage() {
		return language;
	}

	/**
	 * @return
	 * @since 1.0
	 */
	@Nullable
	public String getPartialFilterExpression() {
		return partialFilterExpression;
	}

	/**
	 * Get collation information.
	 *
	 * @return
	 * @since 2.0
	 */
	public Optional<Document> getCollation() {
		return Optional.ofNullable(collation);
	}

	/**
	 * Get {@literal wildcardProjection} information.
	 *
	 * @return {@link Optional#empty() empty} if not set.
	 * @since 3.3
	 */
	public Optional<Document> getWildcardProjection() {
		return Optional.ofNullable(wildcardProjection);
	}

	/**
	 * Get the duration after which documents within the index expire.
	 *
	 * @return the expiration time if set, {@link Optional#empty()} otherwise.
	 * @since 2.2
	 */
	public Optional<Duration> getExpireAfter() {
		return Optional.ofNullable(expireAfter);
	}

	/**
	 * @return {@literal true} if a hashed index field is present.
	 * @since 2.2
	 */
	public boolean isHashed() {
		return getIndexFields().stream().anyMatch(IndexField::isHashed);
	}

	/**
	 * @return {@literal true} if a wildcard index field is present.
	 * @since 3.3
	 */
	public boolean isWildcard() {
		return getIndexFields().stream().anyMatch(IndexField::isWildcard);
	}

	public boolean isHidden() {
		return hidden;
	}

	@Override
	public String toString() {

		return "IndexInfo [indexFields=" + indexFields + ", name=" + name + ", unique=" + unique + ", sparse=" + sparse
				+ ", language=" + language + ", partialFilterExpression=" + partialFilterExpression + ", collation=" + collation
				+ ", expireAfterSeconds=" + ObjectUtils.nullSafeToString(expireAfter) + ", hidden=" + hidden + "]";
	}

	@Override
	public int hashCode() {

		int result = 17;
		result += 31 * ObjectUtils.nullSafeHashCode(indexFields);
		result += 31 * ObjectUtils.nullSafeHashCode(name);
		result += 31 * ObjectUtils.nullSafeHashCode(unique);
		result += 31 * ObjectUtils.nullSafeHashCode(sparse);
		result += 31 * ObjectUtils.nullSafeHashCode(language);
		result += 31 * ObjectUtils.nullSafeHashCode(partialFilterExpression);
		result += 31 * ObjectUtils.nullSafeHashCode(collation);
		result += 31 * ObjectUtils.nullSafeHashCode(expireAfter);
		result += 31 * ObjectUtils.nullSafeHashCode(hidden);
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
		if (getClass() != obj.getClass()) {
			return false;
		}
		IndexInfo other = (IndexInfo) obj;
		if (indexFields == null) {
			if (other.indexFields != null) {
				return false;
			}
		} else if (!indexFields.equals(other.indexFields)) {
			return false;
		}
		if (name == null) {
			if (other.name != null) {
				return false;
			}
		} else if (!name.equals(other.name)) {
			return false;
		}
		if (sparse != other.sparse) {
			return false;
		}
		if (unique != other.unique) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(language, other.language)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(partialFilterExpression, other.partialFilterExpression)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(collation, other.collation)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(expireAfter, other.expireAfter)) {
			return false;
		}
		if (hidden != other.hidden) {
			return false;
		}
		return true;
	}

}
