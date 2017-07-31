/*
 * Copyright 2002-2017 the original author or authors.
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

import static org.springframework.data.domain.Sort.Direction.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.bson.Document;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class IndexInfo {

	private static final Double ONE = Double.valueOf(1);
	private static final Double MINUS_ONE = Double.valueOf(-1);
	private static final Collection<String> TWO_D_IDENTIFIERS = Arrays.asList("2d", "2dsphere");

	private final List<IndexField> indexFields;

	private final String name;
	private final boolean unique;
	private final boolean sparse;
	private final String language;
	private @Nullable String partialFilterExpression;
	private @Nullable Document collation;

	public IndexInfo(List<IndexField> indexFields, String name, boolean unique, boolean sparse, String language) {

		this.indexFields = Collections.unmodifiableList(indexFields);
		this.name = name;
		this.unique = unique;
		this.sparse = sparse;
		this.language = language;
	}

	/**
	 * Creates new {@link IndexInfo} parsing required properties from the given {@literal sourceDocument}.
	 *
	 * @param sourceDocument
	 * @return
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

				Double keyValue = new Double(value.toString());

				if (ONE.equals(keyValue)) {
					indexFields.add(IndexField.create(key, ASC));
				} else if (MINUS_ONE.equals(keyValue)) {
					indexFields.add(IndexField.create(key, DESC));
				}
			}
		}

		String name = sourceDocument.get("name").toString();

		boolean unique = sourceDocument.containsKey("unique") ? (Boolean) sourceDocument.get("unique") : false;
		boolean sparse = sourceDocument.containsKey("sparse") ? (Boolean) sourceDocument.get("sparse") : false;
		String language = sourceDocument.containsKey("default_language") ? (String) sourceDocument.get("default_language")
				: "";
		String partialFilter = sourceDocument.containsKey("partialFilterExpression")
				? ((Document) sourceDocument.get("partialFilterExpression")).toJson() : "";

		IndexInfo info = new IndexInfo(indexFields, name, unique, sparse, language);
		info.partialFilterExpression = partialFilter;
		info.collation = sourceDocument.get("collation", Document.class);
		return info;
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

		Assert.notNull(keys, "Collection of keys must not be null!");

		List<String> indexKeys = new ArrayList<String>(indexFields.size());

		for (IndexField field : indexFields) {
			indexKeys.add(field.getKey());
		}

		return indexKeys.containsAll(keys);
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

	@Override
	public String toString() {
		return "IndexInfo [indexFields=" + indexFields + ", name=" + name + ", unique=" + unique + ", sparse=" + sparse
				+ ", language=" + language + ", partialFilterExpression=" + partialFilterExpression + ", collation=" + collation
				+ "]";
	}

	@Override
	public int hashCode() {

		final int prime = 31;
		int result = 1;
		result = prime * result + ObjectUtils.nullSafeHashCode(indexFields);
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + (sparse ? 1231 : 1237);
		result = prime * result + (unique ? 1231 : 1237);
		result = prime * result + ObjectUtils.nullSafeHashCode(language);
		result = prime * result + ObjectUtils.nullSafeHashCode(partialFilterExpression);
		result = prime * result + ObjectUtils.nullSafeHashCode(collation);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
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

		if (!ObjectUtils.nullSafeEquals(collation, collation)) {
			return false;
		}
		return true;
	}
}
