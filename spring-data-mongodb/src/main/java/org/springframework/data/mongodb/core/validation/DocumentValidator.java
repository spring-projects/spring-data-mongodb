/*
 * Copyright 2018-2022 the original author or authors.
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
package org.springframework.data.mongodb.core.validation;

import org.bson.Document;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

/**
 * Most trivial {@link Validator} implementation using plain {@link Document} to describe the desired document structure
 * which can be either a {@code $jsonSchema} or query expression.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 * @see <a href="https://docs.mongodb.com/manual/core/schema-validation/">Schema Validation</a>
 */
class DocumentValidator implements Validator {

	private final Document validatorObject;

	private DocumentValidator(Document validatorObject) {
		this.validatorObject = validatorObject;
	}

	/**
	 * Create new {@link DocumentValidator} defining validation rules via a plain {@link Document}.
	 *
	 * @param validatorObject must not be {@literal null}.
	 * @throws IllegalArgumentException if validatorObject is {@literal null}.
	 */
	static DocumentValidator of(Document validatorObject) {

		Assert.notNull(validatorObject, "ValidatorObject must not be null");

		return new DocumentValidator(new Document(validatorObject));
	}

	@Override
	public Document toDocument() {
		return new Document(validatorObject);
	}

	@Override
	public String toString() {
		return SerializationUtils.serializeToJsonSafely(validatorObject);
	}

	@Override
	public boolean equals(@Nullable Object o) {

		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;

		DocumentValidator that = (DocumentValidator) o;

		return ObjectUtils.nullSafeEquals(validatorObject, that.validatorObject);
	}

	@Override
	public int hashCode() {
		return ObjectUtils.nullSafeHashCode(validatorObject);
	}
}
