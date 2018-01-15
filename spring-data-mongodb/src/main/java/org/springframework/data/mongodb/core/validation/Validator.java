/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.core.validation;

import org.bson.Document;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.util.Assert;

/**
 * Provides a {@code validator} object to be used for collection validation via
 * {@link org.springframework.data.mongodb.core.CollectionOptions.ValidationOptions}.
 *
 * @author Andreas Zink
 * @author Christoph Strobl
 * @since 2.1
 * @see <a href="https://docs.mongodb.com/manual/reference/method/db.createCollection/">MongoDB Collection Options</a>
 */
public interface Validator {

	/**
	 * Get the {@link Document} containing the validation specific rules. The document may contain fields that may require
	 * type and/or field name mapping.
	 *
	 * @return a MongoDB {@code validator} {@link Document}. Never {@literal null}.
	 */
	Document toDocument();

	/**
	 * Creates a basic {@link Validator} checking documents against a given set of rules.
	 *
	 * @param validationRules must not be {@literal null}.
	 * @return new instance of {@link Validator}.
	 * @throws IllegalArgumentException if validationRules is {@literal null}.
	 */
	static Validator document(Document validationRules) {

		Assert.notNull(validationRules, "ValidationRules must not be null!");
		return DocumentValidator.of(validationRules);
	}

	/**
	 * Creates a new {@link Validator} checking documents against the structure defined in {@link MongoJsonSchema}.
	 *
	 * @param schema must not be {@literal null}.
	 * @return new instance of {@link Validator}.
	 * @throws IllegalArgumentException if schema is {@literal null}.
	 */
	static Validator schema(MongoJsonSchema schema) {

		Assert.notNull(schema, "Schema must not be null!");
		return JsonSchemaValidator.of(schema);
	}

	/**
	 * Creates a new {@link Validator} checking documents against a given query structure expressed by
	 * {@link CriteriaDefinition}. <br />
	 *
	 * @param criteria must not be {@literal null}.
	 * @return new instance of {@link Validator}.
	 * @throws IllegalArgumentException if criteria is {@literal null}.
	 */
	static Validator criteria(CriteriaDefinition criteria) {

		Assert.notNull(criteria, "Criteria must not be null!");
		return CriteriaValidator.of(criteria);
	}
}
