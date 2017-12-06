/*
 * Copyright 2017 the original author or authors.
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

import lombok.EqualsAndHashCode;

import org.bson.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

/**
 * Utility to build a MongoDB {@code validator} based on a {@link CriteriaDefinition}.
 * 
 * @author Andreas Zink
 * @since 2.1
 * @see Criteria
 */
@EqualsAndHashCode
public class CriteriaValidator implements ValidatorDefinition {

	private final Document document;

	private CriteriaValidator(Document document) {
		Assert.notNull(document, "Document must not be null!");
		this.document = document;
	}

	/**
	 * Builds a {@code validator} object, which is basically setup of query operators, based on a
	 * {@link CriteriaDefinition} instance.
	 * 
	 * @param criteria the criteria to build the {@code validator} from
	 * @return
	 */
	public static CriteriaValidator fromCriteria(@NonNull CriteriaDefinition criteria) {
		Assert.notNull(criteria, "Criteria must not be null!");
		return new CriteriaValidator(criteria.getCriteriaObject());
	}

	@Override
	public Document toDocument() {
		return this.document;
	}

	@Override
	public String toString() {
		return document.toString();
	}

}
