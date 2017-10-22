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
import lombok.ToString;

import java.util.Optional;

import org.springframework.data.mongodb.core.CollectionOptions;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * Wraps the collection validation options.
 * 
 * @author Andreas Zink
 * @since 2.1
 * @see {@link CollectionOptions}
 * @see <a href="https://docs.mongodb.com/manual/core/document-validation/">MongoDB Document Validation</a>
 * @see <a href="https://docs.mongodb.com/manual/reference/method/db.createCollection/">MongoDB Collection Options</a>
 */
@EqualsAndHashCode
@ToString
public class ValidationOptions {
	private ValidatorDefinition validator;
	private ValidationLevel validationLevel;
	private ValidationAction validationAction;

	private ValidationOptions(ValidatorDefinition validator) {
		Assert.notNull(validator, "ValidatorDefinition must not be null!");
		this.validator = validator;
	}

	public static ValidationOptions validator(@NonNull ValidatorDefinition validator) {
		return new ValidationOptions(validator);
	}

	public ValidationOptions validationLevel(@Nullable ValidationLevel validationLevel) {
		this.validationLevel = validationLevel;
		return this;
	}

	public ValidationOptions validationAction(@Nullable ValidationAction validationAction) {
		this.validationAction = validationAction;
		return this;
	}

	public ValidatorDefinition getValidator() {
		return validator;
	}

	public Optional<ValidationLevel> getValidationLevel() {
		return Optional.ofNullable(validationLevel);
	}

	public Optional<ValidationAction> getValidationAction() {
		return Optional.ofNullable(validationAction);
	}
}
