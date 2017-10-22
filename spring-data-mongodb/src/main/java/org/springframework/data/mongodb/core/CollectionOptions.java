/*
 * Copyright 2010-2018 the original author or authors.
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
package org.springframework.data.mongodb.core;

import lombok.RequiredArgsConstructor;

import java.util.Optional;

import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.data.mongodb.core.validation.ValidatorDefinition;
import org.springframework.data.util.Optionals;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.client.model.ValidationAction;
import com.mongodb.client.model.ValidationLevel;

/**
 * Provides a simple wrapper to encapsulate the variety of settings you can use when creating a collection.
 *
 * @author Thomas Risberg
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Andreas Zink
 */
public class CollectionOptions {

	private @Nullable Long maxDocuments;
	private @Nullable Long size;
	private @Nullable Boolean capped;
	private @Nullable Collation collation;
	private Validator validator;

	/**
	 * Constructs a new <code>CollectionOptions</code> instance.
	 *
	 * @param size the collection size in bytes, this data space is preallocated. Can be {@literal null}.
	 * @param maxDocuments the maximum number of documents in the collection. Can be {@literal null}.
	 * @param capped true to created a "capped" collection (fixed size with auto-FIFO behavior based on insertion order),
	 *          false otherwise. Can be {@literal null}.
	 * @deprecated since 2.0 please use {@link CollectionOptions#empty()} as entry point.
	 */
	@Deprecated
	public CollectionOptions(@Nullable Long size, @Nullable Long maxDocuments, @Nullable Boolean capped) {
		this(size, maxDocuments, capped, null, Validator.none());
	}

	private CollectionOptions(@Nullable Long size, @Nullable Long maxDocuments, @Nullable Boolean capped,
			@Nullable Collation collation, Validator validator) {

		this.maxDocuments = maxDocuments;
		this.size = size;
		this.capped = capped;
		this.collation = collation;
		this.validator = validator;
	}

	/**
	 * Create new {@link CollectionOptions} by just providing the {@link Collation} to use.
	 *
	 * @param collation must not be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public static CollectionOptions just(Collation collation) {

		Assert.notNull(collation, "Collation must not be null!");

		return new CollectionOptions(null, null, null, collation, Validator.none());
	}

	/**
	 * Create new empty {@link CollectionOptions}.
	 *
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public static CollectionOptions empty() {
		return new CollectionOptions(null, null, null, null, Validator.none());
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and capped set to {@literal true}. <br />
	 * <strong>NOTE</strong> Using capped collections requires defining {@link #size(int)}.
	 *
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions capped() {
		return new CollectionOptions(size, maxDocuments, true, collation, validator);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code maxDocuments} set to given value.
	 *
	 * @param maxDocuments can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions maxDocuments(long maxDocuments) {
		return new CollectionOptions(size, maxDocuments, capped, collation, validator);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code size} set to given value.
	 *
	 * @param size can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions size(long size) {
		return new CollectionOptions(size, maxDocuments, capped, collation, validator);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code collation} set to given value.
	 *
	 * @param collation can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions collation(@Nullable Collation collation) {
		return new CollectionOptions(size, maxDocuments, capped, collation, validator);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code validator} set to given
	 * {@link MongoJsonSchema}.
	 *
	 * @param schema can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.1
	 */
	public CollectionOptions schema(@Nullable MongoJsonSchema schema) {
		return validation(new Validator(schema, null, validator.validationLevel, validator.validationAction));
	}

	public CollectionOptions validatorDefinition(@Nullable ValidatorDefinition definition) {
		return validation(new Validator(null, definition, validator.validationLevel, validator.validationAction));
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code validationLevel} set to
	 * {@link ValidationLevel#OFF}.
	 *
	 * @return new {@link CollectionOptions}.
	 * @since 2.1
	 */
	public CollectionOptions disableValidation() {
		return schemaValidationLevel(ValidationLevel.OFF);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code validationLevel} set to
	 * {@link ValidationLevel#STRICT}.
	 *
	 * @return new {@link CollectionOptions}.
	 * @since 2.1
	 */
	public CollectionOptions strictValidation() {
		return schemaValidationLevel(ValidationLevel.STRICT);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code validationLevel} set to
	 * {@link ValidationLevel#MODERATE}.
	 *
	 * @return new {@link CollectionOptions}.
	 * @since 2.1
	 */
	public CollectionOptions moderateValidation() {
		return schemaValidationLevel(ValidationLevel.MODERATE);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code validationAction} set to
	 * {@link ValidationAction#WARN}.
	 *
	 * @return new {@link CollectionOptions}.
	 * @since 2.1
	 */
	public CollectionOptions warnOnValidationError() {
		return schemaValidationAction(ValidationAction.WARN);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code validationAction} set to
	 * {@link ValidationAction#ERROR}.
	 *
	 * @return new {@link CollectionOptions}.
	 * @since 2.1
	 */
	public CollectionOptions failOnValidationError() {
		return schemaValidationAction(ValidationAction.ERROR);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code validationLevel} set given
	 * {@link ValidationLevel}.
	 *
	 * @param validationLevel must not be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.1
	 */
	public CollectionOptions schemaValidationLevel(ValidationLevel validationLevel) {

		Assert.notNull(validationLevel, "ValidationLevel must not be null!");
		return validation(new Validator(validator.schema, validator.validatorDefinition, validationLevel, validator.validationAction));
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code validationAction} set given
	 * {@link ValidationAction}.
	 *
	 * @param validationAction must not be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.1
	 */
	public CollectionOptions schemaValidationAction(ValidationAction validationAction) {

		Assert.notNull(validationAction, "ValidationAction must not be null!");
		return validation(new Validator(validator.schema, validator.validatorDefinition, validator.validationLevel, validationAction));
	}

	/**
	 * Create new {@link CollectionOptions} with the given {@link Validator}.
	 *
	 * @param validator must not be {@literal null}. Use {@link Validator#none()} to remove validation.
	 * @return new {@link CollectionOptions}.
	 * @since 2.1
	 */
	public CollectionOptions validation(Validator validator) {

		Assert.notNull(validator, "Validator must not be null!");
		return new CollectionOptions(size, maxDocuments, capped, collation, validator);
	}

	/**
	 * Get the max number of documents the collection should be limited to.
	 *
	 * @return {@link Optional#empty()} if not set.
	 */
	public Optional<Long> getMaxDocuments() {
		return Optional.ofNullable(maxDocuments);
	}

	/**
	 * Get the {@literal size} in bytes the collection should be limited to.
	 *
	 * @return {@link Optional#empty()} if not set.
	 */
	public Optional<Long> getSize() {
		return Optional.ofNullable(size);
	}

	/**
	 * Get if the collection should be capped.
	 *
	 * @return {@link Optional#empty()} if not set.
	 * @since 2.0
	 */
	public Optional<Boolean> getCapped() {
		return Optional.ofNullable(capped);
	}

	/**
	 * Get the {@link Collation} settings.
	 *
	 * @return {@link Optional#empty()} if not set.
	 * @since 2.0
	 */
	public Optional<Collation> getCollation() {
		return Optional.ofNullable(collation);
	}

	/**
	 * Get the {@link MongoJsonSchema} for the collection.
	 *
	 * @return {@link Optional#empty()} if not set.
	 * @since 2.1
	 */
	public Optional<Validator> getValidator() {
		return validator.isEmpty() ? Optional.empty() : Optional.of(validator);
	}

	/**
	 * Encapsulation of Validator options.
	 *
	 * @author Christoph Strobl
	 * @author Andreas Zink
	 * @since 2.1
	 */
	@RequiredArgsConstructor
	public static class Validator {

		private static final Validator NONE = new Validator(null, null, null, null);

		private final @Nullable MongoJsonSchema schema;
		private final @Nullable ValidatorDefinition validatorDefinition;
		private final @Nullable ValidationLevel validationLevel;
		private final @Nullable ValidationAction validationAction;

		/**
		 * Create an empty {@link Validator}.
		 *
		 * @return never {@literal null}.
		 */
		public static Validator none() {
			return NONE;
		}

		/**
		 * Get the {@code $jsonSchema} used for validation.
		 *
		 * @return {@link Optional#empty()} if not set.
		 */
		public Optional<MongoJsonSchema> getSchema() {
			return Optional.ofNullable(schema);
		}

		public Optional<ValidatorDefinition> getValidatorDefinition() {
			return Optional.ofNullable(validatorDefinition);
		}

		/**
		 * Get the {@code validationLevel} to apply.
		 *
		 * @return {@link Optional#empty()} if not set.
		 */
		public Optional<ValidationLevel> getValidationLevel() {
			return Optional.ofNullable(validationLevel);
		}

		/**
		 * Get the {@code validationAction} to perform.
		 *
		 * @return @return {@link Optional#empty()} if not set.
		 */
		public Optional<ValidationAction> getValidationAction() {
			return Optional.ofNullable(validationAction);
		}

		/**
		 * @return {@literal true} if no arguments set.
		 */
		boolean isEmpty() {
			return !Optionals.isAnyPresent(getSchema(), getValidatorDefinition(), getValidationAction(), getValidationLevel());
		}
	}
}
