/*
 * Copyright 2010-2023 the original author or authors.
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
package org.springframework.data.mongodb.core;

import java.util.Optional;

import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.schema.MongoJsonSchema;
import org.springframework.data.mongodb.core.timeseries.Granularity;
import org.springframework.data.mongodb.core.timeseries.GranularityDefinition;
import org.springframework.data.mongodb.core.validation.Validator;
import org.springframework.data.util.Optionals;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ObjectUtils;

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
	private ValidationOptions validationOptions;
	private @Nullable TimeSeriesOptions timeSeriesOptions;
	private @Nullable CollectionChangeStreamOptions changeStreamOptions;

	private CollectionOptions(@Nullable Long size, @Nullable Long maxDocuments, @Nullable Boolean capped,
			@Nullable Collation collation, ValidationOptions validationOptions, @Nullable TimeSeriesOptions timeSeriesOptions,
			@Nullable CollectionChangeStreamOptions changeStreamOptions) {

		this.maxDocuments = maxDocuments;
		this.size = size;
		this.capped = capped;
		this.collation = collation;
		this.validationOptions = validationOptions;
		this.timeSeriesOptions = timeSeriesOptions;
		this.changeStreamOptions = changeStreamOptions;
	}

	/**
	 * Create new {@link CollectionOptions} by just providing the {@link Collation} to use.
	 *
	 * @param collation must not be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public static CollectionOptions just(Collation collation) {

		Assert.notNull(collation, "Collation must not be null");

		return new CollectionOptions(null, null, null, collation, ValidationOptions.none(), null, null);
	}

	/**
	 * Create new empty {@link CollectionOptions}.
	 *
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public static CollectionOptions empty() {
		return new CollectionOptions(null, null, null, null, ValidationOptions.none(), null, null);
	}

	/**
	 * Quick way to set up {@link CollectionOptions} for a Time Series collection. For more advanced settings use
	 * {@link #timeSeries(TimeSeriesOptions)}.
	 *
	 * @param timeField The name of the property which contains the date in each time series document. Must not be
	 *          {@literal null}.
	 * @return new instance of {@link CollectionOptions}.
	 * @see #timeSeries(TimeSeriesOptions)
	 * @since 3.3
	 */
	public static CollectionOptions timeSeries(String timeField) {
		return empty().timeSeries(TimeSeriesOptions.timeSeries(timeField));
	}

	/**
	 * Quick way to set up {@link CollectionOptions} for emitting (pre & post) change events.
	 *
	 * @return new instance of {@link CollectionOptions}.
	 * @see #changeStream(CollectionChangeStreamOptions)
	 * @see CollectionChangeStreamOptions#preAndPostImages(boolean)
	 * @since 4.0
	 */
	public static CollectionOptions emitChangedRevisions() {
		return empty().changeStream(CollectionChangeStreamOptions.preAndPostImages(true));
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and capped set to {@literal true}. <br />
	 * <strong>NOTE:</strong> Using capped collections requires defining {@link #size(long)}.
	 *
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions capped() {
		return new CollectionOptions(size, maxDocuments, true, collation, validationOptions, timeSeriesOptions,
				changeStreamOptions);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code maxDocuments} set to given value.
	 *
	 * @param maxDocuments can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions maxDocuments(long maxDocuments) {
		return new CollectionOptions(size, maxDocuments, capped, collation, validationOptions, timeSeriesOptions,
				changeStreamOptions);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code size} set to given value.
	 *
	 * @param size can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions size(long size) {
		return new CollectionOptions(size, maxDocuments, capped, collation, validationOptions, timeSeriesOptions,
				changeStreamOptions);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code collation} set to given value.
	 *
	 * @param collation can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.0
	 */
	public CollectionOptions collation(@Nullable Collation collation) {
		return new CollectionOptions(size, maxDocuments, capped, collation, validationOptions, timeSeriesOptions,
				changeStreamOptions);
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code validationOptions} set to given
	 * {@link MongoJsonSchema}.
	 *
	 * @param schema can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.1
	 */
	public CollectionOptions schema(@Nullable MongoJsonSchema schema) {
		return validator(Validator.schema(schema));
	}

	/**
	 * Create new {@link CollectionOptions} with already given settings and {@code validationOptions} set to given
	 * {@link Validator}.
	 *
	 * @param validator can be {@literal null}.
	 * @return new {@link CollectionOptions}.
	 * @since 2.1
	 */
	public CollectionOptions validator(@Nullable Validator validator) {
		return validation(validationOptions.validator(validator));
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

		Assert.notNull(validationLevel, "ValidationLevel must not be null");
		return validation(validationOptions.validationLevel(validationLevel));
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

		Assert.notNull(validationAction, "ValidationAction must not be null");
		return validation(validationOptions.validationAction(validationAction));
	}

	/**
	 * Create new {@link CollectionOptions} with the given {@link ValidationOptions}.
	 *
	 * @param validationOptions must not be {@literal null}. Use {@link ValidationOptions#none()} to remove validation.
	 * @return new {@link CollectionOptions}.
	 * @since 2.1
	 */
	public CollectionOptions validation(ValidationOptions validationOptions) {

		Assert.notNull(validationOptions, "ValidationOptions must not be null");
		return new CollectionOptions(size, maxDocuments, capped, collation, validationOptions, timeSeriesOptions,
				changeStreamOptions);
	}

	/**
	 * Create new {@link CollectionOptions} with the given {@link TimeSeriesOptions}.
	 *
	 * @param timeSeriesOptions must not be {@literal null}.
	 * @return new instance of {@link CollectionOptions}.
	 * @since 3.3
	 */
	public CollectionOptions timeSeries(TimeSeriesOptions timeSeriesOptions) {

		Assert.notNull(timeSeriesOptions, "TimeSeriesOptions must not be null");
		return new CollectionOptions(size, maxDocuments, capped, collation, validationOptions, timeSeriesOptions,
				changeStreamOptions);
	}

	/**
	 * Create new {@link CollectionOptions} with the given {@link TimeSeriesOptions}.
	 *
	 * @param changeStreamOptions must not be {@literal null}.
	 * @return new instance of {@link CollectionOptions}.
	 * @since 3.3
	 */
	public CollectionOptions changeStream(CollectionChangeStreamOptions changeStreamOptions) {

		Assert.notNull(changeStreamOptions, "ChangeStreamOptions must not be null");
		return new CollectionOptions(size, maxDocuments, capped, collation, validationOptions, timeSeriesOptions,
				changeStreamOptions);
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
	public Optional<ValidationOptions> getValidationOptions() {
		return validationOptions.isEmpty() ? Optional.empty() : Optional.of(validationOptions);
	}

	/**
	 * Get the {@link TimeSeriesOptions} if available.
	 *
	 * @return {@link Optional#empty()} if not specified.
	 * @since 3.3
	 */
	public Optional<TimeSeriesOptions> getTimeSeriesOptions() {
		return Optional.ofNullable(timeSeriesOptions);
	}

	/**
	 * Get the {@link CollectionChangeStreamOptions} if available.
	 *
	 * @return {@link Optional#empty()} if not specified.
	 * @since 4.0
	 */
	public Optional<CollectionChangeStreamOptions> getChangeStreamOptions() {
		return Optional.ofNullable(changeStreamOptions);
	}

	@Override
	public String toString() {
		return "CollectionOptions{" + "maxDocuments=" + maxDocuments + ", size=" + size + ", capped=" + capped
				+ ", collation=" + collation + ", validationOptions=" + validationOptions + ", timeSeriesOptions="
				+ timeSeriesOptions + ", changeStreamOptions=" + changeStreamOptions + ", disableValidation="
				+ disableValidation() + ", strictValidation=" + strictValidation() + ", moderateValidation="
				+ moderateValidation() + ", warnOnValidationError=" + warnOnValidationError() + ", failOnValidationError="
				+ failOnValidationError() + '}';
	}

	@Override
	public boolean equals(@Nullable Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}

		CollectionOptions that = (CollectionOptions) o;

		if (!ObjectUtils.nullSafeEquals(maxDocuments, that.maxDocuments)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(size, that.size)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(capped, that.capped)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(collation, that.collation)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(validationOptions, that.validationOptions)) {
			return false;
		}
		if (!ObjectUtils.nullSafeEquals(timeSeriesOptions, that.timeSeriesOptions)) {
			return false;
		}
		return ObjectUtils.nullSafeEquals(changeStreamOptions, that.changeStreamOptions);
	}

	@Override
	public int hashCode() {
		int result = ObjectUtils.nullSafeHashCode(maxDocuments);
		result = 31 * result + ObjectUtils.nullSafeHashCode(size);
		result = 31 * result + ObjectUtils.nullSafeHashCode(capped);
		result = 31 * result + ObjectUtils.nullSafeHashCode(collation);
		result = 31 * result + ObjectUtils.nullSafeHashCode(validationOptions);
		result = 31 * result + ObjectUtils.nullSafeHashCode(timeSeriesOptions);
		result = 31 * result + ObjectUtils.nullSafeHashCode(changeStreamOptions);
		return result;
	}

	/**
	 * Encapsulation of ValidationOptions options.
	 *
	 * @author Christoph Strobl
	 * @author Andreas Zink
	 * @since 2.1
	 */
	public static class ValidationOptions {

		private static final ValidationOptions NONE = new ValidationOptions(null, null, null);

		private final @Nullable Validator validator;
		private final @Nullable ValidationLevel validationLevel;
		private final @Nullable ValidationAction validationAction;

		public ValidationOptions(Validator validator, ValidationLevel validationLevel, ValidationAction validationAction) {

			this.validator = validator;
			this.validationLevel = validationLevel;
			this.validationAction = validationAction;
		}

		/**
		 * Create an empty {@link ValidationOptions}.
		 *
		 * @return never {@literal null}.
		 */
		public static ValidationOptions none() {
			return NONE;
		}

		/**
		 * Define the {@link Validator} to be used for document validation.
		 *
		 * @param validator can be {@literal null}.
		 * @return new instance of {@link ValidationOptions}.
		 */
		public ValidationOptions validator(@Nullable Validator validator) {
			return new ValidationOptions(validator, validationLevel, validationAction);
		}

		/**
		 * Define the validation level to apply.
		 *
		 * @param validationLevel can be {@literal null}.
		 * @return new instance of {@link ValidationOptions}.
		 */
		public ValidationOptions validationLevel(ValidationLevel validationLevel) {
			return new ValidationOptions(validator, validationLevel, validationAction);
		}

		/**
		 * Define the validation action to take.
		 *
		 * @param validationAction can be {@literal null}.
		 * @return new instance of {@link ValidationOptions}.
		 */
		public ValidationOptions validationAction(ValidationAction validationAction) {
			return new ValidationOptions(validator, validationLevel, validationAction);
		}

		/**
		 * Get the {@link Validator} to use.
		 *
		 * @return never {@literal null}.
		 */
		public Optional<Validator> getValidator() {
			return Optional.ofNullable(validator);
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
		 * @return {@link Optional#empty()} if not set.
		 */
		public Optional<ValidationAction> getValidationAction() {
			return Optional.ofNullable(validationAction);
		}

		/**
		 * @return {@literal true} if no arguments set.
		 */
		boolean isEmpty() {
			return !Optionals.isAnyPresent(getValidator(), getValidationAction(), getValidationLevel());
		}

		@Override
		public String toString() {

			return "ValidationOptions{" + "validator=" + validator + ", validationLevel=" + validationLevel
					+ ", validationAction=" + validationAction + '}';
		}

		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			ValidationOptions that = (ValidationOptions) o;

			if (!ObjectUtils.nullSafeEquals(validator, that.validator)) {
				return false;
			}
			if (validationLevel != that.validationLevel)
				return false;
			return validationAction == that.validationAction;
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(validator);
			result = 31 * result + ObjectUtils.nullSafeHashCode(validationLevel);
			result = 31 * result + ObjectUtils.nullSafeHashCode(validationAction);
			return result;
		}
	}

	/**
	 * Encapsulation of options applied to define collections change stream behaviour.
	 *
	 * @author Christoph Strobl
	 * @since 4.0
	 */
	public static class CollectionChangeStreamOptions {

		private final boolean preAndPostImages;

		private CollectionChangeStreamOptions(boolean emitChangedRevisions) {
			this.preAndPostImages = emitChangedRevisions;
		}

		/**
		 * Output the version of a document before and after changes (the document pre- and post-images).
		 *
		 * @return new instance of {@link CollectionChangeStreamOptions}.
		 */
		public static CollectionChangeStreamOptions preAndPostImages(boolean emitChangedRevisions) {
			return new CollectionChangeStreamOptions(true);
		}

		public boolean getPreAndPostImages() {
			return preAndPostImages;
		}

		@Override
		public String toString() {
			return "CollectionChangeStreamOptions{" + "preAndPostImages=" + preAndPostImages + '}';
		}

		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			CollectionChangeStreamOptions that = (CollectionChangeStreamOptions) o;

			return preAndPostImages == that.preAndPostImages;
		}

		@Override
		public int hashCode() {
			return (preAndPostImages ? 1 : 0);
		}
	}

	/**
	 * Options applicable to Time Series collections.
	 *
	 * @author Christoph Strobl
	 * @since 3.3
	 * @see <a href=
	 *      "https://docs.mongodb.com/manual/core/timeseries-collections">https://docs.mongodb.com/manual/core/timeseries-collections</a>
	 */
	public static class TimeSeriesOptions {

		private final String timeField;

		private @Nullable final String metaField;

		private final GranularityDefinition granularity;

		private TimeSeriesOptions(String timeField, @Nullable String metaField, GranularityDefinition granularity) {

			Assert.hasText(timeField, "Time field must not be empty or null");

			this.timeField = timeField;
			this.metaField = metaField;
			this.granularity = granularity;
		}

		/**
		 * Create a new instance of {@link TimeSeriesOptions} using the given field as its {@literal timeField}. The one,
		 * that contains the date in each time series document. <br />
		 * {@link Field#name() Annotated fieldnames} will be considered during the mapping process.
		 *
		 * @param timeField must not be {@literal null}.
		 * @return new instance of {@link TimeSeriesOptions}.
		 */
		public static TimeSeriesOptions timeSeries(String timeField) {
			return new TimeSeriesOptions(timeField, null, Granularity.DEFAULT);
		}

		/**
		 * Set the name of the field which contains metadata in each time series document. Should not be the {@literal id}
		 * nor {@link TimeSeriesOptions#timeSeries(String)} timeField} nor point to an {@literal array} or
		 * {@link java.util.Collection}. <br />
		 * {@link Field#name() Annotated fieldnames} will be considered during the mapping process.
		 *
		 * @param metaField must not be {@literal null}.
		 * @return new instance of {@link TimeSeriesOptions}.
		 */
		public TimeSeriesOptions metaField(String metaField) {
			return new TimeSeriesOptions(timeField, metaField, granularity);
		}

		/**
		 * Select the {@link GranularityDefinition} parameter to define how data in the time series collection is organized.
		 * Select one that is closest to the time span between incoming measurements.
		 *
		 * @return new instance of {@link TimeSeriesOptions}.
		 * @see Granularity
		 */
		public TimeSeriesOptions granularity(GranularityDefinition granularity) {
			return new TimeSeriesOptions(timeField, metaField, granularity);
		}

		/**
		 * @return never {@literal null}.
		 */
		public String getTimeField() {
			return timeField;
		}

		/**
		 * @return can be {@literal null}. Might be an {@literal empty} {@link String} as well, so maybe check via
		 *         {@link org.springframework.util.StringUtils#hasText(String)}.
		 */
		@Nullable
		public String getMetaField() {
			return metaField;
		}

		/**
		 * @return never {@literal null}.
		 */
		public GranularityDefinition getGranularity() {
			return granularity;
		}

		@Override
		public String toString() {

			return "TimeSeriesOptions{" + "timeField='" + timeField + '\'' + ", metaField='" + metaField + '\''
					+ ", granularity=" + granularity + '}';
		}

		@Override
		public boolean equals(@Nullable Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			TimeSeriesOptions that = (TimeSeriesOptions) o;

			if (!ObjectUtils.nullSafeEquals(timeField, that.timeField)) {
				return false;
			}
			if (!ObjectUtils.nullSafeEquals(metaField, that.metaField)) {
				return false;
			}
			return ObjectUtils.nullSafeEquals(granularity, that.granularity);
		}

		@Override
		public int hashCode() {
			int result = ObjectUtils.nullSafeHashCode(timeField);
			result = 31 * result + ObjectUtils.nullSafeHashCode(metaField);
			result = 31 * result + ObjectUtils.nullSafeHashCode(granularity);
			return result;
		}
	}
}
