/*
 * Copyright 2013-2022 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.CompositeIterator;
import org.springframework.util.ObjectUtils;

/**
 * Value object to capture the fields exposed by an {@link AggregationOperation}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Mark Paluch
 * @since 1.3
 */
public final class ExposedFields implements Iterable<ExposedField> {

	private static final List<ExposedField> NO_FIELDS = Collections.emptyList();
	private static final ExposedFields EMPTY = new ExposedFields(NO_FIELDS, NO_FIELDS);

	private final List<ExposedField> originalFields;
	private final List<ExposedField> syntheticFields;

	/**
	 * Returns an empty {@link ExposedFields} instance.
	 *
	 * @return never {@literal null}.
	 * @since 2.0
	 */
	public static ExposedFields empty() {
		return EMPTY;
	}

	/**
	 * Creates a new {@link ExposedFields} instance from the given {@link ExposedField}s.
	 *
	 * @param fields must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	public static ExposedFields from(ExposedField... fields) {
		return from(Arrays.asList(fields));
	}

	/**
	 * Creates a new {@link ExposedFields} instance from the given {@link ExposedField}s.
	 *
	 * @param fields must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	private static ExposedFields from(List<ExposedField> fields) {

		ExposedFields result = EMPTY;

		for (ExposedField field : fields) {
			result = result.and(field);
		}

		return result;
	}

	/**
	 * Creates synthetic {@link ExposedFields} from the given {@link Fields}.
	 *
	 * @param fields must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	public static ExposedFields synthetic(Fields fields) {
		return createFields(fields, true);
	}

	/**
	 * Creates non-synthetic {@link ExposedFields} from the given {@link Fields}.
	 *
	 * @param fields must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	public static ExposedFields nonSynthetic(Fields fields) {
		return createFields(fields, false);
	}

	/**
	 * Creates a new {@link ExposedFields} instance for the given fields in either synthetic or non-synthetic way.
	 *
	 * @param fields must not be {@literal null}.
	 * @param synthetic
	 * @return never {@literal null}.
	 */
	private static ExposedFields createFields(Fields fields, boolean synthetic) {

		Assert.notNull(fields, "Fields must not be null");
		List<ExposedField> result = new ArrayList<ExposedField>(fields.size());

		for (Field field : fields) {
			result.add(new ExposedField(field, synthetic));
		}

		return from(result);
	}

	/**
	 * Creates a new {@link ExposedFields} with the given originals and synthetics.
	 *
	 * @param originals must not be {@literal null}.
	 * @param synthetic must not be {@literal null}.
	 */
	private ExposedFields(List<ExposedField> originals, List<ExposedField> synthetic) {

		this.originalFields = originals;
		this.syntheticFields = synthetic;
	}

	/**
	 * Creates a new {@link ExposedFields} adding the given {@link ExposedField}.
	 *
	 * @param field must not be {@literal null}.
	 * @return new instance of {@link ExposedFields}.
	 */
	public ExposedFields and(ExposedField field) {

		Assert.notNull(field, "Exposed field must not be null");

		ArrayList<ExposedField> result = new ArrayList<ExposedField>();
		result.addAll(field.synthetic ? syntheticFields : originalFields);
		result.add(field);

		return new ExposedFields(field.synthetic ? originalFields : result, field.synthetic ? result : syntheticFields);
	}

	/**
	 * Returns the field with the given name or {@literal null} if no field with the given name is available.
	 *
	 * @param name must not be {@literal null}.
	 * @return can be {@literal null}.
	 */
	@Nullable
	public ExposedField getField(String name) {

		for (ExposedField field : this) {
			if (field.canBeReferredToBy(name)) {
				return field;
			}
		}

		return null;
	}

	/**
	 * Returns whether the {@link ExposedFields} exposes no non-synthetic fields at all.
	 *
	 * @return
	 */
	boolean exposesNoNonSyntheticFields() {
		return originalFields.isEmpty();
	}

	/**
	 * Returns whether the {@link ExposedFields} exposes a single non-synthetic field only.
	 *
	 * @return
	 */
	boolean exposesSingleNonSyntheticFieldOnly() {
		return originalFields.size() == 1;
	}

	/**
	 * Returns whether the {@link ExposedFields} exposes no fields at all.
	 *
	 * @return
	 */
	boolean exposesNoFields() {
		return exposedFieldsCount() == 0;
	}

	/**
	 * Returns whether the {@link ExposedFields} exposes a single field only.
	 *
	 * @return
	 */
	boolean exposesSingleFieldOnly() {
		return exposedFieldsCount() == 1;
	}

	/**
	 * @return
	 */
	private int exposedFieldsCount() {
		return originalFields.size() + syntheticFields.size();
	}

	@Override
	public Iterator<ExposedField> iterator() {

		CompositeIterator<ExposedField> iterator = new CompositeIterator<ExposedField>();
		if (!syntheticFields.isEmpty()) {
			iterator.add(syntheticFields.iterator());
		}

		if (!originalFields.isEmpty()) {
			iterator.add(originalFields.iterator());
		}

		return iterator;
	}

	/**
	 * A single exposed field.
	 *
	 * @author Oliver Gierke
	 */
	static class ExposedField implements Field {

		private final boolean synthetic;
		private final Field field;

		/**
		 * Creates a new {@link ExposedField} with the given key.
		 *
		 * @param key must not be {@literal null} or empty.
		 * @param synthetic whether the exposed field is synthetic.
		 */
		public ExposedField(String key, boolean synthetic) {
			this(Fields.field(key), synthetic);
		}

		/**
		 * Creates a new {@link ExposedField} for the given {@link Field}.
		 *
		 * @param delegate must not be {@literal null}.
		 * @param synthetic whether the exposed field is synthetic.
		 */
		public ExposedField(Field delegate, boolean synthetic) {

			this.field = delegate;
			this.synthetic = synthetic;
		}

		@Override
		public String getName() {
			return field.getName();
		}

		@Override
		public String getTarget() {
			return field.getTarget();
		}

		@Override
		public boolean isAliased() {
			return field.isAliased();
		}

		/**
		 * @return the synthetic
		 */
		public boolean isSynthetic() {
			return synthetic;
		}

		/**
		 * Returns whether the field can be referred to using the given name.
		 *
		 * @param name
		 * @return
		 */
		public boolean canBeReferredToBy(String name) {
			return getName().equals(name) || getTarget().equals(name);
		}

		@Override
		public String toString() {
			return String.format("AggregationField: %s, synthetic: %s", field, synthetic);
		}

		@Override
		public boolean equals(@Nullable Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof ExposedField)) {
				return false;
			}

			ExposedField that = (ExposedField) obj;

			return this.field.equals(that.field) && this.synthetic == that.synthetic;
		}

		@Override
		public int hashCode() {

			int result = 17;

			result += 31 * field.hashCode();
			result += 31 * (synthetic ? 0 : 1);

			return result;
		}
	}

	/**
	 * A reference to an {@link ExposedField}.
	 *
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	public interface FieldReference {

		/**
		 * Returns the raw, unqualified reference, i.e. the field reference without a {@literal $} prefix.
		 *
		 * @return
		 */
		String getRaw();

		/**
		 * Returns the reference value for the given field reference. Will return 1 for a synthetic, unaliased field or the
		 * raw rendering of the reference otherwise.
		 *
		 * @return
		 */
		Object getReferenceValue();
	}

	/**
	 * A reference to an {@link ExposedField}.
	 *
	 * @author Oliver Gierke
	 */
	static class DirectFieldReference implements FieldReference {

		private final ExposedField field;

		/**
		 * Creates a new {@link FieldReference} for the given {@link ExposedField}.
		 *
		 * @param field must not be {@literal null}.
		 */
		public DirectFieldReference(ExposedField field) {

			Assert.notNull(field, "ExposedField must not be null");

			this.field = field;
		}

		public String getRaw() {

			String target = field.getTarget();
			return field.synthetic ? target : String.format("%s.%s", Fields.UNDERSCORE_ID, target);
		}

		public Object getReferenceValue() {
			return field.synthetic && !field.isAliased() ? 1 : toString();
		}

		@Override
		public String toString() {

			if (getRaw().startsWith("$")) {
				return getRaw();
			}

			return String.format("$%s", getRaw());
		}

		@Override
		public boolean equals(@Nullable Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof DirectFieldReference)) {
				return false;
			}

			DirectFieldReference that = (DirectFieldReference) obj;

			return this.field.equals(that.field);
		}

		@Override
		public int hashCode() {
			return field.hashCode();
		}
	}

	/**
	 * A {@link FieldReference} to a {@link Field} used within a nested {@link AggregationExpression}.
	 *
	 * @author Christoph Strobl
	 * @since 1.10
	 */
	static class ExpressionFieldReference implements FieldReference {

		private FieldReference delegate;

		/**
		 * Creates a new {@link FieldReference} for the given {@link ExposedField}.
		 *
		 * @param field must not be {@literal null}.
		 */
		public ExpressionFieldReference(FieldReference field) {
			delegate = field;
		}

		@Override
		public String getRaw() {
			return delegate.getRaw();
		}

		@Override
		public Object getReferenceValue() {
			return delegate.getReferenceValue();
		}

		@Override
		public String toString() {

			String fieldRef = delegate.toString();

			if (fieldRef.startsWith("$$")) {
				return fieldRef;
			}

			if (fieldRef.startsWith("$")) {
				return "$" + fieldRef;
			}

			return fieldRef;
		}

		@Override
		public boolean equals(@Nullable Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof ExpressionFieldReference)) {
				return false;
			}

			ExpressionFieldReference that = (ExpressionFieldReference) obj;
			return ObjectUtils.nullSafeEquals(this.delegate, that.delegate);
		}

		@Override
		public int hashCode() {
			return delegate.hashCode();
		}
	}
}
