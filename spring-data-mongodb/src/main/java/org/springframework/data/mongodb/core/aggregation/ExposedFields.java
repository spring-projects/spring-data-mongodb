/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.mongodb.core.aggregation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.util.Assert;
import org.springframework.util.CompositeIterator;

/**
 * Value object to capture the fields exposed by an {@link AggregationOperation}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @since 1.3
 */
public class ExposedFields implements Iterable<ExposedField> {

	private static final List<ExposedField> NO_FIELDS = Collections.emptyList();
	private static final ExposedFields EMPTY = new ExposedFields(NO_FIELDS, NO_FIELDS);

	private final List<ExposedField> originalFields;
	private final List<ExposedField> syntheticFields;

	/**
	 * Creates a new {@link ExposedFields} instance from the given {@link ExposedField}s.
	 * 
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	public static ExposedFields from(ExposedField... fields) {
		return from(Arrays.asList(fields));
	}

	/**
	 * Creates a new {@link ExposedFields} instance from the given {@link ExposedField}s.
	 * 
	 * @param fields must not be {@literal null}.
	 * @return
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
	 * @return
	 */
	public static ExposedFields synthetic(Fields fields) {
		return createFields(fields, true);
	}

	/**
	 * Creates non-synthetic {@link ExposedFields} from the given {@link Fields}.
	 * 
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	public static ExposedFields nonSynthetic(Fields fields) {
		return createFields(fields, false);
	}

	/**
	 * Creates a new {@link ExposedFields} instance for the given fields in either sythetic or non-synthetic way.
	 * 
	 * @param fields must not be {@literal null}.
	 * @param synthetic
	 * @return
	 */
	private static ExposedFields createFields(Fields fields, boolean synthetic) {

		Assert.notNull(fields, "Fields must not be null!");
		List<ExposedField> result = new ArrayList<ExposedField>();

		for (Field field : fields) {
			result.add(new ExposedField(field, synthetic));
		}

		return ExposedFields.from(result);
	}

	/**
	 * Creates a new {@link ExposedFields} with the given orignals and synthetics.
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
	 * @return
	 */
	public ExposedFields and(ExposedField field) {

		Assert.notNull(field, "Exposed field must not be null!");

		ArrayList<ExposedField> result = new ArrayList<ExposedField>();
		result.addAll(field.synthetic ? syntheticFields : originalFields);
		result.add(field);

		return new ExposedFields(field.synthetic ? originalFields : result, field.synthetic ? result : syntheticFields);
	}

	/**
	 * Returns the field with the given name or {@literal null} if no field with the given name is available.
	 * 
	 * @param name
	 * @return
	 */
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

	/* 
	 * (non-Javadoc)
	 * @see java.lang.Iterable#iterator()
	 */
	@Override
	public Iterator<ExposedField> iterator() {

		CompositeIterator<ExposedField> iterator = new CompositeIterator<ExposedField>();
		iterator.add(syntheticFields.iterator());
		iterator.add(originalFields.iterator());

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

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.Field#getKey()
		 */
		@Override
		public String getName() {
			return field.getName();
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.Field#getTarget()
		 */
		@Override
		public String getTarget() {
			return field.getTarget();
		}

		/**
		 * Returns whether the field can be referred to using the given name.
		 * 
		 * @param input
		 * @return
		 */
		public boolean canBeReferredToBy(String input) {
			return getTarget().equals(input);
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("AggregationField: %s, synthetic: %s", field, synthetic);
		}

		/* 
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof ExposedField)) {
				return false;
			}

			ExposedField that = (ExposedField) obj;

			return this.field.equals(that.field) && this.synthetic == that.synthetic;
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
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
	 * @author Oliver Gierke
	 */
	static class FieldReference {

		private final ExposedField field;

		/**
		 * Creates a new {@link FieldReference} for the given {@link ExposedField}.
		 * 
		 * @param field must not be {@literal null}.
		 */
		public FieldReference(ExposedField field) {

			Assert.notNull(field, "ExposedField must not be null!");
			this.field = field;
		}

		/**
		 * @return
		 */
		public boolean isSynthetic() {
			return field.synthetic;
		}

		/**
		 * Returns the raw, unqualified reference, i.e. the field reference without a {@literal $} prefix.
		 * 
		 * @return
		 */
		public String getRaw() {

			String target = field.getTarget();

			return field.synthetic ? target : String.format("%s.%s", Fields.UNDERSCORE_ID, target);
		}

		/*
		 * (non-Javadoc)
		 * @see java.lang.Object#toString()
		 */
		@Override
		public String toString() {
			return String.format("$%s", getRaw());
		}

		/* 
		 * (non-Javadoc)
		 * @see java.lang.Object#equals(java.lang.Object)
		 */
		@Override
		public boolean equals(Object obj) {

			if (this == obj) {
				return true;
			}

			if (!(obj instanceof FieldReference)) {
				return false;
			}

			FieldReference that = (FieldReference) obj;

			return this.field.equals(that.field);
		}

		/* 
		 * (non-Javadoc)
		 * @see java.lang.Object#hashCode()
		 */
		@Override
		public int hashCode() {
			return field.hashCode();
		}
	}
}
