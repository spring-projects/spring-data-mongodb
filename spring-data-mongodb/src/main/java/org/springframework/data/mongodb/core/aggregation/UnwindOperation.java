/*
 * Copyright 2013-2016 the original author or authors.
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

import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $unwind}-operation.
 * <p>
 * We recommend to use the static factory method {@link Aggregation#unwind(String)} instead of creating instances of
 * this class directly.
 * 
 * @see http://docs.mongodb.org/manual/reference/aggregation/unwind/#pipe._S_unwind
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Mark Paluch
 * @since 1.3
 */
public class UnwindOperation
		implements AggregationOperation, FieldsExposingAggregationOperation.InheritsFieldsAggregationOperation {

	private final ExposedField field;
	private final ExposedField arrayIndex;
	private final boolean preserveNullAndEmptyArrays;

	/**
	 * Creates a new {@link UnwindOperation} for the given {@link Field}.
	 * 
	 * @param field must not be {@literal null}.
	 */
	public UnwindOperation(Field field) {
		this(new ExposedField(field, true), false);
	}

	/**
	 * Creates a new {@link UnwindOperation} using Mongo 3.2 syntax.
	 * 
	 * @param field must not be {@literal null}.
	 * @param preserveNullAndEmptyArrays {@literal true} to output the document if path is {@literal null}, missing or
	 *          array is empty.
	 * @since 1.10
	 */
	public UnwindOperation(Field field, boolean preserveNullAndEmptyArrays) {
		Assert.notNull(field, "Field must not be null!");

		this.field = new ExposedField(field, true);
		this.arrayIndex = null;
		this.preserveNullAndEmptyArrays = preserveNullAndEmptyArrays;
	}

	/**
	 * Creates a new {@link UnwindOperation} using Mongo 3.2 syntax.
	 * 
	 * @param field must not be {@literal null}.
	 * @param arrayIndex optional field name to expose the field array index, must not be {@literal null}.
	 * @param preserveNullAndEmptyArrays {@literal true} to output the document if path is {@literal null}, missing or
	 *          array is empty.
	 * @since 1.10
	 */
	public UnwindOperation(Field field, Field arrayIndex, boolean preserveNullAndEmptyArrays) {

		Assert.notNull(field, "Field must not be null!");
		Assert.notNull(arrayIndex, "ArrayIndex must not be null!");

		this.field = new ExposedField(field, true);
		this.arrayIndex = new ExposedField(arrayIndex, true);
		this.preserveNullAndEmptyArrays = preserveNullAndEmptyArrays;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public DBObject toDBObject(AggregationOperationContext context) {

		String unwindField = context.getReference(field).toString();
		Object unwindArg;

		if (preserveNullAndEmptyArrays || arrayIndex != null) {

			BasicDBObjectBuilder builder = BasicDBObjectBuilder.start().add("path", unwindField);
			builder.add("preserveNullAndEmptyArrays", preserveNullAndEmptyArrays);

			if (arrayIndex != null) {
				builder.add("includeArrayIndex", arrayIndex.getName());
			}

			unwindArg = builder.get();
		} else {
			unwindArg = unwindField;
		}

		return new BasicDBObject("$unwind", unwindArg);
	}

	@Override
	public ExposedFields getFields() {
		return arrayIndex != null ? ExposedFields.from(arrayIndex) : ExposedFields.from();
	}

	/**
	 * Get a builder that allows creation of {@link LookupOperation}.
	 *
	 * @return
	 */
	public static PathBuilder newUnwind() {
		return UnwindOperationBuilder.newBuilder();
	}

	public static interface PathBuilder {

		/**
		 * @param path the path to unwind, must not be {@literal null} or empty.
		 * @return
		 */
		IndexBuilder path(String path);
	}

	public static interface IndexBuilder {

		/**
		 * Exposes the array index as {@code field}.
		 * 
		 * @param field field name to expose the field array index, must not be {@literal null} or empty.
		 * @return
		 */
		EmptyArraysBuilder arrayIndex(String field);

		/**
		 * Do not expose the array index.
		 * 
		 * @return
		 */
		EmptyArraysBuilder noArrayIndex();
	}

	public static interface EmptyArraysBuilder {

		/**
		 * Output documents if the array is null or empty.
		 * 
		 * @return
		 */
		UnwindOperation preserveNullAndEmptyArrays();

		/**
		 * Do not output documents if the array is null or empty.
		 * 
		 * @return
		 */
		UnwindOperation skipNullAndEmptyArrays();
	}

	/**
	 * Builder for fluent {@link UnwindOperation} creation.
	 *
	 * @author Mark Paluch
	 * @since 1.10
	 */
	public static final class UnwindOperationBuilder implements PathBuilder, IndexBuilder, EmptyArraysBuilder {

		private Field field;
		private Field arrayIndex;

		private UnwindOperationBuilder() {}

		/**
		 * Creates new builder for {@link UnwindOperation}.
		 *
		 * @return never {@literal null}.
		 */
		public static PathBuilder newBuilder() {
			return new UnwindOperationBuilder();
		}

		@Override
		public UnwindOperation preserveNullAndEmptyArrays() {

			if (arrayIndex != null) {
				return new UnwindOperation(field, arrayIndex, true);
			}

			return new UnwindOperation(field, true);
		}

		@Override
		public UnwindOperation skipNullAndEmptyArrays() {

			if (arrayIndex != null) {
				return new UnwindOperation(field, arrayIndex, false);
			}

			return new UnwindOperation(field, false);
		}

		@Override
		public EmptyArraysBuilder arrayIndex(String field) {
			Assert.hasText(field, "'ArrayIndex' must not be null or empty!");
			arrayIndex = Fields.field(field);
			return this;
		}

		@Override
		public EmptyArraysBuilder noArrayIndex() {
			arrayIndex = null;
			return this;
		}

		@Override
		public UnwindOperationBuilder path(String path) {
			Assert.hasText(path, "'Path' must not be null or empty!");
			field = Fields.field(path);
			return this;
		}

	}

}
