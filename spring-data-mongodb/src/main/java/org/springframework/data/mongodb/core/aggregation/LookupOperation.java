/*
 * Copyright 2016 the original author or authors.
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
import org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation.InheritsFieldsAggregationOperation;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $lookup}-operation. We recommend to use the static factory method
 * {@link Aggregation#lookup(String, String, String, String)} instead of creating instances of this class directly.
 *
 * @author Alessio Fachechi
 * @author Christoph Strobl
 * @author Mark Paluch
 * @see http://docs.mongodb.org/manual/reference/aggregation/lookup/#stage._S_lookup
 * @since 1.9
 */
public class LookupOperation implements FieldsExposingAggregationOperation, InheritsFieldsAggregationOperation {

	private Field from;
	private Field localField;
	private Field foreignField;
	private ExposedField as;

	/**
	 * Creates a new {@link LookupOperation} for the given {@link Field}s.
	 *
	 * @param from must not be {@literal null}.
	 * @param localField must not be {@literal null}.
	 * @param foreignField must not be {@literal null}.
	 * @param as must not be {@literal null}.
	 */
	public LookupOperation(Field from, Field localField, Field foreignField, Field as) {

		Assert.notNull(from, "From must not be null!");
		Assert.notNull(localField, "LocalField must not be null!");
		Assert.notNull(foreignField, "ForeignField must not be null!");
		Assert.notNull(as, "As must not be null!");

		this.from = from;
		this.localField = localField;
		this.foreignField = foreignField;
		this.as = new ExposedField(as, true);
	}

	private LookupOperation() {
		// used by builder
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation#getFields()
	 */
	@Override
	public ExposedFields getFields() {
		return ExposedFields.from(as);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public DBObject toDBObject(AggregationOperationContext context) {

		BasicDBObject lookupObject = new BasicDBObject();

		lookupObject.append("from", from.getTarget());
		lookupObject.append("localField", localField.getTarget());
		lookupObject.append("foreignField", foreignField.getTarget());
		lookupObject.append("as", as.getTarget());

		return new BasicDBObject("$lookup", lookupObject);
	}

	/**
	 * Get a builder that allows creation of {@link LookupOperation}.
	 *
	 * @return
	 */
	public static FromBuilder newLookup() {
		return new LookupOperationBuilder();
	}

	public static interface FromBuilder {

		/**
		 * @param name the collection in the same database to perform the join with, must not be {@literal null} or empty.
		 * @return
		 */
		LocalFieldBuilder from(String name);
	}

	public static interface LocalFieldBuilder {

		/**
		 * @param name the field from the documents input to the {@code $lookup} stage, must not be {@literal null} or
		 *          empty.
		 * @return
		 */
		ForeignFieldBuilder localField(String name);
	}

	public static interface ForeignFieldBuilder {

		/**
		 * @param name the field from the documents in the {@code from} collection, must not be {@literal null} or empty.
		 * @return
		 */
		AsBuilder foreignField(String name);
	}

	public static interface AsBuilder {

		/**
		 * @param name the name of the new array field to add to the input documents, must not be {@literal null} or empty.
		 * @return
		 */
		LookupOperation as(String name);
	}

	/**
	 * Builder for fluent {@link LookupOperation} creation.
	 *
	 * @author Christoph Strobl
	 * @since 1.9
	 */
	public static final class LookupOperationBuilder
			implements FromBuilder, LocalFieldBuilder, ForeignFieldBuilder, AsBuilder {

		private final LookupOperation lookupOperation;

		private LookupOperationBuilder() {
			this.lookupOperation = new LookupOperation();
		}

		/**
		 * Creates new builder for {@link LookupOperation}.
		 *
		 * @return never {@literal null}.
		 */
		public static FromBuilder newBuilder() {
			return new LookupOperationBuilder();
		}

		@Override
		public LocalFieldBuilder from(String name) {

			Assert.hasText(name, "'From' must not be null or empty!");
			lookupOperation.from = Fields.field(name);
			return this;
		}

		@Override
		public LookupOperation as(String name) {

			Assert.hasText(name, "'As' must not be null or empty!");
			lookupOperation.as = new ExposedField(Fields.field(name), true);
			return new LookupOperation(lookupOperation.from, lookupOperation.localField, lookupOperation.foreignField,
					lookupOperation.as);
		}

		@Override
		public AsBuilder foreignField(String name) {

			Assert.hasText(name, "'ForeignField' must not be null or empty!");
			lookupOperation.foreignField = Fields.field(name);
			return this;
		}

		@Override
		public ForeignFieldBuilder localField(String name) {

			Assert.hasText(name, "'LocalField' must not be null or empty!");
			lookupOperation.localField = Fields.field(name);
			return this;
		}
	}
}
