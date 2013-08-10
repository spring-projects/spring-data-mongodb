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

import static org.springframework.data.mongodb.core.aggregation.Fields.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.ExposedFields.FieldReference;
import org.springframework.data.mongodb.core.aggregation.Fields.AggregationField;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.SerializationUtils;
import org.springframework.util.Assert;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * An {@code Aggregation} is a representation of a list of aggregation steps to be performed by the MongoDB Aggregation
 * Framework.
 * 
 * @author Tobias Trelle
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @since 1.3
 */
public class Aggregation {

	public static final AggregationOperationContext DEFAULT_CONTEXT = new NoOpAggregationOperationContext();

	private final List<AggregationOperation> operations;

	/**
	 * Creates a new {@link Aggregation} from the given {@link AggregationOperation}s.
	 * 
	 * @param operations must not be {@literal null} or empty.
	 */
	public static Aggregation newAggregation(AggregationOperation... operations) {
		return new Aggregation(operations);
	}

	/**
	 * Creates a new {@link TypedAggregation} for the given type and {@link AggregationOperation}s.
	 * 
	 * @param type must not be {@literal null}.
	 * @param operations must not be {@literal null} or empty.
	 */
	public static <T> TypedAggregation<T> newAggregation(Class<T> type, AggregationOperation... operations) {
		return new TypedAggregation<T>(type, operations);
	}

	/**
	 * Creates a new {@link Aggregation} from the given {@link AggregationOperation}s.
	 * 
	 * @param aggregationOperations must not be {@literal null} or empty.
	 */
	protected Aggregation(AggregationOperation... aggregationOperations) {

		Assert.notNull(aggregationOperations, "AggregationOperations must not be null!");
		Assert.isTrue(aggregationOperations.length > 0, "At least one AggregationOperation has to be provided");

		this.operations = Arrays.asList(aggregationOperations);
	}

	/**
	 * A pointer to the previous {@link AggregationOperation}.
	 * 
	 * @return
	 */
	public static String previousOperation() {
		return "_id";
	}

	/**
	 * Creates a new {@link ProjectionOperation} including the given fields.
	 * 
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	public static ProjectionOperation project(String... fields) {
		return project(fields(fields));
	}

	/**
	 * Creates a new {@link ProjectionOperation} includeing the given {@link Fields}.
	 * 
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	public static ProjectionOperation project(Fields fields) {
		return new ProjectionOperation(fields);
	}

	/**
	 * Factory method to create a new {@link UnwindOperation} for the field with the given name.
	 * 
	 * @param fieldName must not be {@literal null} or empty.
	 * @return
	 */
	public static UnwindOperation unwind(String field) {
		return new UnwindOperation(field(field));
	}

	/**
	 * Creates a new {@link GroupOperation} for the given fields.
	 * 
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	public static GroupOperation group(String... fields) {
		return group(fields(fields));
	}

	/**
	 * Creates a new {@link GroupOperation} for the given {@link Fields}.
	 * 
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	public static GroupOperation group(Fields fields) {
		return new GroupOperation(fields);
	}

	/**
	 * Factory method to create a new {@link SortOperation} for the given {@link Sort}.
	 * 
	 * @param sort must not be {@literal null}.
	 * @return
	 */
	public static SortOperation sort(Sort sort) {
		return new SortOperation(sort);
	}

	/**
	 * Factory method to create a new {@link SortOperation} for the given sort {@link Direction} and {@code fields}.
	 * 
	 * @param direction must not be {@literal null}.
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	public static SortOperation sort(Direction direction, String... fields) {
		return new SortOperation(new Sort(direction, fields));
	}

	/**
	 * Creates a new {@link SkipOperation} skipping the given number of elements.
	 * 
	 * @param elementsToSkip must not be less than zero.
	 * @return
	 */
	public static SkipOperation skip(int elementsToSkip) {
		return new SkipOperation(elementsToSkip);
	}

	/**
	 * Creates a new {@link LimitOperation} limiting the result to the given number of elements.
	 * 
	 * @param maxElements must not be less than zero.
	 * @return
	 */
	public static LimitOperation limit(long maxElements) {
		return new LimitOperation(maxElements);
	}

	/**
	 * Creates a new {@link MatchOperation} using the given {@link Criteria}.
	 * 
	 * @param criteria must not be {@literal null}.
	 * @return
	 */
	public static MatchOperation match(Criteria criteria) {
		return new MatchOperation(criteria);
	}

	/**
	 * Creates a new {@link Fields} instance for the given field names.
	 * 
	 * @see Fields#fields(String...)
	 * @param fields must not be {@literal null}.
	 * @return
	 */
	public static Fields fields(String... fields) {
		return Fields.fields(fields);
	}

	/**
	 * Creates a new {@link Fields} instance from the given field name and target reference.
	 * 
	 * @param name must not be {@literal null} or empty.
	 * @param target must not be {@literal null} or empty.
	 * @return
	 */
	public static Fields bind(String name, String target) {
		return Fields.from(field(name, target));
	}

	/**
	 * Converts this {@link Aggregation} specification to a {@link DBObject}.
	 * 
	 * @param inputCollectionName the name of the input collection
	 * @return the {@code DBObject} representing this aggregation
	 */
	public DBObject toDbObject(String inputCollectionName, AggregationOperationContext rootContext) {

		AggregationOperationContext context = rootContext;
		List<DBObject> operationDocuments = new ArrayList<DBObject>(operations.size());

		for (AggregationOperation operation : operations) {

			operationDocuments.add(operation.toDBObject(context));

			if (operation instanceof AggregationOperationContext) {
				context = (AggregationOperationContext) operation;
			}
		}

		DBObject command = new BasicDBObject("aggregate", inputCollectionName);
		command.put("pipeline", operationDocuments);

		return command;
	}

	/* 
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return SerializationUtils
				.serializeToJsonSafely(toDbObject("__collection__", new NoOpAggregationOperationContext()));
	}

	/**
	 * Simple {@link AggregationOperationContext} that just returns {@link FieldReference}s as is.
	 * 
	 * @author Oliver Gierke
	 */
	private static class NoOpAggregationOperationContext implements AggregationOperationContext {

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperationContext#getMappedObject(com.mongodb.DBObject)
		 */
		@Override
		public DBObject getMappedObject(DBObject dbObject) {
			return dbObject;
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperationContext#getReference(org.springframework.data.mongodb.core.aggregation.ExposedFields.AvailableField)
		 */
		@Override
		public FieldReference getReference(Field field) {
			return new FieldReference(new ExposedField(field, true));
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperationContext#getReference(java.lang.String)
		 */
		@Override
		public FieldReference getReference(String name) {
			return new FieldReference(new ExposedField(new AggregationField(name), true));
		}
	}
}
