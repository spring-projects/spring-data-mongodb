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
import java.util.List;

import org.springframework.data.domain.Sort;
import org.springframework.data.domain.Sort.Direction;
import org.springframework.data.mongodb.MongoCollectionUtils;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * @author Tobias Trelle - Original API and Implementation
 * @author Thomas Darimont - Refactoring, embedded DSL
 */
public class Aggregation<I, O> implements HasToDbObject {

	private Class<I> inputType;

	private final List<AggregationOperation> operations = new ArrayList<AggregationOperation>();

	private final String inputCollectionName;

	private Aggregation(String inputCollectionName) {
		this.inputCollectionName = inputCollectionName;
	}

	private Aggregation(Class<I> inputType) {
		this(inputType != null ? MongoCollectionUtils.getPreferredCollectionName(inputType) : null);
		this.inputType = inputType;
	}

	/**
	 * Creates a new {@link Aggregation} from the given {@link AggregationOperation}s.
	 * 
	 * @param operations must not be {@literal null} or empty.
	 */
	public Aggregation(AggregationOperation... operations) {

		this((Class<I>) null, operations);
		registerOperations(operations);
	}

	/**
	 * Creates a new {@link Aggregation} from the given {@link AggregationOperation}s.
	 * 
	 * @param operations must not be {@literal null} or empty.
	 */
	public Aggregation(Class<I> inputType, AggregationOperation... operations) {

		this(inputType);
		registerOperations(operations);
	}

	/**
	 * Creates a new {@link Aggregation} from the given {@link AggregationOperation}s.
	 * 
	 * @param operations must not be {@literal null} or empty.
	 */
	public Aggregation(String inputCollectionName, AggregationOperation... operations) {

		this(inputCollectionName);
		registerOperations(operations);
	}

	/**
	 * @param operations
	 */
	private void registerOperations(AggregationOperation... operations) {

		Assert.notNull(operations, "Operations must not be null!");
		Assert.isTrue(operations.length > 0, "operations must not be empty!");
		for (AggregationOperation operation : operations) {
			Assert.notNull(operation, "Operation is not allowed to be null");
			this.operations.add(operation);
		}
	}

	/**
	 * @return the inputType
	 */
	public Class<?> getInputType() {
		return inputType;
	}

	/**
	 * Converts this {@link Aggregation} specification to a @see {@link DBObject}.
	 * 
	 * @see org.springframework.data.mongodb.core.aggregation.HasToDbObject#toDbObject()
	 */
	@Override
	public DBObject toDbObject() {

		DBObject command = new BasicDBObject("aggregate", getInputCollectionName());
		command.put("pipeline", getOperationObjects());
		return command;
	}

	/**
	 * @return
	 */
	private List<DBObject> getOperationObjects() {

		List<DBObject> operationObjects = new ArrayList<DBObject>();
		for (AggregationOperation operation : operations) {
			operationObjects.add(operation.toDbObject());
		}
		return operationObjects;
	}

	/**
	 * @return
	 */
	public String getInputCollectionName() {
		return this.inputCollectionName;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return StringUtils.collectionToCommaDelimitedString(operations);
	}

	/**
	 * Holds the embedded DSL for Mongo DB Aggregation Framework operations. One-stop-shop for a static import.
	 * 
	 * @author Thomas Darimont
	 */
	public static class DSL {

		/**
		 * Factory method to create a new {@link GroupOperation} for the given {@literal id}.
		 * 
		 * @param id, must not be null
		 * @return
		 */
		public static GroupOperation group(DBObject id) {
			return new GroupOperation(id);
		}

		/**
		 * Factory method to create a new {@link GroupOperation} for the given {@literal idFields}.
		 * 
		 * @param idField the first idField to use, must not be null.
		 * @param moreIdFields more id fields to use, can be null.
		 * @return
		 */
		public static GroupOperation group(String idField, String... moreIdFields) {
			return new GroupOperation(idField, moreIdFields);
		}

		/**
		 * Factory method to create a new {@link GroupOperation} for the given {@literal idFields}.
		 * 
		 * @param idFields
		 * @return
		 */
		public static GroupOperation group(Fields idFields) {
			return new GroupOperation(idFields);
		}

		/**
		 * Factory method to create a new {@link ProjectionOperation} for the given {@literal fields}. The {@literal _id}
		 * field is implicitly excluded.
		 * 
		 * @param fields a list of fields to include in the projection.
		 * @return The {@link ProjectionOperation}.
		 */
		public static ProjectionOperation project(String... fields) {
			return new ProjectionOperation(fields);
		}

		/**
		 * Factory method to create a new {@link ProjectionOperation}.
		 * 
		 * @return The {@link ProjectionOperation}.
		 */
		public static ProjectionOperation project() {
			return new ProjectionOperation();
		}

		/**
		 * Factory method to create a new {@link ProjectionOperation} for the given {@literal targetClass}.
		 * 
		 * @param targetClass
		 * @return
		 */
		public static ProjectionOperation project(Class<?> targetClass) {
			return new ProjectionOperation(targetClass);
		}

		/**
		 * Factory method to create a new {@link MatchOperation} for the given {@link Criteria}.
		 * 
		 * @param criteria must not be {@literal null}
		 * @return
		 */
		public static MatchOperation match(Criteria criteria) {
			return new MatchOperation(criteria);
		}

		/**
		 * Factory method to create a new {@link UnwindOperation} for the given {@literal fieldName}.
		 * 
		 * @param fieldName {@link UnwindOperation}.
		 * @return
		 */
		public static UnwindOperation unwind(String fieldName) {
			return new UnwindOperation(fieldName);
		}

		/**
		 * Factory method to create a new {@link SkipOperation} for the given {@literal skipCount}.
		 * 
		 * @param skipCount the number of documents to skip.
		 * @return
		 */
		public static SkipOperation skip(int skipCount) {
			return new SkipOperation(skipCount);
		}

		/**
		 * Factory method to create a new {@link LimitOperation} for the given {@literal maxElements}.
		 * 
		 * @param maxElements, the max number of documents to return.
		 * @return
		 */
		public static LimitOperation limit(int maxElements) {
			return new LimitOperation(maxElements);
		}

		/**
		 * Factory method to create a new {@link GeoNearOperation} for the given {@literal nearQuery}.
		 * 
		 * @param nearQuery, must not be null.
		 * @return
		 */
		public static GeoNearOperation geoNear(NearQuery nearQuery) {
			return new GeoNearOperation(nearQuery);
		}

		/**
		 * Factory method to create a new {@link SortOperation} for the given sort {@link Direction}  {@literal direction}
		 * and {@literal fields}.
		 * 
		 * @param direction, the sort direction, must not be null.
		 * @param fields must not be null.
		 * @return
		 */
		public static SortOperation sort(Sort.Direction direction, String... fields) {
			return sort(new Sort(direction, fields));
		}

		/**
		 * Factory method to create a new {@link SortOperation} for the given {@link Sort}.
		 * 
		 * @param sort
		 * @return
		 */
		public static SortOperation sort(Sort sort) {
			return new SortOperation(sort);
		}

		/**
		 * Factory method to create a new empty {@link Fields} container for key-value pairs.
		 * 
		 * @return
		 */
		public static Fields fields() {
			return fields(new String[0]);
		}

		/**
		 * Factory method to create a new {@link Fields} container for key-value pairs from the given {@literal fieldNames}.
		 * A call to fields("a","b","c") generates:
		 * 
		 * <pre>
		 * {    
		 *   a: $a,
		 *   b: $b,
		 *   c: $c
		 * }
		 * </pre>
		 * 
		 * @return
		 */
		public static Fields fields(String... fieldNames) {
			return new BackendFields(fieldNames);
		}

		/**
		 * A convenience shortcut to {@link ReferenceUtil#$id()}
		 * 
		 * @return
		 */
		public static String $id() {
			return ReferenceUtil.$id();
		}

		/**
		 * A convenience shortcut to {@link ReferenceUtil#$(String)}
		 * 
		 * @return
		 */
		public static String $(String fieldName) {
			return ReferenceUtil.$(fieldName);
		}

		/**
		 * A convenience shortcut to {@link ReferenceUtil#$id(String)}
		 * 
		 * @return
		 */
		public static String $id(String fieldName) {
			return ReferenceUtil.$id(fieldName);
		}

		/**
		 * A convenience shortcut to {@link ReferenceUtil#ID_KEY}
		 * 
		 * @return
		 */
		public static String id() {
			return ReferenceUtil.ID_KEY;
		}

		/**
		 * A convenience shortcut to {@link ReferenceUtil#id(String)}
		 * 
		 * @return
		 */
		public static String id(String fieldName) {
			return ReferenceUtil.id(fieldName);
		}

		/**
		 * Creates a new {@link Aggregation}.
		 * 
		 * @param <I> the input type of the {@link Aggregation}.
		 * @param <O> the output type of the {@link Aggregation}.
		 * @param inputType
		 * @param operations
		 * @return
		 */
		public static <I, O> Aggregation<I, O> newAggregation(Class<I> inputType, AggregationOperation... operations) {
			return new Aggregation<I, O>(inputType, operations);
		}

		/**
		 * Creates a new {@link Aggregation}.
		 * 
		 * @param <I> the input type of the {@link Aggregation}.
		 * @param <O> the output type of the {@link Aggregation}.
		 * @param inputType
		 * @param operations
		 * @return
		 */
		public static <I, O> Aggregation<I, O> newAggregation(String inputCollectionName,
				AggregationOperation... operations) {
			return new Aggregation<I, O>(inputCollectionName, operations);
		}
	}
}
