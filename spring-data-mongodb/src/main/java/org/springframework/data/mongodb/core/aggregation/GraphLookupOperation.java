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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.data.mongodb.core.aggregation.ExposedFields.ExposedField;
import org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation.InheritsFieldsAggregationOperation;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $graphLookup}-operation. <br />
 * Performs a recursive search on a collection, with options for restricting the search by recursion depth and query
 * filter. <br />
 * We recommend to use the static factory method {@link Aggregation#graphLookup(String)} instead of creating instances
 * of this class directly.
 *
 * @see <a href=
 *      "https://docs.mongodb.org/manual/reference/aggregation/graphLookup/">https://docs.mongodb.org/manual/reference/aggregation/graphLookup/</a>
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 1.10
 */
public class GraphLookupOperation implements InheritsFieldsAggregationOperation {

	private static final Set<Class<?>> ALLOWED_START_TYPES = new HashSet<Class<?>>(
			Arrays.<Class<?>> asList(AggregationExpression.class, String.class, Field.class, DBObject.class));

	private final String from;
	private final List<Object> startWith;
	private final Field connectFrom;
	private final Field connectTo;
	private final Field as;
	private final Long maxDepth;
	private final Field depthField;
	private final CriteriaDefinition restrictSearchWithMatch;

	private GraphLookupOperation(String from, List<Object> startWith, Field connectFrom, Field connectTo, Field as,
			Long maxDepth, Field depthField, CriteriaDefinition restrictSearchWithMatch) {

		this.from = from;
		this.startWith = startWith;
		this.connectFrom = connectFrom;
		this.connectTo = connectTo;
		this.as = as;
		this.maxDepth = maxDepth;
		this.depthField = depthField;
		this.restrictSearchWithMatch = restrictSearchWithMatch;
	}

	/**
	 * Creates a new {@link FromBuilder} to build {@link GraphLookupOperation}.
	 *
	 * @return a new {@link FromBuilder}.
	 */
	public static FromBuilder builder() {
		return new GraphLookupOperationFromBuilder();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationOperation#toDBObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public DBObject toDBObject(AggregationOperationContext context) {

		DBObject graphLookup = new BasicDBObject();

		graphLookup.put("from", from);

		List<Object> mappedStartWith = new ArrayList<Object>(startWith.size());

		for (Object startWithElement : startWith) {

			if (startWithElement instanceof AggregationExpression) {
				mappedStartWith.add(((AggregationExpression) startWithElement).toDbObject(context));
			} else if (startWithElement instanceof Field) {
				mappedStartWith.add(context.getReference((Field) startWithElement).toString());
			} else {
				mappedStartWith.add(startWithElement);
			}
		}

		graphLookup.put("startWith", mappedStartWith.size() == 1 ? mappedStartWith.iterator().next() : mappedStartWith);

		graphLookup.put("connectFromField", connectFrom.getName());
		graphLookup.put("connectToField", connectTo.getName());
		graphLookup.put("as", as.getName());

		if (maxDepth != null) {
			graphLookup.put("maxDepth", maxDepth);
		}

		if (depthField != null) {
			graphLookup.put("depthField", depthField.getName());
		}

		if (restrictSearchWithMatch != null) {
			graphLookup.put("restrictSearchWithMatch", context.getMappedObject(restrictSearchWithMatch.getCriteriaObject()));
		}

		return new BasicDBObject("$graphLookup", graphLookup);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.FieldsExposingAggregationOperation#getFields()
	 */
	@Override
	public ExposedFields getFields() {
		return ExposedFields.from(new ExposedField(as, true));
	}

	/**
	 * @author Mark Paluch
	 */
	public interface FromBuilder {

		/**
		 * Set the {@literal collectionName} to apply the {@code $graphLookup} to.
		 *
		 * @param collectionName must not be {@literal null} or empty.
		 * @return
		 */
		StartWithBuilder from(String collectionName);
	}

	/**
	 * @author Mark Paluch
	 * @author Christoph Strobl
	 */
	public interface StartWithBuilder {

		/**
		 * Set the startWith {@literal fieldReferences} to apply the {@code $graphLookup} to.
		 *
		 * @param fieldReferences must not be {@literal null}.
		 * @return
		 */
		ConnectFromBuilder startWith(String... fieldReferences);

		/**
		 * Set the startWith {@literal expressions} to apply the {@code $graphLookup} to.
		 *
		 * @param expressions must not be {@literal null}.
		 * @return
		 */
		ConnectFromBuilder startWith(AggregationExpression... expressions);

		/**
		 * Set the startWith as either {@literal fieldReferences}, {@link Fields}, {@link DBObject} or
		 * {@link AggregationExpression} to apply the {@code $graphLookup} to.
		 *
		 * @param expressions must not be {@literal null}.
		 * @return
		 * @throws IllegalArgumentException
		 */
		ConnectFromBuilder startWith(Object... expressions);
	}

	/**
	 * @author Mark Paluch
	 */
	public interface ConnectFromBuilder {

		/**
		 * Set the connectFrom {@literal fieldName} to apply the {@code $graphLookup} to.
		 *
		 * @param fieldName must not be {@literal null} or empty.
		 * @return
		 */
		ConnectToBuilder connectFrom(String fieldName);
	}

	/**
	 * @author Mark Paluch
	 */
	public interface ConnectToBuilder {

		/**
		 * Set the connectTo {@literal fieldName} to apply the {@code $graphLookup} to.
		 *
		 * @param fieldName must not be {@literal null} or empty.
		 * @return
		 */
		GraphLookupOperationBuilder connectTo(String fieldName);
	}

	/**
	 * Builder to build the initial {@link GraphLookupOperationBuilder} that configures the initial mandatory set of
	 * {@link GraphLookupOperation} properties.
	 *
	 * @author Mark Paluch
	 */
	static final class GraphLookupOperationFromBuilder
			implements FromBuilder, StartWithBuilder, ConnectFromBuilder, ConnectToBuilder {

		private String from;
		private List<? extends Object> startWith;
		private String connectFrom;

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.GraphLookupOperation.FromBuilder#from(java.lang.String)
		 */
		@Override
		public StartWithBuilder from(String collectionName) {

			Assert.hasText(collectionName, "CollectionName must not be null or empty!");

			this.from = collectionName;
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.GraphLookupOperation.StartWithBuilder#startWith(java.lang.String[])
		 */
		@Override
		public ConnectFromBuilder startWith(String... fieldReferences) {

			Assert.notNull(fieldReferences, "FieldReferences must not be null!");
			Assert.noNullElements(fieldReferences, "FieldReferences must not contain null elements!");

			List<Object> fields = new ArrayList<Object>(fieldReferences.length);

			for (String fieldReference : fieldReferences) {
				fields.add(Fields.field(fieldReference));
			}

			this.startWith = fields;
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.GraphLookupOperation.StartWithBuilder#startWith(org.springframework.data.mongodb.core.aggregation.AggregationExpression[])
		 */
		@Override
		public ConnectFromBuilder startWith(AggregationExpression... expressions) {

			Assert.notNull(expressions, "AggregationExpressions must not be null!");
			Assert.noNullElements(expressions, "AggregationExpressions must not contain null elements!");

			this.startWith = Arrays.asList(expressions);
			return this;
		}

		@Override
		public ConnectFromBuilder startWith(Object... expressions) {

			Assert.notNull(expressions, "Expressions must not be null!");
			Assert.noNullElements(expressions, "Expressions must not contain null elements!");

			this.startWith = verifyAndPotentiallyTransformStartsWithTypes(expressions);
			return this;
		}

		private List<Object> verifyAndPotentiallyTransformStartsWithTypes(Object... expressions) {

			List<Object> expressionsToUse = new ArrayList<Object>(expressions.length);

			for (Object expression : expressions) {

				assertStartWithType(expression);

				if (expression instanceof String) {
					expressionsToUse.add(Fields.field((String) expression));
				} else {
					expressionsToUse.add(expression);
				}

			}
			return expressionsToUse;
		}

		private void assertStartWithType(Object expression) {

			for (Class<?> type : ALLOWED_START_TYPES) {

				if (ClassUtils.isAssignable(type, expression.getClass())) {
					return;
				}
			}

			throw new IllegalArgumentException(
					String.format("Expression must be any of %s but was %s", ALLOWED_START_TYPES, expression.getClass()));
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.GraphLookupOperation.ConnectFromBuilder#connectFrom(java.lang.String)
		 */
		@Override
		public ConnectToBuilder connectFrom(String fieldName) {

			Assert.hasText(fieldName, "ConnectFrom must not be null or empty!");

			this.connectFrom = fieldName;
			return this;
		}

		/* (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.GraphLookupOperation.ConnectToBuilder#connectTo(java.lang.String)
		 */
		@Override
		public GraphLookupOperationBuilder connectTo(String fieldName) {

			Assert.hasText(fieldName, "ConnectTo must not be null or empty!");

			return new GraphLookupOperationBuilder(from, startWith, connectFrom, fieldName);
		}
	}

	/**
	 * @author Mark Paluch
	 */
	static final class GraphLookupOperationBuilder {

		private final String from;
		private final List<Object> startWith;
		private final Field connectFrom;
		private final Field connectTo;
		private Long maxDepth;
		private Field depthField;
		private CriteriaDefinition restrictSearchWithMatch;

		protected GraphLookupOperationBuilder(String from, List<? extends Object> startWith, String connectFrom,
				String connectTo) {

			this.from = from;
			this.startWith = new ArrayList<Object>(startWith);
			this.connectFrom = Fields.field(connectFrom);
			this.connectTo = Fields.field(connectTo);
		}

		/**
		 * Optionally limit the number of recursions.
		 *
		 * @param numberOfRecursions must be greater or equal to zero.
		 * @return
		 */
		public GraphLookupOperationBuilder maxDepth(long numberOfRecursions) {

			Assert.isTrue(numberOfRecursions >= 0, "Max depth must be >= 0!");

			this.maxDepth = numberOfRecursions;
			return this;
		}

		/**
		 * Optionally add a depth field {@literal fieldName} to each traversed document in the search path.
		 *
		 * @param fieldName must not be {@literal null} or empty.
		 * @return
		 */
		public GraphLookupOperationBuilder depthField(String fieldName) {

			Assert.hasText(fieldName, "Depth field name must not be null or empty!");

			this.depthField = Fields.field(fieldName);
			return this;
		}

		/**
		 * Optionally add a query specifying conditions to the recursive search.
		 *
		 * @param criteriaDefinition must not be {@literal null}.
		 * @return
		 */
		public GraphLookupOperationBuilder restrict(CriteriaDefinition criteriaDefinition) {

			Assert.notNull(criteriaDefinition, "CriteriaDefinition must not be null!");

			this.restrictSearchWithMatch = criteriaDefinition;
			return this;
		}

		/**
		 * Set the name of the array field added to each output document and return the final {@link GraphLookupOperation}.
		 * Contains the documents traversed in the {@literal $graphLookup} stage to reach the document.
		 *
		 * @param fieldName must not be {@literal null} or empty.
		 * @return the final {@link GraphLookupOperation}.
		 */
		public GraphLookupOperation as(String fieldName) {

			Assert.hasText(fieldName, "As field name must not be null or empty!");

			return new GraphLookupOperation(from, startWith, connectFrom, connectTo, Fields.field(fieldName), maxDepth,
					depthField, restrictSearchWithMatch);
		}
	}
}
