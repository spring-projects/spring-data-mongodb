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
import java.util.List;

import org.bson.Document;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mongodb.core.query.CriteriaDefinition;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

/**
 * Encapsulates the aggregation framework {@code $cond} operator. A {@link ConditionalOperator} allows nested conditions
 * {@code if-then[if-then-else]-else} using {@link Field}, {@link CriteriaDefinition} or a {@link DBObject custom}
 * condition. Replacement values can be either {@link Field field references}, values of simple MongoDB types or values
 * that can be converted to a simple MongoDB type.
 * 
 * @see http://docs.mongodb.com/manual/reference/operator/aggregation/cond/
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 1.10
 */
public class ConditionalOperator implements AggregationExpression {

	private final Object condition;
	private final Object thenValue;
	private final Object otherwiseValue;

	/**
	 * Creates a new {@link ConditionalOperator} for a given {@link Field} and {@code then}/{@code otherwise} values.
	 *
	 * @param condition must not be {@literal null}.
	 * @param thenValue must not be {@literal null}.
	 * @param otherwiseValue must not be {@literal null}.
	 */
	public ConditionalOperator(Field condition, Object thenValue, Object otherwiseValue) {
		this((Object) condition, thenValue, otherwiseValue);
	}

	/**
	 * Creates a new {@link ConditionalOperator} for a given {@link CriteriaDefinition} and {@code then}/{@code otherwise}
	 * values.
	 *
	 * @param condition must not be {@literal null}.
	 * @param thenValue must not be {@literal null}.
	 * @param otherwiseValue must not be {@literal null}.
	 */
	public ConditionalOperator(CriteriaDefinition condition, Object thenValue, Object otherwiseValue) {
		this((Object) condition, thenValue, otherwiseValue);
	}

	/**
	 * Creates a new {@link ConditionalOperator} for a given {@link DBObject criteria} and {@code then}/{@code otherwise}
	 * values.
	 *
	 * @param condition must not be {@literal null}.
	 * @param thenValue must not be {@literal null}.
	 * @param otherwiseValue must not be {@literal null}.
	 */
	public ConditionalOperator(Document condition, Object thenValue, Object otherwiseValue) {
		this((Object) condition, thenValue, otherwiseValue);
	}

	private ConditionalOperator(Object condition, Object thenValue, Object otherwiseValue) {

		Assert.notNull(condition, "Condition must not be null!");
		Assert.notNull(thenValue, "'Then value' must not be null!");
		Assert.notNull(otherwiseValue, "'Otherwise value' must not be null!");

		assertNotBuilder(condition, "Condition");
		assertNotBuilder(thenValue, "'Then value'");
		assertNotBuilder(otherwiseValue, "'Otherwise value'");

		this.condition = condition;
		this.thenValue = thenValue;
		this.otherwiseValue = otherwiseValue;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.aggregation.AggregationExpression#toDbObject(org.springframework.data.mongodb.core.aggregation.AggregationOperationContext)
	 */
	@Override
	public Document toDocument(AggregationOperationContext context) {

		Document condObject = new Document();

		condObject.append("if", resolveCriteria(context, condition));
		condObject.append("then", resolveValue(context, thenValue));
		condObject.append("else", resolveValue(context, otherwiseValue));

		return new Document("$cond", condObject);
	}

	private Object resolveValue(AggregationOperationContext context, Object value) {

		if (value instanceof DBObject || value instanceof Field) {
			return resolve(context, value);
		}

		if (value instanceof ConditionalOperator) {
			return ((ConditionalOperator) value).toDocument(context);
		}

		return context.getMappedObject(new Document("$set", value)).get("$set");
	}

	private Object resolveCriteria(AggregationOperationContext context, Object value) {

		if (value instanceof DBObject || value instanceof Field) {
			return resolve(context, value);
		}

		if (value instanceof CriteriaDefinition) {

			Document mappedObject = context.getMappedObject(((CriteriaDefinition) value).getCriteriaObject());
			List<Object> clauses = new ArrayList<Object>();

			clauses.addAll(getClauses(context, mappedObject));

			if (clauses.size() == 1) {
				return clauses.get(0);
			}

			return clauses;
		}

		throw new InvalidDataAccessApiUsageException(
				String.format("Invalid value in condition. Supported: DBObject, Field references, Criteria, got: %s", value));
	}

	private List<Object> getClauses(AggregationOperationContext context, Document mappedObject) {

		List<Object> clauses = new ArrayList<Object>();

		for (String key : mappedObject.keySet()) {

			Object predicate = mappedObject.get(key);
			clauses.addAll(getClauses(context, key, predicate));
		}

		return clauses;
	}

	private List<Object> getClauses(AggregationOperationContext context, String key, Object predicate) {

		List<Object> clauses = new ArrayList<Object>();

		if (predicate instanceof List) {

			List<Object> args = new ArrayList<Object>();
			for (Object clause : (List<?>) predicate) {
				if (clause instanceof Document) {
					args.addAll(getClauses(context, (Document) clause));
				}
			}

			clauses.add(new BasicDBObject(key, args));

		} else if (predicate instanceof Document) {

			Document nested = (Document) predicate;

			for (String s : nested.keySet()) {

				if (!isKeyword(s)) {
					continue;
				}

				List<Object> args = new ArrayList<Object>();
				args.add("$" + key);
				args.add(nested.get(s));
				clauses.add(new BasicDBObject(s, args));
			}

		} else if (!isKeyword(key)) {

			List<Object> args = new ArrayList<Object>();
			args.add("$" + key);
			args.add(predicate);
			clauses.add(new BasicDBObject("$eq", args));
		}

		return clauses;
	}

	/**
	 * Returns whether the given {@link String} is a MongoDB keyword.
	 *
	 * @param candidate
	 * @return
	 */
	private boolean isKeyword(String candidate) {
		return candidate.startsWith("$");
	}

	private Object resolve(AggregationOperationContext context, Object value) {

		if (value instanceof Document) {
			return context.getMappedObject((Document) value);
		}

		return context.getReference((Field) value).toString();
	}

	private void assertNotBuilder(Object toCheck, String name) {
		Assert.isTrue(!ClassUtils.isAssignableValue(ConditionalExpressionBuilder.class, toCheck),
				String.format("%s must not be of type %s", name, ConditionalExpressionBuilder.class.getSimpleName()));
	}

	/**
	 * Get a builder that allows fluent creation of {@link ConditionalOperator}.
	 *
	 * @return a new {@link ConditionalExpressionBuilder}.
	 */
	public static ConditionalExpressionBuilder newBuilder() {
		return ConditionalExpressionBuilder.newBuilder();
	}

	/**
	 * @since 1.10
	 */
	public static interface WhenBuilder {

		/**
		 * @param booleanExpression expression that yields in a boolean result, must not be {@literal null}.
		 * @return the {@link ThenBuilder}
		 */
		ThenBuilder when(Document booleanExpression);

		/**
		 * @param booleanField reference to a field holding a boolean value, must not be {@literal null}.
		 * @return the {@link ThenBuilder}
		 */
		ThenBuilder when(Field booleanField);

		/**
		 * @param booleanField name of a field holding a boolean value, must not be {@literal null}.
		 * @return the {@link ThenBuilder}
		 */
		ThenBuilder when(String booleanField);

		/**
		 * @param criteria criteria to evaluate, must not be {@literal null}.
		 * @return the {@link ThenBuilder}
		 */
		ThenBuilder when(CriteriaDefinition criteria);
	}

	/**
	 * @since 1.10
	 */
	public static interface ThenBuilder {

		/**
		 * @param value the value to be used if the condition evaluates {@literal true}. Can be a {@link DBObject}, a value
		 *          that is supported by MongoDB or a value that can be converted to a MongoDB representation but must not
		 *          be {@literal null}.
		 * @return the {@link OtherwiseBuilder}
		 */
		OtherwiseBuilder then(Object value);
	}

	/**
	 * @since 1.10
	 */
	public static interface OtherwiseBuilder {

		/**
		 * @param value the value to be used if the condition evaluates {@literal false}. Can be a {@link DBObject}, a value
		 *          that is supported by MongoDB or a value that can be converted to a MongoDB representation but must not
		 *          be {@literal null}.
		 * @return the {@link ConditionalOperator}
		 */
		ConditionalOperator otherwise(Object value);
	}

	/**
	 * Builder for fluent {@link ConditionalOperator} creation.
	 *
	 * @author Mark Paluch
	 * @since 1.10
	 */
	public static final class ConditionalExpressionBuilder implements WhenBuilder, ThenBuilder, OtherwiseBuilder {

		private Object condition;
		private Object thenValue;

		private ConditionalExpressionBuilder() {}

		/**
		 * Creates a new builder for {@link ConditionalOperator}.
		 *
		 * @return never {@literal null}.
		 */
		public static ConditionalExpressionBuilder newBuilder() {
			return new ConditionalExpressionBuilder();
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ConditionalOperator.WhenBuilder#when(com.mongodb.DBObject)
		 */
		@Override
		public ConditionalExpressionBuilder when(Document booleanExpression) {

			Assert.notNull(booleanExpression, "'Boolean expression' must not be null!");

			this.condition = booleanExpression;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ConditionalOperator.WhenBuilder#when(org.springframework.data.mongodb.core.query.CriteriaDefinition)
		 */
		@Override
		public ThenBuilder when(CriteriaDefinition criteria) {

			Assert.notNull(criteria, "Criteria must not be null!");

			this.condition = criteria;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ConditionalOperator.WhenBuilder#when(org.springframework.data.mongodb.core.aggregation.Field)
		 */
		@Override
		public ThenBuilder when(Field booleanField) {

			Assert.notNull(booleanField, "Boolean field must not be null!");

			this.condition = booleanField;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ConditionalOperator.WhenBuilder#when(java.lang.String)
		 */
		@Override
		public ThenBuilder when(String booleanField) {

			Assert.hasText(booleanField, "Boolean field name must not be null or empty!");

			this.condition = Fields.field(booleanField);
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ConditionalOperator.ThenBuilder#then(java.lang.Object)
		 */
		@Override
		public OtherwiseBuilder then(Object thenValue) {

			Assert.notNull(thenValue, "'Then-value' must not be null!");

			this.thenValue = thenValue;
			return this;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.aggregation.ConditionalOperator.OtherwiseBuilder#otherwise(java.lang.Object)
		 */
		@Override
		public ConditionalOperator otherwise(Object otherwiseValue) {

			Assert.notNull(otherwiseValue, "'Otherwise-value' must not be null!");

			return new ConditionalOperator(condition, thenValue, otherwiseValue);
		}
	}
}
