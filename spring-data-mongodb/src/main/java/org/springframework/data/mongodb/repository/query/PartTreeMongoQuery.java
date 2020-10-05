/*
 * Copyright 2002-2020 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import org.bson.Document;
import org.bson.json.JsonParseException;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.Field;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.QueryMethodEvaluationContextProvider;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.ResultProcessor;
import org.springframework.data.repository.query.ReturnedType;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.expression.ExpressionParser;
import org.springframework.util.StringUtils;

/**
 * {@link RepositoryQuery} implementation for Mongo.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 * @author Mark Paluch
 */
public class PartTreeMongoQuery extends AbstractMongoQuery {

	private final PartTree tree;
	private final boolean isGeoNearQuery;
	private final MappingContext<?, MongoPersistentProperty> context;
	private final ResultProcessor processor;

	/**
	 * Creates a new {@link PartTreeMongoQuery} from the given {@link QueryMethod} and {@link MongoTemplate}.
	 *
	 * @param method must not be {@literal null}.
	 * @param mongoOperations must not be {@literal null}.
	 * @param expressionParser must not be {@literal null}.
	 * @param evaluationContextProvider must not be {@literal null}.
	 */
	public PartTreeMongoQuery(MongoQueryMethod method, MongoOperations mongoOperations,
			ExpressionParser expressionParser, QueryMethodEvaluationContextProvider evaluationContextProvider) {

		super(method, mongoOperations, expressionParser, evaluationContextProvider);

		this.processor = method.getResultProcessor();
		this.tree = new PartTree(method.getName(), processor.getReturnedType().getDomainType());
		this.isGeoNearQuery = method.isGeoNearQuery();
		this.context = mongoOperations.getConverter().getMappingContext();
	}

	/**
	 * Return the {@link PartTree} backing the query.
	 *
	 * @return the tree
	 */
	public PartTree getTree() {
		return tree;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#createQuery(org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor, boolean)
	 */
	@Override
	protected Query createQuery(ConvertingParameterAccessor accessor) {

		MongoQueryCreator creator = new MongoQueryCreator(tree, accessor, context, isGeoNearQuery);
		Query query = creator.createQuery();

		if (tree.isLimiting()) {
			query.limit(tree.getMaxResults());
		}

		TextCriteria textCriteria = accessor.getFullText();
		if (textCriteria != null) {
			query.addCriteria(textCriteria);
		}

		String fieldSpec = this.getQueryMethod().getFieldSpecification();

		if (!StringUtils.hasText(fieldSpec)) {

			ReturnedType returnedType = processor.withDynamicProjection(accessor).getReturnedType();

			if (returnedType.needsCustomConstruction()) {

				Field fields = query.fields();

				for (String field : returnedType.getInputProperties()) {
					fields.include(field);
				}
			}

			return query;
		}

		try {

			BasicQuery result = new BasicQuery(query.getQueryObject(), Document.parse(fieldSpec));
			result.setSortObject(query.getSortObject());

			return result;

		} catch (JsonParseException o_O) {
			throw new IllegalStateException(String.format("Invalid query or field specification in %s!", getQueryMethod()),
					o_O);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#createCountQuery(org.springframework.data.mongodb.repository.query.ConvertingParameterAccessor)
	 */
	@Override
	protected Query createCountQuery(ConvertingParameterAccessor accessor) {
		return new MongoQueryCreator(tree, accessor, context, false).createQuery();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isCountQuery()
	 */
	@Override
	protected boolean isCountQuery() {
		return tree.isCountProjection();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isExistsQuery()
	 */
	@Override
	protected boolean isExistsQuery() {
		return tree.isExistsProjection();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isDeleteQuery()
	 */
	@Override
	protected boolean isDeleteQuery() {
		return tree.isDelete();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isLimiting()
	 */
	@Override
	protected boolean isLimiting() {
		return tree.isLimiting();
	}
}
