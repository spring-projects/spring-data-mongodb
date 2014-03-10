/*
 * Copyright 2002-2014 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.PartTree;

/**
 * {@link RepositoryQuery} implementation for Mongo.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class PartTreeMongoQuery extends AbstractMongoQuery {

	private final PartTree tree;
	private final boolean isGeoNearQuery;
	private final MappingContext<?, MongoPersistentProperty> context;

	/**
	 * Creates a new {@link PartTreeMongoQuery} from the given {@link QueryMethod} and {@link MongoTemplate}.
	 * 
	 * @param method must not be {@literal null}.
	 * @param template must not be {@literal null}.
	 */
	public PartTreeMongoQuery(MongoQueryMethod method, MongoOperations mongoOperations) {

		super(method, mongoOperations);
		this.tree = new PartTree(method.getName(), method.getEntityInformation().getJavaType());
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
		return creator.createQuery();
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
	 * @see org.springframework.data.mongodb.repository.query.AbstractMongoQuery#isDeleteQuery()
	 */
	@Override
	protected boolean isDeleteQuery() {
		return tree.isDelete();
	}
}
