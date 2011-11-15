/*
 * Copyright 2002-2011 the original author or authors.
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

import static org.springframework.data.mongodb.repository.query.QueryUtils.applyPagination;

import java.util.List;

import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.CollectionCallback;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.geo.Distance;
import org.springframework.data.mongodb.core.geo.GeoResult;
import org.springframework.data.mongodb.core.geo.GeoResults;
import org.springframework.data.mongodb.core.geo.Point;
import org.springframework.data.mongodb.core.query.NearQuery;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;

import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

/**
 * Base class for {@link RepositoryQuery} implementations for Mongo.
 * 
 * @author Oliver Gierke
 */
public abstract class AbstractMongoQuery implements RepositoryQuery {

	private final MongoQueryMethod method;
	private final MongoOperations mongoOperations;

	/**
	 * Creates a new {@link AbstractMongoQuery} from the given {@link MongoQueryMethod} and {@link MongoOperations}.
	 * 
	 * @param method
	 * @param template
	 */
	public AbstractMongoQuery(MongoQueryMethod method, MongoOperations template) {

		Assert.notNull(template);
		Assert.notNull(method);

		this.method = method;
		this.mongoOperations = template;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	public MongoQueryMethod getQueryMethod() {

		return method;
	}

	/*
	  * (non-Javadoc)
	  *
	  * @see org.springframework.data.repository.query.RepositoryQuery#execute(java .lang.Object[])
	  */
	public Object execute(Object[] parameters) {

		MongoParameterAccessor accessor = new MongoParametersParameterAccessor(method, parameters);
		Query query = createQuery(new ConvertingParameterAccessor(mongoOperations.getConverter(), accessor));

		if (method.isGeoNearQuery()) {
			return new GeoNearExecution(accessor).execute(query);
		} else if (method.isCollectionQuery()) {
			return new CollectionExecution().execute(query);
		} else if (method.isPageQuery()) {
			return new PagedExecution(accessor.getPageable()).execute(query);
		} else {
			return new SingleEntityExecution().execute(query);
		}
	}

	/**
	 * Create a {@link Query} instance using the given {@link ParameterAccessor}
	 * 
	 * @param accessor
	 * @param converter
	 * @return
	 */
	protected abstract Query createQuery(ConvertingParameterAccessor accessor);

	private abstract class Execution {

		abstract Object execute(Query query);

		protected List<?> readCollection(Query query) {

			MongoEntityInformation<?, ?> metadata = method.getEntityInformation();

			String collectionName = metadata.getCollectionName();
			return mongoOperations.find(query, metadata.getJavaType(), collectionName);
		}
	}

	/**
	 * {@link Execution} for collection returning queries.
	 * 
	 * @author Oliver Gierke
	 */
	class CollectionExecution extends Execution {

		/*
		   * (non-Javadoc)
		   *
		   * @see org.springframework.data.mongodb.repository.MongoQuery.Execution #execute(com.mongodb.DBObject)
		   */
		@Override
		public Object execute(Query query) {

			return readCollection(query);
		}
	}

	/**
	 * {@link Execution} for pagination queries.
	 * 
	 * @author Oliver Gierke
	 */
	class PagedExecution extends Execution {

		private final Pageable pageable;

		/**
		 * Creates a new {@link PagedExecution}.
		 * 
		 * @param pageable
		 */
		public PagedExecution(Pageable pageable) {

			Assert.notNull(pageable);
			this.pageable = pageable;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		@SuppressWarnings({ "rawtypes", "unchecked" })
		Object execute(Query query) {

			MongoEntityInformation<?, ?> metadata = method.getEntityInformation();
			int count = getCollectionCursor(metadata.getCollectionName(), query.getQueryObject()).count();

			List<?> result = mongoOperations.find(applyPagination(query, pageable), metadata.getJavaType(),
					metadata.getCollectionName());

			return new PageImpl(result, pageable, count);
		}

		private DBCursor getCollectionCursor(String collectionName, final DBObject query) {

			return mongoOperations.execute(collectionName, new CollectionCallback<DBCursor>() {

				public DBCursor doInCollection(DBCollection collection) {

					return collection.find(query);
				}
			});
		}
	}

	/**
	 * {@link Execution} to return a single entity.
	 * 
	 * @author Oliver Gierke
	 */
	class SingleEntityExecution extends Execution {

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.core.query.Query)
		 */
		@Override
		Object execute(Query query) {

			MongoEntityInformation<?, ?> entityInformation = method.getEntityInformation();
			return mongoOperations.findOne(query, entityInformation.getJavaType());
		}
	}

	/**
	 * {@link Execution} to execute geo-near queries.
	 *
	 * @author Oliver Gierke
	 */
	class GeoNearExecution extends Execution {
		
		private final MongoParameterAccessor accessor;
		
		public GeoNearExecution(MongoParameterAccessor accessor) {
			this.accessor = accessor;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.repository.AbstractMongoQuery.Execution#execute(org.springframework.data.mongodb.core.query.Query)
		 */
		@Override
		Object execute(Query query) {
			
			Point nearLocation = accessor.getGeoNearLocation();
			NearQuery nearQuery = NearQuery.near(nearLocation);
			
			if (query != null) {
				nearQuery.query(query);
			}
			
			Distance maxDistance = accessor.getMaxDistance();
			if (maxDistance != null) {
				nearQuery.maxDistance(maxDistance);
			}
			
			MongoEntityInformation<?,?> entityInformation = method.getEntityInformation();
			GeoResults<?> results = mongoOperations.geoNear(nearQuery, entityInformation.getJavaType(), entityInformation.getCollectionName());
			
			return isListOfGeoResult() ? results.getContent() : results;
		}
		
		private boolean isListOfGeoResult() {
			
			TypeInformation<?> returnType = method.getReturnType();
			return returnType.getType().equals(List.class) && GeoResult.class.equals(returnType.getComponentType());
		}
	}
}
