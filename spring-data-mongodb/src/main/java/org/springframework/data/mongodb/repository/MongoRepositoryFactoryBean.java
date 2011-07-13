/*
 * Copyright 2002-2010 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import static org.springframework.data.querydsl.QueryDslUtils.*;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.convert.MongoPropertyDescriptors.MongoPropertyDescriptor;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.core.query.Order;
import org.springframework.data.querydsl.QueryDslPredicateExecutor;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.NamedQueries;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.QueryCreationListener;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.Part.Type;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * {@link org.springframework.beans.factory.FactoryBean} to create {@link MongoRepository} instances.
 * 
 * @author Oliver Gierke
 */
public class MongoRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable> extends
		RepositoryFactoryBeanSupport<T, S, ID> {

	private MongoTemplate template;
	private boolean createIndexesForQueryMethods = false;

	/**
	 * Configures the {@link MongoTemplate} to be used.
	 * 
	 * @param template
	 *          the template to set
	 */
	public void setTemplate(MongoTemplate template) {

		this.template = template;
	}
	
	/**
	 * Configures whether to automatically create indexes for the properties referenced in a query method.
	 * 
	 * @param createIndexesForQueryMethods the createIndexesForQueryMethods to set
	 */
	public void setCreateIndexesForQueryMethods(boolean createIndexesForQueryMethods) {
		this.createIndexesForQueryMethods = createIndexesForQueryMethods;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.repository.support.RepositoryFactoryBeanSupport
	 * #createRepositoryFactory()
	 */
	@Override
	protected final RepositoryFactorySupport createRepositoryFactory() {

		RepositoryFactorySupport factory = getFactoryInstance(template);
		
		if (createIndexesForQueryMethods) {
			factory.addQueryCreationListener(new IndexEnsuringQueryCreationListener(template));
		}

		return factory;
	}
	
	/**
	 * Creates and initializes a {@link RepositoryFactorySupport} instance.
	 * 
	 * @param template
	 * @return
	 */
	protected RepositoryFactorySupport getFactoryInstance(MongoTemplate template) {
		return new MongoRepositoryFactory(template);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.repository.support.RepositoryFactoryBeanSupport
	 * #afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {

		super.afterPropertiesSet();
		Assert.notNull(template, "MongoTemplate must not be null!");
	}

	/**
	 * Repository to create {@link MongoRepository} instances.
	 * 
	 * @author Oliver Gierke
	 */
	public static class MongoRepositoryFactory extends RepositoryFactorySupport {

		private final MongoTemplate template;
		private final EntityInformationCreator entityInformationCreator;

		/**
		 * Creates a new {@link MongoRepositoryFactory} with the given {@link MongoTemplate} and {@link MappingContext}.
		 * 
		 * @param template
		 *          must not be {@literal null}
		 * @param mappingContext
		 */
		public MongoRepositoryFactory(MongoTemplate template) {

			Assert.notNull(template);
			this.template = template;
			this.entityInformationCreator = new EntityInformationCreator(template.getConverter()
					.getMappingContext());
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.springframework.data.repository.support.RepositoryFactorySupport
		 * #getRepositoryBaseClass()
		 */
		@Override
		protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {

			return isQueryDslRepository(metadata.getRepositoryInterface()) ? QueryDslMongoRepository.class
					: SimpleMongoRepository.class;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.springframework.data.repository.support.RepositoryFactorySupport
		 * #getTargetRepository
		 * (org.springframework.data.repository.support.RepositoryMetadata)
		 */
		@Override
		@SuppressWarnings({ "rawtypes", "unchecked" })
		protected Object getTargetRepository(RepositoryMetadata metadata) {

			Class<?> repositoryInterface = metadata.getRepositoryInterface();
			MongoEntityInformation<?, Serializable> entityInformation = getEntityInformation(metadata.getDomainClass());

			if (isQueryDslRepository(repositoryInterface)) {
				return new QueryDslMongoRepository(entityInformation, template);
			} else {
				return new SimpleMongoRepository(entityInformation, template);
			}
		}

		private static boolean isQueryDslRepository(Class<?> repositoryInterface) {

			return QUERY_DSL_PRESENT && QueryDslPredicateExecutor.class.isAssignableFrom(repositoryInterface);
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.springframework.data.repository.support.RepositoryFactorySupport
		 * #getQueryLookupStrategy
		 * (org.springframework.data.repository.query.QueryLookupStrategy.Key)
		 */
		@Override
		protected QueryLookupStrategy getQueryLookupStrategy(Key key) {

			return new MongoQueryLookupStrategy();
		}

		/**
		 * {@link QueryLookupStrategy} to create {@link PartTreeMongoQuery} instances.
		 * 
		 * @author Oliver Gierke
		 */
		private class MongoQueryLookupStrategy implements QueryLookupStrategy {

			/*
			 * (non-Javadoc)
			 * 
			 * @see
			 * org.springframework.data.repository.query.QueryLookupStrategy
			 * #resolveQuery(java.lang.reflect.Method, java.lang.Class)
			 */
			public RepositoryQuery resolveQuery(Method method, RepositoryMetadata metadata, NamedQueries namedQueries) {

				MongoQueryMethod queryMethod = new MongoQueryMethod(method, metadata, entityInformationCreator);
				String namedQueryName = queryMethod.getNamedQueryName();
				
				if (namedQueries.hasQuery(namedQueryName)) {
					String namedQuery = namedQueries.getQuery(namedQueryName);
					return new StringBasedMongoQuery(namedQuery, queryMethod, template);
				} else if (queryMethod.hasAnnotatedQuery()) {
					return new StringBasedMongoQuery(queryMethod, template);
				} else {
					return new PartTreeMongoQuery(queryMethod, template);
				}
			}
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.support.RepositoryFactorySupport#validate(org.springframework.data.repository.support.RepositoryMetadata)
		 */
		@Override
		protected void validate(RepositoryMetadata metadata) {

			Class<?> idClass = metadata.getIdClass();
			if (!MongoPropertyDescriptor.SUPPORTED_ID_CLASSES.contains(idClass)) {
				throw new IllegalArgumentException(String.format("Unsupported id class! Only %s are supported!",
						StringUtils.collectionToCommaDelimitedString(MongoPropertyDescriptor.SUPPORTED_ID_CLASSES)));
			}
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.springframework.data.repository.support.RepositoryFactorySupport
		 * #getEntityInformation(java.lang.Class)
		 */
		@Override
		public <T, ID extends Serializable> MongoEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {

			return entityInformationCreator.getEntityInformation(domainClass);
		}
	}

	/**
	 * Simple wrapper to to create {@link MongoEntityInformation} instances based on a {@link MappingContext}.
	 * 
	 * @author Oliver Gierke
	 */
	static class EntityInformationCreator {

		private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

		public EntityInformationCreator(MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {
			Assert.notNull(mappingContext);
			this.mappingContext = mappingContext;
		}

		@SuppressWarnings("unchecked")
		public <T, ID extends Serializable> MongoEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
			MongoPersistentEntity<T> persistentEntity = (MongoPersistentEntity<T>) mappingContext
					.getPersistentEntity(domainClass);
			return new MappingMongoEntityInformation<T, ID>(persistentEntity);
		}
	}

	/**
	 * {@link QueryCreationListener} inspecting {@link PartTreeMongoQuery}s and creating an index for the properties it
	 * refers to.
	 * 
	 * @author Oliver Gierke
	 */
	static class IndexEnsuringQueryCreationListener implements QueryCreationListener<PartTreeMongoQuery> {

		private static final Set<Type> GEOSPATIAL_TYPES = new HashSet<Part.Type>(Arrays.asList(Type.NEAR, Type.WITHIN));
		private static final Log LOG = LogFactory.getLog(IndexEnsuringQueryCreationListener.class);
		private final MongoOperations operations;

		public IndexEnsuringQueryCreationListener(MongoOperations operations) {

			this.operations = operations;
		}

		/*
		 * (non-Javadoc)
		 * 
		 * @see
		 * org.springframework.data.repository.support.QueryCreationListener
		 * #onCreation(org.springframework.data.repository
		 * .query.RepositoryQuery)
		 */
		public void onCreation(PartTreeMongoQuery query) {

			PartTree tree = query.getTree();
			Index index = new Index();
			index.named(query.getQueryMethod().getName());
			Sort sort = tree.getSort();

			for (Part part : tree.getParts()) {
				if (GEOSPATIAL_TYPES.contains(part.getType())) {
					return;
				}
				String property = part.getProperty().toDotPath();
				Order order = toOrder(sort, property);
				index.on(property, order);
			}

			MongoEntityInformation<?, ?> metadata = query.getQueryMethod().getEntityInformation();
			operations.ensureIndex(index, metadata.getCollectionName());
			LOG.debug(String.format("Created %s!", index));
		}

		private static Order toOrder(Sort sort, String property) {

			if (sort == null) {
				return Order.DESCENDING;
			}

			org.springframework.data.domain.Sort.Order order = sort.getOrderFor(property);
			return order == null ? Order.DESCENDING : order.isAscending() ? Order.ASCENDING : Order.DESCENDING;
		}
	}
}
