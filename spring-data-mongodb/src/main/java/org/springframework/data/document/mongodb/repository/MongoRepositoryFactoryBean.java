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
package org.springframework.data.document.mongodb.repository;

import java.io.Serializable;
import java.lang.reflect.Method;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.document.mongodb.MongoOperations;
import org.springframework.data.document.mongodb.MongoPropertyDescriptors.MongoPropertyDescriptor;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.mapping.MongoPersistentEntity;
import org.springframework.data.document.mongodb.query.Index;
import org.springframework.data.document.mongodb.query.Order;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.model.MappingContext;
import org.springframework.data.mapping.model.PersistentEntity;
import org.springframework.data.repository.query.QueryLookupStrategy;
import org.springframework.data.repository.query.QueryLookupStrategy.Key;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.data.repository.support.QueryCreationListener;
import org.springframework.data.repository.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.support.RepositoryFactorySupport;
import org.springframework.data.repository.support.RepositoryMetadata;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

/**
 * {@link org.springframework.beans.factory.FactoryBean} to create {@link MongoRepository} instances.
 *
 * @author Oliver Gierke
 */
public class MongoRepositoryFactoryBean<T extends MongoRepository<S, ID>, S, ID extends Serializable> extends
    RepositoryFactoryBeanSupport<T, S, ID> {

  private MongoTemplate template;
  private MappingContext mappingContext;

  /**
   * Configures the {@link MongoTemplate} to be used.
   *
   * @param template the template to set
   */
  public void setTemplate(MongoTemplate template) {

    this.template = template;
  }

  /**
   * Sets the {@link MappingContext} used with the underlying {@link MongoTemplate}.
   *
   * @param mappingContext the mappingContext to set
   */
  public void setMappingContext(MappingContext mappingContext) {
    this.mappingContext = mappingContext;
  }

  /*
   * (non-Javadoc)
   * 
   * @see
   * org.springframework.data.repository.support.RepositoryFactoryBeanSupport
   * #createRepositoryFactory()
   */
  @Override
  protected RepositoryFactorySupport createRepositoryFactory() {

    MongoRepositoryFactory factory = new MongoRepositoryFactory(template, mappingContext);
    factory.addQueryCreationListener(new IndexEnsuringQueryCreationListener(template));
    return factory;
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

    private static final boolean QUERY_DSL_PRESENT = org.springframework.util.ClassUtils.isPresent(
        "com.mysema.query.types.Predicate", MongoRepositoryFactory.class.getClassLoader());

    private final MongoTemplate template;
    private final EntityInformationCreator entityInformationCreator;

    /**
     * Creates a new {@link MongoRepositoryFactory} with the given {@link MongoTemplate} and {@link MappingContext}.
     *
     * @param template       must not be {@literal null}
     * @param mappingContext
     */
    public MongoRepositoryFactory(MongoTemplate template, MappingContext mappingContext) {

      Assert.notNull(template);
      this.template = template;
      this.entityInformationCreator = new EntityInformationCreator(mappingContext);
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.support.RepositoryFactorySupport
     * #getRepositoryBaseClass()
     */
    @Override
    protected Class<?> getRepositoryBaseClass(Class<?> repositoryInterface) {

      return isQueryDslRepository(repositoryInterface) ? QueryDslMongoRepository.class : SimpleMongoRepository.class;
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
    @SuppressWarnings({"rawtypes", "unchecked"})
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
      public RepositoryQuery resolveQuery(Method method, Class<?> domainClass) {

        MongoQueryMethod queryMethod = new MongoQueryMethod(method, entityInformationCreator);

        if (queryMethod.hasAnnotatedQuery()) {
          return new StringBasedMongoQuery(queryMethod, template);
        } else {
          return new PartTreeMongoQuery(queryMethod, template);
        }
      }
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.data.repository.support.RepositoryFactorySupport
     * #validate(java.lang.Class, java.lang.Object)
     */
    @Override
    protected void validate(RepositoryMetadata metadata, Object customImplementation) {

      Class<?> idClass = metadata.getIdClass();
      if (!MongoPropertyDescriptor.SUPPORTED_ID_CLASSES.contains(idClass)) {
        throw new IllegalArgumentException(String.format("Unsupported id class! Only %s are supported!",
            StringUtils.collectionToCommaDelimitedString(MongoPropertyDescriptor.SUPPORTED_ID_CLASSES)));
      }

      super.validate(metadata, customImplementation);
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

    private final MappingContext mappingContext;

    public EntityInformationCreator(MappingContext mappingContext) {
      this.mappingContext = mappingContext;
    }

    public <T, ID extends Serializable> MongoEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {
      if (null == mappingContext) {
        return new SimpleMongoEntityInformation<T, ID>(domainClass);
      }

      PersistentEntity<T> persistentEntity = mappingContext.getPersistentEntity(domainClass);
      if (persistentEntity == null) {
        persistentEntity = mappingContext.addPersistentEntity(domainClass);
      }
      return new MappingMongoEntityInformation<T, ID>((MongoPersistentEntity<T>) persistentEntity);
    }
  }

  /**
   * {@link QueryCreationListener} inspecting {@link PartTreeMongoQuery}s and creating an index for the properties it
   * refers to.
   *
   * @author Oliver Gierke
   */
  private static class IndexEnsuringQueryCreationListener implements QueryCreationListener<PartTreeMongoQuery> {

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
        String property = part.getProperty().toDotPath();
        Order order = toOrder(sort, property);
        index.on(property, order);
      }

      MongoEntityInformation<?, ?> metadata = query.getQueryMethod().getEntityInformation();
      operations.ensureIndex(metadata.getCollectionName(), index);
      LOG.debug(String.format("Created index %s!", index.toString()));
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
