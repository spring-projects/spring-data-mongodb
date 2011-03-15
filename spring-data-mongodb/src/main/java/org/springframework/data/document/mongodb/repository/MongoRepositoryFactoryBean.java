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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.document.mongodb.MongoOperations;
import org.springframework.data.document.mongodb.MongoPropertyDescriptors.MongoPropertyDescriptor;
import org.springframework.data.document.mongodb.MongoTemplate;
import org.springframework.data.document.mongodb.query.Index;
import org.springframework.data.document.mongodb.query.Order;
import org.springframework.data.domain.Sort;
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
public class MongoRepositoryFactoryBean<T extends MongoRepository<S, ID>, S, ID extends Serializable> extends RepositoryFactoryBeanSupport<T, S, ID> {

  private MongoTemplate template;

  /**
   * Configures the {@link MongoTemplate} to be used.
   *
   * @param template the template to set
   */
  public void setTemplate(MongoTemplate template) {

    this.template = template;
  }

  /*
    * (non-Javadoc)
    *
    * @see org.springframework.data.repository.support.RepositoryFactoryBeanSupport #createRepositoryFactory()
    */
  @Override
  protected RepositoryFactorySupport createRepositoryFactory() {

    MongoRepositoryFactory factory = new MongoRepositoryFactory(template);
    factory.addQueryCreationListener(new IndexEnsuringQueryCreationListener(template));
    return factory;
  }

  /*
    * (non-Javadoc)
    *
    * @see org.springframework.data.repository.support.RepositoryFactoryBeanSupport #afterPropertiesSet()
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

    /**
     * Creates a new {@link MongoRepositoryFactory} fwith the given {@link MongoTemplate}.
     *
     * @param template
     */
    public MongoRepositoryFactory(MongoTemplate template) {

      this.template = template;
    }


    /*
       * (non-Javadoc)
       *
       * @see org.springframework.data.repository.support.RepositoryFactorySupport#getRepositoryBaseClass()
       */
    @Override
    protected Class<?> getRepositoryBaseClass(Class<?> repositoryInterface) {
      return SimpleMongoRepository.class;
    }

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

      public RepositoryQuery resolveQuery(Method method, Class<?> domainClass) {

        MongoQueryMethod queryMethod = new MongoQueryMethod(method, domainClass);

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
       * @see org.springframework.data.repository.support.RepositoryFactorySupport#validate(java.lang.Class,
       * java.lang.Object)
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


    /* (non-Javadoc)
    * @see org.springframework.data.repository.support.RepositoryFactorySupport#getEntityInformation(java.lang.Class)
    */
    @Override
    public <T, ID extends Serializable> MongoEntityInformation<T, ID> getEntityInformation(
        Class<T> domainClass) {

      return new MongoEntityInformation<T, ID>(domainClass);
    }


    /*
    * (non-Javadoc)
    * @see org.springframework.data.repository.support.RepositoryFactorySupport#getTargetRepository(org.springframework.data.repository.support.RepositoryMetadata)
    */
    @Override
    @SuppressWarnings({"unchecked", "rawtypes"})
    protected Object getTargetRepository(RepositoryMetadata metadata) {

      MongoEntityInformation<?, ?> info = getEntityInformation(
          metadata.getDomainClass());
      return new SimpleMongoRepository(info, template);
    }
  }

  /**
   * {@link QueryCreationListener} inspecting {@link PartTreeMongoQuery}s and creating an index for the properties it
   * refers to.
   *
   * @author Oliver Gierke
   */
  private static class IndexEnsuringQueryCreationListener implements QueryCreationListener<PartTreeMongoQuery> {

    private static final Logger LOG = LoggerFactory.getLogger(IndexEnsuringQueryCreationListener.class);
    private final MongoOperations operations;

    public IndexEnsuringQueryCreationListener(MongoOperations operations) {
      this.operations = operations;
    }

    /*
       * (non-Javadoc)
       *
       * @see
       * org.springframework.data.repository.support.QueryCreationListener#onCreation(org.springframework.data.repository
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
