/*
 * Copyright 2010-2011 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import java.io.Serializable;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.beans.factory.FactoryBean} to create {@link MongoRepository} instances.
 * 
 * @author Oliver Gierke
 * @author Jordi Llach
 */
public class MongoRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable> 
extends      RepositoryFactoryBeanSupport<T, S, ID> 
implements   ApplicationContextAware {

	private MongoOperations operations;
	private boolean createIndexesForQueryMethods = false;
	private boolean mappingContextConfigured = false;
    private ApplicationEventPublisher eventPublisher;

	/**
	 * Configures the {@link MongoOperations} to be used.
	 * 
	 * @param operations the operations to set
	 */
	public void setMongoOperations(MongoOperations operations) {
		this.operations = operations;
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
	 * @see org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport#setMappingContext(org.springframework.data.mapping.context.MappingContext)
	 */
	@Override
	protected void setMappingContext(MappingContext<?, ?> mappingContext) {

		super.setMappingContext(mappingContext);
		this.mappingContextConfigured = true;
	}
    
    /*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	@Override
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.eventPublisher = applicationContext;
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

		RepositoryFactorySupport factory = getFactoryInstance(operations);

		if (createIndexesForQueryMethods) {
			factory.addQueryCreationListener(new IndexEnsuringQueryCreationListener(operations));
		}

		return factory;
	}

	/**
	 * Creates and initializes a {@link RepositoryFactorySupport} instance.
	 * 
	 * @param operations
	 * @return
	 */
	protected RepositoryFactorySupport getFactoryInstance(MongoOperations operations) {
		return new MongoRepositoryFactory(operations, eventPublisher);
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
		Assert.notNull(operations,     "MongoTemplate must not be null!");
        Assert.notNull(eventPublisher, "ApplicationContext must not be null");

		if (!mappingContextConfigured) {
			setMappingContext(operations.getConverter().getMappingContext());
		}
	}
}