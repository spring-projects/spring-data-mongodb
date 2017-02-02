/*
 * Copyright 2012-2017 the original author or authors.
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
package org.springframework.data.mongodb.repository.cdi;

import java.lang.annotation.Annotation;
import java.util.Optional;
import java.util.Set;

import javax.enterprise.context.spi.CreationalContext;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;

import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.repository.support.MongoRepositoryFactory;
import org.springframework.data.repository.cdi.CdiRepositoryBean;
import org.springframework.data.repository.config.CustomRepositoryImplementationDetector;
import org.springframework.util.Assert;

/**
 * {@link CdiRepositoryBean} to create Mongo repository instances.
 * 
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class MongoRepositoryBean<T> extends CdiRepositoryBean<T> {

	private final Bean<MongoOperations> operations;

	/**
	 * Creates a new {@link MongoRepositoryBean}.
	 * 
	 * @param operations must not be {@literal null}.
	 * @param qualifiers must not be {@literal null}.
	 * @param repositoryType must not be {@literal null}.
	 * @param beanManager must not be {@literal null}.
	 * @param detector detector for the custom {@link org.springframework.data.repository.Repository} implementations
	 *          {@link CustomRepositoryImplementationDetector}, can be {@literal null}.
	 */
	public MongoRepositoryBean(Bean<MongoOperations> operations, Set<Annotation> qualifiers, Class<T> repositoryType,
			BeanManager beanManager, Optional<CustomRepositoryImplementationDetector> detector) {

		super(qualifiers, repositoryType, beanManager, detector);

		Assert.notNull(operations, "MongoOperations bean must not be null!");
		this.operations = operations;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.cdi.CdiRepositoryBean#create(javax.enterprise.context.spi.CreationalContext, java.lang.Class)
	 */
	@Override
	protected T create(CreationalContext<T> creationalContext, Class<T> repositoryType, Optional<Object> customImplementation) {

		MongoOperations mongoOperations = getDependencyInstance(operations, MongoOperations.class);
		MongoRepositoryFactory factory = new MongoRepositoryFactory(mongoOperations);

		return customImplementation.isPresent() ? factory.getRepository(repositoryType, customImplementation.get()) : factory.getRepository(repositoryType);
	}
}
