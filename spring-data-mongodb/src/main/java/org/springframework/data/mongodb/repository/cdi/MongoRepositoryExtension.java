/*
 * Copyright 2011-2014 the original author or authors.
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
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import javax.enterprise.event.Observes;
import javax.enterprise.inject.UnsatisfiedResolutionException;
import javax.enterprise.inject.spi.AfterBeanDiscovery;
import javax.enterprise.inject.spi.Bean;
import javax.enterprise.inject.spi.BeanManager;
import javax.enterprise.inject.spi.ProcessBean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.repository.cdi.CdiRepositoryBean;
import org.springframework.data.repository.cdi.CdiRepositoryExtensionSupport;

/**
 * CDI extension to export Mongo repositories.
 * 
 * @author Oliver Gierke
 * @author Mark Paluch
 */
public class MongoRepositoryExtension extends CdiRepositoryExtensionSupport {

	private static final Logger LOG = LoggerFactory.getLogger(MongoRepositoryExtension.class);

	private final Map<Set<Annotation>, Bean<MongoOperations>> mongoOperations = new HashMap<Set<Annotation>, Bean<MongoOperations>>();

	public MongoRepositoryExtension() {
		LOG.info("Activating CDI extension for Spring Data MongoDB repositories.");
	}

	@SuppressWarnings("unchecked")
	<X> void processBean(@Observes ProcessBean<X> processBean) {

		Bean<X> bean = processBean.getBean();

		for (Type type : bean.getTypes()) {
			if (type instanceof Class<?> && MongoOperations.class.isAssignableFrom((Class<?>) type)) {
				if (LOG.isDebugEnabled()) {
					LOG.debug(String.format("Discovered %s with qualifiers %s.", MongoOperations.class.getName(),
							bean.getQualifiers()));
				}

				// Store the EntityManager bean using its qualifiers.
				mongoOperations.put(new HashSet<Annotation>(bean.getQualifiers()), (Bean<MongoOperations>) bean);
			}
		}
	}

	void afterBeanDiscovery(@Observes AfterBeanDiscovery afterBeanDiscovery, BeanManager beanManager) {

		for (Entry<Class<?>, Set<Annotation>> entry : getRepositoryTypes()) {

			Class<?> repositoryType = entry.getKey();
			Set<Annotation> qualifiers = entry.getValue();

			// Create the bean representing the repository.
			CdiRepositoryBean<?> repositoryBean = createRepositoryBean(repositoryType, qualifiers, beanManager);

			if (LOG.isInfoEnabled()) {
				LOG.info(String.format("Registering bean for %s with qualifiers %s.", repositoryType.getName(), qualifiers));
			}

			// Register the bean to the container.
			registerBean(repositoryBean);
			afterBeanDiscovery.addBean(repositoryBean);
		}
	}

	/**
	 * Creates a {@link CdiRepositoryBean} for the repository of the given type.
	 * 
	 * @param <T> the type of the repository.
	 * @param repositoryType the class representing the repository.
	 * @param qualifiers the qualifiers to be applied to the bean.
	 * @param beanManager the BeanManager instance.
	 * @return
	 */
	private <T> CdiRepositoryBean<T> createRepositoryBean(Class<T> repositoryType, Set<Annotation> qualifiers,
			BeanManager beanManager) {

		// Determine the MongoOperations bean which matches the qualifiers of the repository.
		Bean<MongoOperations> mongoOperations = this.mongoOperations.get(qualifiers);

		if (mongoOperations == null) {
			throw new UnsatisfiedResolutionException(String.format("Unable to resolve a bean for '%s' with qualifiers %s.",
					MongoOperations.class.getName(), qualifiers));
		}

		// Construct and return the repository bean.
		return new MongoRepositoryBean<T>(mongoOperations, qualifiers, repositoryType, beanManager,
				Optional.ofNullable(getCustomImplementationDetector()));
	}
}
