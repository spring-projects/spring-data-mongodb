/*
 * Copyright 2010-2014 the original author or authors.
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

import java.io.Serializable;
import java.util.List;

import org.springframework.data.domain.Sort;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * Mongo specific {@link org.springframework.data.repository.Repository} interface.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Thomas Darimont
 */
@NoRepositoryBean
public interface MongoRepository<T, ID extends Serializable> extends PagingAndSortingRepository<T, ID> {

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#save(java.lang.Iterable)
	 */
	<S extends T> List<S> save(Iterable<S> entites);

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.CrudRepository#findAll()
	 */
	List<T> findAll();

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.PagingAndSortingRepository#findAll(org.springframework.data.domain.Sort)
	 */
	List<T> findAll(Sort sort);

	/**
	 * Saves a given entity. Use the returned instance for further operations as the save operation might have changed the
	 * entity instance completely.
	 * <p>
	 * This uses {@link org.springframework.data.mongodb.core.MongoTemplate#insert(Object)} for storing the given entity.
	 * <p>
	 * Note that this method does neither fire any save events nor performs any id population or version checking.
	 * 
	 * @param entity
	 * @return the saved entity
	 */
	<S extends T> S insert(S entity);

	/**
	 * Saves all given entities.
	 * <p>
	 * This uses {@link org.springframework.data.mongodb.core.MongoTemplate#insert(Object)} for storing the given entity.
	 * <p>
	 * Note that this method does neither fire any save events nor nor performs any id population or version checking.
	 * 
	 * @param entities
	 * @return the saved entities
	 * @throws IllegalArgumentException in case the given entity is (@literal null}.
	 */
	<S extends T> List<S> insert(Iterable<S> entities);
}
