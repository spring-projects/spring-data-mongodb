/*
 * Copyright 2010-2016 the original author or authors.
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

import org.springframework.data.domain.Example;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
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
	 * Inserts the given a given entity. Assumes the instance to be new to be able to apply insertion optimizations. Use
	 * the returned instance for further operations as the save operation might have changed the entity instance
	 * completely. Prefer using {@link #save(Object)} instead to avoid the usage of store-specific API.
	 * 
	 * @param entity must not be {@literal null}.
	 * @return the saved entity
	 * @since 1.7
	 */
	<S extends T> S insert(S entity);

	/**
	 * Inserts the given entities. Assumes the given entities to have not been persisted yet and thus will optimize the
	 * insert over a call to {@link #save(Iterable)}. Prefer using {@link #save(Iterable)} to avoid the usage of store
	 * specific API.
	 * 
	 * @param entities must not be {@literal null}.
	 * @return the saved entities
	 * @since 1.7
	 */
	<S extends T> List<S> insert(Iterable<S> entities);

	/**
	 * Returns all instances of the type specified by the given {@link Example}.
	 * 
	 * @param example must not be {@literal null}.
	 * @return
	 * @since 1.8
	 */
	<S extends T> List<T> findAllByExample(Example<S> example);

	/**
	 * Returns all instances of the type specified by the given {@link Example}.
	 * 
	 * @param example must not be {@literal null}.
	 * @param sort can be {@literal null}.
	 * @return all entities sorted by the given options
	 * @since 1.8
	 */
	<S extends T> List<T> findAllByExample(Example<S> example, Sort sort);

	/**
	 * Returns a {@link Page} of entities meeting the paging restriction specified by the given {@link Example} limited to
	 * criteria provided in the {@code Pageable} object.
	 * 
	 * @param example must not be {@literal null}.
	 * @param pageable can be {@literal null}.
	 * @return a {@link Page} of entities
	 * @since 1.8
	 */
	<S extends T> Page<T> findAllByExample(Example<S> example, Pageable pageable);
}
