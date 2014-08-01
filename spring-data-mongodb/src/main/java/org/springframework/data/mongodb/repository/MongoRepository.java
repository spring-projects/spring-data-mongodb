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

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.text.TextCriteria;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.PagingAndSortingRepository;

/**
 * Mongo specific {@link org.springframework.data.repository.Repository} interface.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
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
	 * Retrieve all documents matching given criteria.
	 * 
	 * @param textCriteria
	 * @return
	 * @since 1.6
	 */
	List<T> findAllBy(TextCriteria textCriteria);

	/**
	 * Retrieve sorted list of documents matching given criteria.
	 * 
	 * @param textCriteria
	 * @param sort
	 * @return
	 * @since 1.6
	 */
	List<T> findAllBy(TextCriteria textCriteria, Sort sort);

	/**
	 * Retrieve all documents matching given criteria in range of page.
	 * 
	 * @param textCriteria
	 * @param pageable
	 * @return
	 * @since 1.6
	 */
	Page<T> findBy(TextCriteria textCriteria, Pageable pageable);
}
