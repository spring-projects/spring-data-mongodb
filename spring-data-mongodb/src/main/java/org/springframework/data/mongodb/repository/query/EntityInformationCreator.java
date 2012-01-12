/*
 * Copyright 2011 the original author or authors.
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

import java.io.Serializable;

/**
 * Interface for components being able to provide {@link EntityInformationCreator} for a given {@link Class}.
 * 
 * @author Oliver Gierke
 */
public interface EntityInformationCreator {

	/**
	 * Returns a {@link MongoEntityInformation} for the given domain class.
	 * 
	 * @param domainClass the domain class to create the {@link MongoEntityInformation} for, must not be {@literal null}.
	 * @return
	 */
	<T, ID extends Serializable> MongoEntityInformation<T, ID> getEntityInformation(Class<T> domainClass);

	/**
	 * Returns a {@link MongoEntityInformation} for the given domain class and class to retrieve the collection to query
	 * against from.
	 * 
	 * @param domainClass the domain class to create the {@link MongoEntityInformation} for, must not be {@literal null}.
	 * @param collectionClass the class to derive the collection from queries to retrieve the domain classes from shall be
	 *          ran against, must not be {@literal null}.
	 * @return
	 */
	<T, ID extends Serializable> MongoEntityInformation<T, ID> getEntityInformation(Class<T> domainClass,
			Class<?> collectionClass);

}