/*
 * Copyright 2013 the original author or authors.
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
package org.springframework.data.mongodb.core.convert;

import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import com.mongodb.DBRef;

/**
 * Used to resolve associations annotated with {@link org.springframework.data.mongodb.core.mapping.DBRef}.
 * 
 * @author Thomas Darimont
 */
public interface DbRefResolver {

	/**
	 * @param property will never be {@literal null}.
	 * @param dbref the {@link DBRef} to resolve.
	 * @param callback will never be {@literal null}.
	 * @return
	 */
	Object resolveDbRef(MongoPersistentProperty property, DBRef dbref, DbRefResolverCallback callback);

	/**
	 * Creates a {@link DBRef} instance for the given {@link org.springframework.data.mongodb.core.mapping.DBRef}
	 * annotation, {@link MongoPersistentEntity} and id.
	 * 
	 * @param annotation will never be {@literal null}.
	 * @param entity will never be {@literal null}.
	 * @param id will never be {@literal null}.
	 * @return
	 */
	DBRef createDbRef(org.springframework.data.mongodb.core.mapping.DBRef annotation, MongoPersistentEntity<?> entity,
			Object id);
}
