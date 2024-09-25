/*
 * Copyright 2013-2024 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.convert;

import java.util.List;

import org.bson.Document;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import com.mongodb.DBRef;

/**
 * Used to resolve associations annotated with {@link org.springframework.data.mongodb.core.mapping.DBRef}.
 *
 * @author Thomas Darimont
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.4
 */
public interface DbRefResolver extends ReferenceResolver {

	/**
	 * Resolves the given {@link DBRef} into an object of the given {@link MongoPersistentProperty}'s type. The method
	 * might return a proxy object for the {@link DBRef} or resolve it immediately. In both cases the
	 * {@link DbRefResolverCallback} will be used to obtain the actual backing object.
	 *
	 * @param property will never be {@literal null}.
	 * @param dbref the {@link DBRef} to resolve.
	 * @param callback will never be {@literal null}.
	 * @return can be {@literal null}.
	 */
	@Nullable
	Object resolveDbRef(MongoPersistentProperty property, @Nullable DBRef dbref, DbRefResolverCallback callback,
			DbRefProxyHandler proxyHandler);

	/**
	 * Creates a {@link DBRef} instance for the given {@link org.springframework.data.mongodb.core.mapping.DBRef}
	 * annotation, {@link MongoPersistentEntity} and id.
	 *
	 * @param annotation will never be {@literal null}.
	 * @param entity will never be {@literal null}.
	 * @param id will never be {@literal null}.
	 * @return new instance of {@link DBRef}.
	 */
	default DBRef createDbRef(@Nullable org.springframework.data.mongodb.core.mapping.DBRef annotation,
			MongoPersistentEntity<?> entity, Object id) {

		if (annotation != null && StringUtils.hasText(annotation.db())) {
			return new DBRef(annotation.db(), entity.getCollection(), id);
		}

		return new DBRef(entity.getCollection(), id);
	}

	/**
	 * Actually loads the {@link DBRef} from the datasource.
	 *
	 * @param dbRef must not be {@literal null}.
	 * @return can be {@literal null}.
	 * @since 1.7
	 */
	@Nullable
	Document fetch(DBRef dbRef);

	/**
	 * Loads a given {@link List} of {@link DBRef}s from the datasource in one batch. The resulting {@link List} of
	 * {@link Document} will reflect the ordering of the {@link DBRef} passed in.<br />
	 * The {@link DBRef} elements in the list must not reference different collections.
	 *
	 * @param dbRefs must not be {@literal null}.
	 * @return never {@literal null}.
	 * @throws InvalidDataAccessApiUsageException in case not all {@link DBRef} target the same collection.
	 * @since 1.10
	 */
	List<Document> bulkFetch(List<DBRef> dbRefs);
}
