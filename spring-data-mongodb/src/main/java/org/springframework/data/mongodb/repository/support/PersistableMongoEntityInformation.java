/*
 * Copyright 2017 the original author or authors.
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

import lombok.NonNull;
import lombok.RequiredArgsConstructor;

import java.util.Optional;

import org.springframework.data.domain.Persistable;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;

/**
 * {@link MongoEntityInformation} implementation wrapping an existing {@link MongoEntityInformation} considering
 * {@link Persistable} types by delegating {@link #isNew(Object)} and {@link #getId(Object)} to the corresponding
 * {@link Persistable#isNew()} and {@link Persistable#getId()} implementations.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.10
 */
@RequiredArgsConstructor
class PersistableMongoEntityInformation<T, ID> implements MongoEntityInformation<T, ID> {

	private final @NonNull MongoEntityInformation<T, ID> delegate;

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoEntityInformation#getCollectionName()
	 */
	@Override
	public String getCollectionName() {
		return delegate.getCollectionName();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoEntityInformation#getIdAttribute()
	 */
	@Override
	public String getIdAttribute() {
		return delegate.getIdAttribute();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.EntityInformation#isNew(java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public boolean isNew(T t) {

		if (t instanceof Persistable) {
			return ((Persistable<ID>) t).isNew();
		}

		return delegate.isNew(t);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.EntityInformation#getId(java.lang.Object)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public Optional<ID> getId(T t) {

		if (t instanceof Persistable) {
			return Optional.ofNullable(((Persistable<ID>) t).getId());
		}

		return delegate.getId(t);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.PersistentEntityInformation#getIdType()
	 */
	@Override
	public Class<ID> getIdType() {
		return delegate.getIdType();
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.EntityMetadata#getJavaType()
	 */
	@Override
	public Class<T> getJavaType() {
		return delegate.getJavaType();
	}
}
