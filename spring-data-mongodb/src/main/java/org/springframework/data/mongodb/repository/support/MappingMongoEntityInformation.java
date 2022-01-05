/*
 * Copyright 2011-2021 the original author or authors.
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
package org.springframework.data.mongodb.repository.support;

import org.bson.types.ObjectId;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.repository.query.MongoEntityInformation;
import org.springframework.data.repository.core.support.PersistentEntityInformation;
import org.springframework.lang.Nullable;

/**
 * {@link MongoEntityInformation} implementation using a {@link MongoPersistentEntity} instance to lookup the necessary
 * information. Can be configured with a custom collection to be returned which will trump the one returned by the
 * {@link MongoPersistentEntity} if given.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class MappingMongoEntityInformation<T, ID> extends PersistentEntityInformation<T, ID>
		implements MongoEntityInformation<T, ID> {

	private final MongoPersistentEntity<T> entityMetadata;
	private final @Nullable String customCollectionName;
	private final Class<ID> fallbackIdType;

	/**
	 * Creates a new {@link MappingMongoEntityInformation} for the given {@link MongoPersistentEntity}.
	 *
	 * @param entity must not be {@literal null}.
	 */
	public MappingMongoEntityInformation(MongoPersistentEntity<T> entity) {
		this(entity, null, null);
	}

	/**
	 * Creates a new {@link MappingMongoEntityInformation} for the given {@link MongoPersistentEntity} and fallback
	 * identifier type.
	 *
	 * @param entity must not be {@literal null}.
	 * @param fallbackIdType can be {@literal null}.
	 */
	public MappingMongoEntityInformation(MongoPersistentEntity<T> entity, @Nullable Class<ID> fallbackIdType) {
		this(entity, null, fallbackIdType);
	}

	/**
	 * Creates a new {@link MappingMongoEntityInformation} for the given {@link MongoPersistentEntity} and custom
	 * collection name.
	 *
	 * @param entity must not be {@literal null}.
	 * @param customCollectionName can be {@literal null}.
	 */
	public MappingMongoEntityInformation(MongoPersistentEntity<T> entity, String customCollectionName) {
		this(entity, customCollectionName, null);
	}

	/**
	 * Creates a new {@link MappingMongoEntityInformation} for the given {@link MongoPersistentEntity}, collection name
	 * and identifier type.
	 *
	 * @param entity must not be {@literal null}.
	 * @param customCollectionName can be {@literal null}.
	 * @param idType can be {@literal null}.
	 */
	@SuppressWarnings("unchecked")
	private MappingMongoEntityInformation(MongoPersistentEntity<T> entity, @Nullable String customCollectionName,
			@Nullable Class<ID> idType) {

		super(entity);

		this.entityMetadata = entity;
		this.customCollectionName = customCollectionName;
		this.fallbackIdType = idType != null ? idType : (Class<ID>) ObjectId.class;
	}

	public String getCollectionName() {
		return customCollectionName == null ? entityMetadata.getCollection() : customCollectionName;
	}

	public String getIdAttribute() {
		return entityMetadata.getRequiredIdProperty().getName();
	}

	@Override
	public Class<ID> getIdType() {

		if (this.entityMetadata.hasIdProperty()) {
			return super.getIdType();
		}

		return fallbackIdType;
	}

	@Override
	public boolean isVersioned() {
		return this.entityMetadata.hasVersionProperty();
	}

	@Override
	public Object getVersion(T entity) {

		if (!isVersioned()) {
			return null;
		}

		PersistentPropertyAccessor<T> accessor = this.entityMetadata.getPropertyAccessor(entity);

		return accessor.getProperty(this.entityMetadata.getRequiredVersionProperty());
	}

	@Nullable
	public Collation getCollation() {
		return this.entityMetadata.getCollation();
	}

}
