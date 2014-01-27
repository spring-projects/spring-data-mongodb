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

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

/**
 * A subclass of {@link QueryMapper} that retains type information on the mongo types.
 * 
 * @author Thomas Darimont
 */
public class UpdateMapper extends QueryMapper {

	private final MongoWriter<?> converter;

	/**
	 * Creates a new {@link UpdateMapper} using the given {@link MongoConverter}.
	 * 
	 * @param converter must not be {@literal null}.
	 */
	public UpdateMapper(MongoConverter converter) {

		super(converter);
		this.converter = converter;
	}

	/**
	 * Converts the given source object to a mongo type retaining the original type information of the source type on the
	 * mongo type.
	 * 
	 * @see org.springframework.data.mongodb.core.convert.QueryMapper#delegateConvertToMongoType(java.lang.Object,
	 *      org.springframework.data.mongodb.core.mapping.MongoPersistentEntity)
	 */
	@Override
	protected Object delegateConvertToMongoType(Object source, MongoPersistentEntity<?> entity) {
		return entity == null ? super.delegateConvertToMongoType(source, null) : converter.convertToMongoType(source,
				entity.getTypeInformation());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.QueryMapper#createPropertyField(org.springframework.data.mongodb.core.mapping.MongoPersistentEntity, java.lang.String, org.springframework.data.mapping.context.MappingContext)
	 */
	@Override
	protected Field createPropertyField(MongoPersistentEntity<?> entity, String key,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		if (entity == null) {
			return super.createPropertyField(entity, key, mappingContext);
		}

		return new UpdateMetadataBackedField(entity, key, mappingContext);
	}

	/**
	 * Extension of {@link MetadataBackedField} to be backed with mapping metadata.
	 * 
	 * @author Thomas Darimont
	 */
	private static class UpdateMetadataBackedField extends MetadataBackedField {

		/**
		 * Holds the {@link PersistentPropertyPath} build from the given field key without collection wildcard elements.
		 * E.g.
		 * 
		 * <pre>
		 * root.list.$.value -> root.list.value
		 * 
		 * <pre>
		 */
		private final PersistentPropertyPath<MongoPersistentProperty> linearStrippedCollectionElementPath;

		/**
		 * Creates a new {@link MetadataBackedField} with the given name, {@link MongoPersistentEntity} and
		 * {@link MappingContext}.
		 * 
		 * @param entity
		 * @param key
		 * @param mappingContext
		 */
		public UpdateMetadataBackedField(MongoPersistentEntity<?> entity, String key,
				MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

			super(key, entity, mappingContext);
			this.linearStrippedCollectionElementPath = getPath(key, true);
		}

		/* 
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.core.convert.QueryMapper.MetadataBackedField#getProperty()
		 */
		@Override
		public MongoPersistentProperty getProperty() {
			return this.linearStrippedCollectionElementPath == null ? null : this.linearStrippedCollectionElementPath
					.getLeafProperty();
		}
	}
}
