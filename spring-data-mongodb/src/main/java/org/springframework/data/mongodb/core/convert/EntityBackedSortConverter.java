/*
 * Copyright 2014 the original author or authors.
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

import org.springframework.data.domain.Sort.Order;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;

import com.mongodb.DBObject;

/**
 * {@link EntityBackedSortConverter} is a {@link SortConverter} using {@link MongoPersistentEntity} type information to
 * resolve names to be used in mongodb readable {@link DBObject}.
 * 
 * @author Christoph Strobl
 * @since 1.4.2
 */
public class EntityBackedSortConverter extends SortConverter {

	private final MongoPersistentEntity<?> entity;

	public EntityBackedSortConverter(MongoPersistentEntity<?> entity) {
		this.entity = entity;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.core.convert.SortConverter#resolvePropertyName(org.springframework.data.domain.Sort.Order)
	 */
	@Override
	protected String resolvePropertyName(Order order) {

		String propertyName = super.resolvePropertyName(order);
		if (entity != null) {
			MongoPersistentProperty persistentProperty = entity.getPersistentProperty(order.getProperty());
			if (persistentProperty != null) {
				if (persistentProperty.isIdProperty()) {
					propertyName = "_id";
				} else {
					propertyName = persistentProperty.getFieldName();
				}
			}
		}

		return propertyName;
	}
}
