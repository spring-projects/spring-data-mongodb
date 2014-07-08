/*
 * Copyright 2011-2012 the original author or authors.
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

import java.util.regex.Pattern;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.util.Assert;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mysema.query.mongodb.MongodbSerializer;
import com.mysema.query.types.Path;
import com.mysema.query.types.PathMetadata;
import com.mysema.query.types.PathType;

/**
 * Custom {@link MongodbSerializer} to take mapping information into account when building keys for constraints.
 * 
 * @author Oliver Gierke
 */
class SpringDataMongodbSerializer extends MongodbSerializer {

	private final MongoConverter converter;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final QueryMapper mapper;

	/**
	 * Creates a new {@link SpringDataMongodbSerializer} for the given {@link MappingContext}.
	 * 
	 * @param mappingContext
	 */
	public SpringDataMongodbSerializer(MongoConverter converter) {

		Assert.notNull(converter, "MongoConverter must not be null!");

		this.mappingContext = converter.getMappingContext();
		this.converter = converter;
		this.mapper = new QueryMapper(converter);
	}

	/*
	 * (non-Javadoc)
	 * @see com.mysema.query.mongodb.MongodbSerializer#getKeyForPath(com.mysema.query.types.Path, com.mysema.query.types.PathMetadata)
	 */
	@Override
	protected String getKeyForPath(Path<?> expr, PathMetadata<?> metadata) {

		if (!metadata.getPathType().equals(PathType.PROPERTY)) {
			return super.getKeyForPath(expr, metadata);
		}

		Path<?> parent = metadata.getParent();
		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(parent.getType());
		MongoPersistentProperty property = entity.getPersistentProperty(metadata.getName());

		return property == null ? super.getKeyForPath(expr, metadata) : property.getFieldName();
	}

	/*
	 * (non-Javadoc)
	 * @see com.mysema.query.mongodb.MongodbSerializer#asDBObject(java.lang.String, java.lang.Object)
	 */
	@Override
	protected DBObject asDBObject(String key, Object value) {

		if ("_id".equals(key)) {
			return super.asDBObject(key, potentiallyConvertNestedValuesToObjectId(value));
		}

		return super.asDBObject(key, value instanceof Pattern ? value : converter.convertToMongoType(value));
	}

	protected Object potentiallyConvertNestedValuesToObjectId(Object source) {

		if (source instanceof DBObject) {
			return potentiallyConvertNestedValuesToObjectId((DBObject) source);
		}
		return mapper.convertId(source);
	}

	@SuppressWarnings("rawtypes")
	protected DBObject potentiallyConvertNestedValuesToObjectId(DBObject source) {

		DBObject target = new BasicDBObject();

		for (String key : source.keySet()) {

			Object value = source.get(key);

			if (key.startsWith("$")) {

				Object convertedValue = null;
				if (value instanceof Iterable) {

					BasicDBList dbList = new BasicDBList();
					for (Object o : (Iterable) value) {
						dbList.add(potentiallyConvertNestedValuesToObjectId(o));
					}
					convertedValue = dbList;
				} else {
					convertedValue = mapper.convertId(value);
				}
				target.put(key, convertedValue);
			} else {
				target.put(key, mapper.convertId(value));
			}
		}

		return target;

	}
}
