/*
 * Copyright 2011-2014 the original author or authors.
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

import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.annotation.Reference;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.util.Assert;

import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.mysema.query.mongodb.MongodbSerializer;
import com.mysema.query.types.Constant;
import com.mysema.query.types.Operation;
import com.mysema.query.types.Path;
import com.mysema.query.types.PathMetadata;
import com.mysema.query.types.PathType;

/**
 * Custom {@link MongodbSerializer} to take mapping information into account when building keys for constraints.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
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
			return super.asDBObject(key, mapper.convertId(value));
		}

		return super.asDBObject(key, value instanceof Pattern ? value : converter.convertToMongoType(value));
	}

	@Override
	protected boolean isReference(Path<?> arg) {
		return AnnotationUtils.getAnnotation(arg.getAnnotatedElement(), Reference.class) != null;
	}

	/*
	 * (non-Javadoc)
	 * @see com.mysema.query.mongodb.MongodbSerializer#asReference(java.lang.Object)
	 */
	@Override
	protected DBRef asReference(Object constant) {
		return converter.toDBRef(constant, null);
	}

	/*
	 * (non-Javadoc)
	 * @see com.mysema.query.mongodb.MongodbSerializer#asReference(com.mysema.query.types.Operation, int)
	 */
	@Override
	protected DBRef asReference(Operation<?> expr, int constIndex) {

		for (Object arg : expr.getArgs()) {
			if (arg instanceof Path) {
				return convertToDBRef(((Constant<?>) expr.getArg(constIndex)).getConstant(), (Path<?>) arg);
			}
		}

		return super.asReference(expr, constIndex);
	}

	private DBRef convertToDBRef(Object source, Path<?> path) {

		MongoPersistentProperty property = null;
		if (path.getMetadata().getParent() != null) {
			MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(path.getMetadata().getParent().getType());
			property = entity != null ? entity.getPersistentProperty(path.getMetadata().getName()) : null;
		}

		return converter.toDBRef(source, property);
	}
}
