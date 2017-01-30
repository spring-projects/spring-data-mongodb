/*
 * Copyright 2011-2016 the original author or authors.
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

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import org.bson.Document;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.DBRef;
import com.querydsl.core.types.Constant;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Operation;
import com.mongodb.util.JSON;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.PathMetadata;
import com.querydsl.core.types.PathType;
import com.querydsl.mongodb.MongodbSerializer;

/**
 * Custom {@link MongodbSerializer} to take mapping information into account when building keys for constraints.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class SpringDataMongodbSerializer extends MongodbSerializer {

	private static final String ID_KEY = "_id";
	private static final Set<PathType> PATH_TYPES;

	static {

		Set<PathType> pathTypes = new HashSet<PathType>();
		pathTypes.add(PathType.VARIABLE);
		pathTypes.add(PathType.PROPERTY);

		PATH_TYPES = Collections.unmodifiableSet(pathTypes);
	}

	private final MongoConverter converter;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final QueryMapper mapper;

	/**
	 * Creates a new {@link SpringDataMongodbSerializer} for the given {@link MappingContext}.
	 * 
	 * @param mappingContext must not be {@literal null}.
	 */
	public SpringDataMongodbSerializer(MongoConverter converter) {

		Assert.notNull(converter, "MongoConverter must not be null!");

		this.mappingContext = converter.getMappingContext();
		this.converter = converter;
		this.mapper = new QueryMapper(converter);
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.mongodb.MongodbSerializer#visit(com.querydsl.core.types.Constant, java.lang.Void)
	 */
	@Override
	public Object visit(Constant<?> expr, Void context) {

		if (!ClassUtils.isAssignable(Enum.class, expr.getType())) {
			return super.visit(expr, context);
		}

		return converter.convertToMongoType(expr.getConstant());
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.mongodb.MongodbSerializer#getKeyForPath(com.querydsl.core.types.Path, com.querydsl.core.types.PathMetadata)
	 */
	@Override
	protected String getKeyForPath(Path<?> expr, PathMetadata metadata) {

		if (!metadata.getPathType().equals(PathType.PROPERTY)) {
			return super.getKeyForPath(expr, metadata);
		}

		Path<?> parent = metadata.getParent();
		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(parent.getType());
		Optional<MongoPersistentProperty> property = entity.getPersistentProperty(metadata.getName());

		return !property.isPresent() ? super.getKeyForPath(expr, metadata) : property.get().getFieldName();
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.mongodb.MongodbSerializer#asDocument(java.lang.String, java.lang.Object)
	 */
	@Override
	protected DBObject asDBObject(String key, Object value) {

		if (ID_KEY.equals(key)) {
			DBObject superIdValue = super.asDBObject(key, value);
			Document mappedIdValue = mapper.getMappedObject((BasicDBObject) superIdValue, Optional.empty());
			return (DBObject) JSON.parse(mappedIdValue.toJson());
		}
		return super.asDBObject(key, value instanceof Pattern ? value : converter.convertToMongoType(value));
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.mongodb.MongodbSerializer#isReference(com.querydsl.core.types.Path)
	 */
	@Override
	protected boolean isReference(Path<?> path) {

		MongoPersistentProperty property = getPropertyForPotentialDbRef(path);
		return property == null ? false : property.isAssociation();
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.mongodb.MongodbSerializer#asReference(java.lang.Object)
	 */
	@Override
	protected DBRef asReference(Object constant) {
		return asReference(constant, null);
	}

	protected DBRef asReference(Object constant, Path<?> path) {
		return converter.toDBRef(constant, getPropertyForPotentialDbRef(path));
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.mongodb.MongodbSerializer#asDBKey(com.querydsl.core.types.Operation, int)
	 */
	@Override
	protected String asDBKey(Operation<?> expr, int index) {

		Expression<?> arg = expr.getArg(index);
		String key = super.asDBKey(expr, index);

		if (!(arg instanceof Path)) {
			return key;
		}

		Path<?> path = (Path<?>) arg;

		if (!isReference(path)) {
			return key;
		}

		MongoPersistentProperty property = getPropertyFor(path);

		return property.isIdProperty() ? key.replaceAll("." + ID_KEY + "$", "") : key;
	}

	/*
	 * (non-Javadoc)
	 * @see com.querydsl.mongodb.MongodbSerializer#convert(com.querydsl.core.types.Path, com.querydsl.core.types.Constant)
	 */
	protected Object convert(Path<?> path, Constant<?> constant) {

		if (!isReference(path)) {
			return super.convert(path, constant);
		}

		MongoPersistentProperty property = getPropertyFor(path);

		return property.isIdProperty() ? asReference(constant.getConstant(), path.getMetadata().getParent())
				: asReference(constant.getConstant(), path);
	}

	private MongoPersistentProperty getPropertyFor(Path<?> path) {

		Path<?> parent = path.getMetadata().getParent();

		if (parent == null || !PATH_TYPES.contains(path.getMetadata().getPathType())) {
			return null;
		}

		Optional<? extends MongoPersistentEntity<?>> entity = mappingContext.getPersistentEntity(parent.getType());
		return entity.isPresent() ? entity.get().getRequiredPersistentProperty(path.getMetadata().getName()) : null;
	}

	/**
	 * Checks the given {@literal path} for referencing the {@literal id} property of a {@link DBRef} referenced object.
	 * If so it returns the referenced {@link MongoPersistentProperty} of the {@link DBRef} instead of the {@literal id}
	 * property.
	 *
	 * @param path
	 * @return
	 */
	private MongoPersistentProperty getPropertyForPotentialDbRef(Path<?> path) {

		if (path == null) {
			return null;
		}

		MongoPersistentProperty property = getPropertyFor(path);
		PathMetadata metadata = path.getMetadata();

		if (property != null && property.isIdProperty() && metadata != null && metadata.getParent() != null) {
			return getPropertyFor(metadata.getParent());
		}

		return property;
	}
}
