/*
 * Copyright 2011-2023 the original author or authors.
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

import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

import org.bson.Document;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.QueryMapper;
import org.springframework.data.mongodb.core.mapping.FieldName;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import com.mongodb.DBRef;
import com.querydsl.core.types.Constant;
import com.querydsl.core.types.Expression;
import com.querydsl.core.types.Operation;
import com.querydsl.core.types.Path;
import com.querydsl.core.types.PathMetadata;
import com.querydsl.core.types.PathType;
import com.querydsl.mongodb.MongodbSerializer;
import com.querydsl.mongodb.document.MongodbDocumentSerializer;

/**
 * Custom {@link MongodbSerializer} to take mapping information into account when building keys for constraints.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
class SpringDataMongodbSerializer extends MongodbDocumentSerializer {

	private static final String ID_KEY = FieldName.ID.name();
	private static final Set<PathType> PATH_TYPES = Set.of(PathType.VARIABLE, PathType.PROPERTY);

	private final MongoConverter converter;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;
	private final QueryMapper mapper;

	/**
	 * Creates a new {@link SpringDataMongodbSerializer} for the given {@link MongoConverter}.
	 *
	 * @param converter must not be {@literal null}.
	 */
	public SpringDataMongodbSerializer(MongoConverter converter) {

		Assert.notNull(converter, "MongoConverter must not be null");

		this.mappingContext = converter.getMappingContext();
		this.converter = converter;
		this.mapper = new QueryMapper(converter);
	}

	@Override
	public Object visit(Constant<?> expr, Void context) {

		if (!ClassUtils.isAssignable(Enum.class, expr.getType())) {
			return super.visit(expr, context);
		}

		return converter.convertToMongoType(expr.getConstant());
	}

	@Override
	protected String getKeyForPath(Path<?> expr, PathMetadata metadata) {

		if (!metadata.getPathType().equals(PathType.PROPERTY)) {
			return super.getKeyForPath(expr, metadata);
		}

		Path<?> parent = metadata.getParent();
		MongoPersistentEntity<?> entity = mappingContext.getRequiredPersistentEntity(parent.getType());
		MongoPersistentProperty property = entity.getPersistentProperty(metadata.getName());

		return property == null ? super.getKeyForPath(expr, metadata) : property.getFieldName();
	}

	@Override
	protected Document asDocument(@Nullable String key, @Nullable Object value) {

		value = value instanceof Optional<?> optional ? optional.orElse(null) : value;

		return super.asDocument(key, value instanceof Pattern ? value : converter.convertToMongoType(value));
	}

	@Override
	protected boolean isReference(@Nullable Path<?> path) {

		MongoPersistentProperty property = getPropertyForPotentialDbRef(path);
		return property != null && property.isAssociation();
	}

	@Override
	protected DBRef asReference(@Nullable Object constant) {
		return asReference(constant, null);
	}

	protected DBRef asReference(Object constant, Path<?> path) {
		return converter.toDBRef(constant, getPropertyForPotentialDbRef(path));
	}

	@Override
	protected String asDBKey(@Nullable Operation<?> expr, int index) {

		Expression<?> arg = expr.getArg(index);
		String key = super.asDBKey(expr, index);

		if (!(arg instanceof Path<?> path)) {
			return key;
		}

		if (!isReference(path)) {
			return key;
		}

		MongoPersistentProperty property = getPropertyFor(path);

		return property.isIdProperty() ? key.replaceAll("." + ID_KEY + "$", "") : key;
	}

	protected Object convert(@Nullable Path<?> path, @Nullable Constant<?> constant) {

		if (!isReference(path)) {
			return super.convert(path, constant);
		}

		MongoPersistentProperty property = getPropertyFor(path);

		if (property.isDocumentReference()) {
			return converter.toDocumentPointer(constant.getConstant(), property).getPointer();
		}

		if (property.isIdProperty()) {

			MongoPersistentProperty propertyForPotentialDbRef = getPropertyForPotentialDbRef(path);
			if (propertyForPotentialDbRef != null && propertyForPotentialDbRef.isDocumentReference()) {
				return converter.toDocumentPointer(constant.getConstant(), propertyForPotentialDbRef).getPointer();
			}
			return asReference(constant.getConstant(), path.getMetadata().getParent());
		}

		return asReference(constant.getConstant(), path);
	}

	@Nullable
	private MongoPersistentProperty getPropertyFor(Path<?> path) {

		Path<?> parent = path.getMetadata().getParent();

		if (parent == null || !PATH_TYPES.contains(path.getMetadata().getPathType())) {
			return null;
		}

		MongoPersistentEntity<?> entity = mappingContext.getPersistentEntity(parent.getType());
		return entity != null ? entity.getPersistentProperty(path.getMetadata().getName()) : null;
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
