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
package org.springframework.data.mongodb.repository.query;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.data.geo.GeoPage;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.repository.InfiniteStream;
import org.springframework.data.mongodb.repository.Meta;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

/**
 * Mongo specific implementation of {@link QueryMethod}.
 * 
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class MongoQueryMethod extends QueryMethod {

	@SuppressWarnings("unchecked") private static final List<Class<? extends Serializable>> GEO_NEAR_RESULTS = Arrays
			.asList(GeoResult.class, GeoResults.class, GeoPage.class);

	private final Method method;
	private final MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext;

	private MongoEntityMetadata<?> metadata;

	/**
	 * Creates a new {@link MongoQueryMethod} from the given {@link Method}.
	 * 
	 * @param method must not be {@literal null}.
	 * @param metadata must not be {@literal null}.
	 * @param projectionFactory must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public MongoQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory projectionFactory,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		super(method, metadata, projectionFactory);

		Assert.notNull(mappingContext, "MappingContext must not be null!");

		this.method = method;
		this.mappingContext = mappingContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#getParameters(java.lang.reflect.Method)
	 */
	@Override
	protected MongoParameters createParameters(Method method) {
		return new MongoParameters(method, isGeoNearQuery(method));
	}

	/**
	 * Returns whether the method has an annotated query.
	 * 
	 * @return
	 */
	public boolean hasAnnotatedQuery() {
		return getAnnotatedQuery() != null;
	}

	/**
	 * Returns the query string declared in a {@link Query} annotation or {@literal null} if neither the annotation found
	 * nor the attribute was specified.
	 * 
	 * @return
	 */
	String getAnnotatedQuery() {

		String query = (String) AnnotationUtils.getValue(getQueryAnnotation());
		return StringUtils.hasText(query) ? query : null;
	}

	/**
	 * Returns the field specification to be used for the query.
	 * 
	 * @return
	 */
	String getFieldSpecification() {

		String value = (String) AnnotationUtils.getValue(getQueryAnnotation(), "fields");
		return StringUtils.hasText(value) ? value : null;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#getEntityInformation()
	 */
	@Override
	@SuppressWarnings("unchecked")
	public MongoEntityMetadata<?> getEntityInformation() {

		if (metadata == null) {

			Class<?> returnedObjectType = getReturnedObjectType();
			Class<?> domainClass = getDomainClass();

			if (ClassUtils.isPrimitiveOrWrapper(returnedObjectType)) {

				this.metadata = new SimpleMongoEntityMetadata<Object>((Class<Object>) domainClass,
						mappingContext.getRequiredPersistentEntity(domainClass));

			} else {

				Optional<? extends MongoPersistentEntity<?>> returnedEntity = mappingContext.getPersistentEntity(returnedObjectType);
				MongoPersistentEntity<?> managedEntity = mappingContext.getRequiredPersistentEntity(domainClass);
				returnedEntity = !returnedEntity.isPresent() || returnedEntity.get().getType().isInterface() ? Optional.of(managedEntity)
						: returnedEntity;
				MongoPersistentEntity<?> collectionEntity = domainClass.isAssignableFrom(returnedObjectType) ? returnedEntity.get()
						: managedEntity;

				this.metadata = new SimpleMongoEntityMetadata<Object>((Class<Object>) returnedEntity.get().getType(),
						collectionEntity);
			}
		}

		return this.metadata;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#getParameters()
	 */
	@Override
	public MongoParameters getParameters() {
		return (MongoParameters) super.getParameters();
	}

	/**
	 * Returns whether the query is a geo near query.
	 * 
	 * @return
	 */
	public boolean isGeoNearQuery() {
		return isGeoNearQuery(this.method);
	}

	private boolean isGeoNearQuery(Method method) {

		Class<?> returnType = method.getReturnType();

		for (Class<?> type : GEO_NEAR_RESULTS) {
			if (type.isAssignableFrom(returnType)) {
				return true;
			}
		}

		if (Iterable.class.isAssignableFrom(returnType)) {
			TypeInformation<?> from = ClassTypeInformation.fromReturnTypeOf(method);
			return GeoResult.class.equals(from.getComponentType().get().getType());
		}

		return false;
	}

	/**
	 * Returns the {@link Query} annotation that is applied to the method or {@code null} if none available.
	 * 
	 * @return
	 */
	Query getQueryAnnotation() {
		return AnnotatedElementUtils.findMergedAnnotation(method, Query.class);
	}

	TypeInformation<?> getReturnType() {
		return ClassTypeInformation.fromReturnTypeOf(method);
	}

	/**
	 * @return return true if {@link Meta} annotation is available.
	 * @since 1.6
	 */
	public boolean hasQueryMetaAttributes() {
		return getMetaAnnotation() != null;
	}

	/**
	 * Returns the {@link Meta} annotation that is applied to the method or {@code null} if not available.
	 * 
	 * @return
	 * @since 1.6
	 */
	Meta getMetaAnnotation() {
		return AnnotatedElementUtils.findMergedAnnotation(method, Meta.class);
	}

	/**
	 * Returns the {@link InfiniteStream} annotation that is applied to the method or {@code null} if not available.
	 *
	 * @return
	 * @since 2.0
	 */
	InfiniteStream getInfiniteStreamAnnotation() {
		return AnnotatedElementUtils.findMergedAnnotation(method, InfiniteStream.class);
	}

	/**
	 * Returns the {@link org.springframework.data.mongodb.core.query.Meta} attributes to be applied.
	 * 
	 * @return never {@literal null}.
	 * @since 1.6
	 */
	public org.springframework.data.mongodb.core.query.Meta getQueryMetaAttributes() {

		Meta meta = getMetaAnnotation();
		if (meta == null) {
			return new org.springframework.data.mongodb.core.query.Meta();
		}

		org.springframework.data.mongodb.core.query.Meta metaAttributes = new org.springframework.data.mongodb.core.query.Meta();
		if (meta.maxExecutionTimeMs() > 0) {
			metaAttributes.setMaxTimeMsec(meta.maxExecutionTimeMs());
		}

		if (meta.maxScanDocuments() > 0) {
			metaAttributes.setMaxScan(meta.maxScanDocuments());
		}

		if (StringUtils.hasText(meta.comment())) {
			metaAttributes.setComment(meta.comment());
		}

		if (meta.snapshot()) {
			metaAttributes.setSnapshot(meta.snapshot());
		}

		if (!ObjectUtils.isEmpty(meta.flags())) {

			for (org.springframework.data.mongodb.core.query.Meta.CursorOption option : meta.flags()) {
				metaAttributes.addFlag(option);
			}
		}

		return metaAttributes;
	}
}
