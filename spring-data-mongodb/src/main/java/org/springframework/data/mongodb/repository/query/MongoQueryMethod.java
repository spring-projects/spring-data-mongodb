/*
 * Copyright 2011-2019 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.geo.GeoPage;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.repository.Aggregation;
import org.springframework.data.mongodb.repository.Meta;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.data.mongodb.repository.Tailable;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.query.QueryMethod;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;
import org.springframework.util.ConcurrentReferenceHashMap;
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
	private final Map<Class<? extends Annotation>, Optional<Annotation>> annotationCache;

	private @Nullable MongoEntityMetadata<?> metadata;

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
		this.annotationCache = new ConcurrentReferenceHashMap<>();
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
		return findAnnotatedQuery().isPresent();
	}

	/**
	 * Returns the query string declared in a {@link Query} annotation or {@literal null} if neither the annotation found
	 * nor the attribute was specified.
	 *
	 * @return
	 */
	@Nullable
	String getAnnotatedQuery() {
		return findAnnotatedQuery().orElse(null);
	}

	private Optional<String> findAnnotatedQuery() {

		return lookupQueryAnnotation() //
				.map(Query::value) //
				.filter(StringUtils::hasText);
	}

	/**
	 * Returns the field specification to be used for the query.
	 *
	 * @return
	 */
	@Nullable
	String getFieldSpecification() {

		return lookupQueryAnnotation() //
				.map(Query::fields) //
				.filter(StringUtils::hasText) //
				.orElse(null);
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

				MongoPersistentEntity<?> returnedEntity = mappingContext.getPersistentEntity(returnedObjectType);
				MongoPersistentEntity<?> managedEntity = mappingContext.getRequiredPersistentEntity(domainClass);
				returnedEntity = returnedEntity == null || returnedEntity.getType().isInterface() ? managedEntity
						: returnedEntity;
				MongoPersistentEntity<?> collectionEntity = domainClass.isAssignableFrom(returnedObjectType) ? returnedEntity
						: managedEntity;

				this.metadata = new SimpleMongoEntityMetadata<>((Class<Object>) returnedEntity.getType(), collectionEntity);
			}
		}

		return this.metadata;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#getDomainClass()
	 */
	protected Class<?> getDomainClass() {
		return super.getDomainClass();
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
			return GeoResult.class.equals(from.getRequiredComponentType().getType());
		}

		return false;
	}

	/**
	 * Returns the {@link Query} annotation that is applied to the method or {@code null} if none available.
	 *
	 * @return
	 */
	@Nullable
	Query getQueryAnnotation() {
		return lookupQueryAnnotation().orElse(null);
	}

	Optional<Query> lookupQueryAnnotation() {
		return doFindAnnotation(Query.class);
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
	@Nullable
	Meta getMetaAnnotation() {
		return doFindAnnotation(Meta.class).orElse(null);
	}

	/**
	 * Returns the {@link Tailable} annotation that is applied to the method or {@code null} if not available.
	 *
	 * @return
	 * @since 2.0
	 */
	@Nullable
	Tailable getTailableAnnotation() {
		return doFindAnnotation(Tailable.class).orElse(null);
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

		if (meta.cursorBatchSize() != 0) {
			metaAttributes.setCursorBatchSize(meta.cursorBatchSize());
		}

		if (StringUtils.hasText(meta.comment())) {
			metaAttributes.setComment(meta.comment());
		}

		if (!ObjectUtils.isEmpty(meta.flags())) {

			for (org.springframework.data.mongodb.core.query.Meta.CursorOption option : meta.flags()) {
				metaAttributes.addFlag(option);
			}
		}

		return metaAttributes;
	}

	/**
	 * Check if the query method is decorated with an non empty {@link Query#sort()}.
	 *
	 * @return true if method annotated with {@link Query} having an non empty sort attribute.
	 * @since 2.1
	 */
	public boolean hasAnnotatedSort() {
		return lookupQueryAnnotation().map(Query::sort).filter(StringUtils::hasText).isPresent();
	}

	/**
	 * Get the sort value, used as default, extracted from the {@link Query} annotation.
	 *
	 * @return the {@link Query#sort()} value.
	 * @throws IllegalStateException if method not annotated with {@link Query}. Make sure to check
	 *           {@link #hasAnnotatedQuery()} first.
	 * @since 2.1
	 */
	public String getAnnotatedSort() {

		return lookupQueryAnnotation().map(Query::sort).orElseThrow(() -> new IllegalStateException(
				"Expected to find @Query annotation but did not. Make sure to check hasAnnotatedSort() before."));
	}

	/**
	 * Check if the query method is decorated with an non empty {@link Query#collation()} or or
	 * {@link Aggregation#collation()}.
	 *
	 * @return true if method annotated with {@link Query} or {@link Aggregation} having a non-empty collation attribute.
	 * @since 2.2
	 */
	public boolean hasAnnotatedCollation() {

		Optional<String> optionalCollation = lookupQueryAnnotation().map(Query::collation);

		if (!optionalCollation.isPresent()) {
			optionalCollation = lookupAggregationAnnotation().map(Aggregation::collation);
		}

		return optionalCollation.filter(StringUtils::hasText).isPresent();
	}

	/**
	 * Get the collation value extracted from the {@link Query} or {@link Aggregation} annotation.
	 *
	 * @return the {@link Query#collation()} or or {@link Aggregation#collation()} value.
	 * @throws IllegalStateException if method not annotated with {@link Query} or {@link Aggregation}. Make sure to check
	 *           {@link #hasAnnotatedQuery()} first.
	 * @since 2.2
	 */
	public String getAnnotatedCollation() {

		return lookupQueryAnnotation().map(Query::collation)
				.orElseGet(() -> lookupAggregationAnnotation().map(Aggregation::collation) //
						.orElseThrow(() -> new IllegalStateException(
								"Expected to find @Query annotation but did not. Make sure to check hasAnnotatedCollation() before.")));
	}

	/**
	 * Returns whether the method has an annotated query.
	 *
	 * @return true if {@link Aggregation} is present.
	 * @since 2.2
	 */
	public boolean hasAnnotatedAggregation() {
		return findAnnotatedAggregation().isPresent();
	}

	/**
	 * Returns the aggregation pipeline declared in a {@link Aggregation} annotation.
	 *
	 * @return the aggregation pipeline.
	 * @throws IllegalStateException if method not annotated with {@link Aggregation}. Make sure to check
	 *           {@link #hasAnnotatedAggregation()} first.
	 * @since 2.2
	 */
	public String[] getAnnotatedAggregation() {
		return findAnnotatedAggregation().orElseThrow(() -> new IllegalStateException(
				"Expected to find @Aggregation annotation but did not. Make sure to check hasAnnotatedAggregation() before."));
	}

	private Optional<String[]> findAnnotatedAggregation() {

		return lookupAggregationAnnotation() //
				.map(Aggregation::pipeline) //
				.filter(it -> !ObjectUtils.isEmpty(it));
	}

	Optional<Aggregation> lookupAggregationAnnotation() {
		return doFindAnnotation(Aggregation.class);
	}

	@SuppressWarnings("unchecked")
	private <A extends Annotation> Optional<A> doFindAnnotation(Class<A> annotationType) {

		return (Optional<A>) this.annotationCache.computeIfAbsent(annotationType,
				it -> Optional.ofNullable(AnnotatedElementUtils.findMergedAnnotation(method, it)));
	}
}
