/*
 * Copyright 2016 the original author or authors.
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

import static org.springframework.data.repository.util.ClassUtils.*;

import java.lang.reflect.Method;

import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Slice;
import org.springframework.data.domain.Sort;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mongodb.core.mapping.MongoPersistentEntity;
import org.springframework.data.mongodb.core.mapping.MongoPersistentProperty;
import org.springframework.data.mongodb.repository.query.MongoParameters.MongoParameter;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.util.ReactiveWrapperConverters;
import org.springframework.data.repository.util.ReactiveWrappers;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.ClassUtils;

/**
 * Reactive specific implementation of {@link MongoQueryMethod}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.0
 */
public class ReactiveMongoQueryMethod extends MongoQueryMethod {

	private static final ClassTypeInformation<Page> PAGE_TYPE = ClassTypeInformation.from(Page.class);
	private static final ClassTypeInformation<Slice> SLICE_TYPE = ClassTypeInformation.from(Slice.class);

	private final Method method;

	/**
	 * Creates a new {@link ReactiveMongoQueryMethod} from the given {@link Method}.
	 *
	 * @param method must not be {@literal null}.
	 * @param metadata must not be {@literal null}.
	 * @param projectionFactory must not be {@literal null}.
	 * @param mappingContext must not be {@literal null}.
	 */
	public ReactiveMongoQueryMethod(Method method, RepositoryMetadata metadata, ProjectionFactory projectionFactory,
			MappingContext<? extends MongoPersistentEntity<?>, MongoPersistentProperty> mappingContext) {

		super(method, metadata, projectionFactory, mappingContext);

		if (hasParameterOfType(method, Pageable.class)) {

			TypeInformation<?> returnType = ClassTypeInformation.fromReturnTypeOf(method);

			boolean multiWrapper = ReactiveWrappers.isMultiValueType(returnType.getType());
			boolean singleWrapperWithWrappedPageableResult = ReactiveWrappers.isSingleValueType(returnType.getType())
					&& (PAGE_TYPE.isAssignableFrom(returnType.getComponentType().get())
							|| SLICE_TYPE.isAssignableFrom(returnType.getComponentType().get()));

			if (singleWrapperWithWrappedPageableResult) {
				throw new InvalidDataAccessApiUsageException(
						String.format("'%s.%s' must not use sliced or paged execution. Please use Flux.buffer(size, skip).",
								ClassUtils.getShortName(method.getDeclaringClass()), method.getName()));
			}

			if (!multiWrapper && !singleWrapperWithWrappedPageableResult) {
				throw new IllegalStateException(String.format(
						"Method has to use a either multi-item reactive wrapper return type or a wrapped Page/Slice type. Offending method: %s",
						method.toString()));
			}

			if (hasParameterOfType(method, Sort.class)) {
				throw new IllegalStateException(String.format("Method must not have Pageable *and* Sort parameter. "
						+ "Use sorting capabilities on Pageble instead! Offending method: %s", method.toString()));
			}
		}

		this.method = method;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.MongoQueryMethod#createParameters(java.lang.reflect.Method)
	 */
	@Override
	protected MongoParameters createParameters(Method method) {
		return new MongoParameters(method, isGeoNearQuery(method));
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#isCollectionQuery()
	 */
	@Override
	public boolean isCollectionQuery() {
		return !(isPageQuery() || isSliceQuery()) && ReactiveWrappers.isMultiValueType(method.getReturnType());
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.query.MongoQueryMethod#isGeoNearQuery()
	 */
	@Override
	public boolean isGeoNearQuery() {
		return isGeoNearQuery(method);
	}

	private boolean isGeoNearQuery(Method method) {

		if (ReactiveWrappers.supports(method.getReturnType())) {
			TypeInformation<?> from = ClassTypeInformation.fromReturnTypeOf(method);
			return GeoResult.class.equals(from.getComponentType().get().getType());
		}

		return false;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#isModifyingQuery()
	 */
	@Override
	public boolean isModifyingQuery() {
		return super.isModifyingQuery();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#isQueryForEntity()
	 */
	@Override
	public boolean isQueryForEntity() {
		return super.isQueryForEntity();
	}

	/*
	 * All reactive query methods are streaming queries.
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.QueryMethod#isStreamQuery()
	 */
	@Override
	public boolean isStreamQuery() {
		return true;
	}

	/**
	 * Check if the given {@link org.springframework.data.repository.query.QueryMethod} receives a reactive parameter
	 * wrapper as one of its parameters.
	 *
	 * @return
	 */
	public boolean hasReactiveWrapperParameter() {

		for (MongoParameter mongoParameter : getParameters()) {
			if (ReactiveWrapperConverters.supports(mongoParameter.getType())) {
				return true;
			}
		}
		return false;
	}

}
