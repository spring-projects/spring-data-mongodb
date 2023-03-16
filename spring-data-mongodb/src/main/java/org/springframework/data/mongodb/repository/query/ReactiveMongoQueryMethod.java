/*
 * Copyright 2016-2023 the original author or authors.
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
import org.springframework.data.util.Lazy;
import org.springframework.data.util.ReactiveWrappers;
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

	private static final TypeInformation<Page> PAGE_TYPE = TypeInformation.of(Page.class);
	private static final TypeInformation<Slice> SLICE_TYPE = TypeInformation.of(Slice.class);

	private final Method method;
	private final Lazy<Boolean> isCollectionQuery;

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

		this.method = method;
		this.isCollectionQuery = Lazy.of(() -> (!(isPageQuery() || isSliceQuery() || isScrollQuery())
				&& ReactiveWrappers.isMultiValueType(metadata.getReturnType(method).getType()) || super.isCollectionQuery()));
	}

	@Override
	protected MongoParameters createParameters(Method method) {
		return new MongoParameters(method, isGeoNearQuery(method));
	}

	@Override
	public boolean isCollectionQuery() {
		return isCollectionQuery.get();
	}

	@Override
	public boolean isGeoNearQuery() {
		return isGeoNearQuery(method);
	}

	private boolean isGeoNearQuery(Method method) {

		if (ReactiveWrappers.supports(method.getReturnType())) {
			TypeInformation<?> from = TypeInformation.fromReturnTypeOf(method);
			return GeoResult.class.equals(from.getRequiredComponentType().getType());
		}

		return false;
	}

	@Override
	public boolean isModifyingQuery() {
		return super.isModifyingQuery();
	}

	@Override
	public boolean isQueryForEntity() {
		return super.isQueryForEntity();
	}

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

	@Override
	public void verify() {

		if (hasParameterOfType(method, Pageable.class)) {

			TypeInformation<?> returnType = TypeInformation.fromReturnTypeOf(method);

			boolean multiWrapper = ReactiveWrappers.isMultiValueType(returnType.getType());
			boolean singleWrapperWithWrappedPageableResult = ReactiveWrappers.isSingleValueType(returnType.getType())
					&& (PAGE_TYPE.isAssignableFrom(returnType.getRequiredComponentType())
							|| SLICE_TYPE.isAssignableFrom(returnType.getRequiredComponentType()));

			if (hasParameterOfType(method, Sort.class)) {
				throw new IllegalStateException(String.format("Method must not have Pageable *and* Sort parameter;"
						+ " Use sorting capabilities on Pageable instead; Offending method: %s", method));
			}

			if (isScrollQuery()) {
				return;
			}

			if (singleWrapperWithWrappedPageableResult) {
				throw new InvalidDataAccessApiUsageException(
						String.format("'%s.%s' must not use sliced or paged execution; Please use Flux.buffer(size, skip).",
								ClassUtils.getShortName(method.getDeclaringClass()), method.getName()));
			}

			if (!multiWrapper) {
				throw new IllegalStateException(String.format(
						"Method has to use a either multi-item reactive wrapper return type or a wrapped Page/Slice type; Offending method: %s",
						method.toString()));
			}
		}

		super.verify();
	}
}
