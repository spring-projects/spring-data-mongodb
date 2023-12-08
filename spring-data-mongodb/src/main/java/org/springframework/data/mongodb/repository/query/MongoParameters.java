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
package org.springframework.data.mongodb.repository.query;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.springframework.core.MethodParameter;
import org.springframework.data.domain.Range;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.core.query.UpdateDefinition;
import org.springframework.data.mongodb.repository.Near;
import org.springframework.data.mongodb.repository.query.MongoParameters.MongoParameter;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;
import org.springframework.data.repository.query.ParametersSource;
import org.springframework.data.util.TypeInformation;
import org.springframework.lang.Nullable;

/**
 * Custom extension of {@link Parameters} discovering additional
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 * @author Mark Paluch
 * @author Thomas Darimont
 */
public class MongoParameters extends Parameters<MongoParameters, MongoParameter> {

	private final int rangeIndex;
	private final int maxDistanceIndex;
	private final @Nullable Integer fullTextIndex;
	private final @Nullable Integer nearIndex;
	private final @Nullable Integer collationIndex;
	private final int updateIndex;
	private final TypeInformation<?> domainType;

	/**
	 * Creates a new {@link MongoParameters} instance from the given {@link Method} and {@link MongoQueryMethod}.
	 *
	 * @param parametersSource must not be {@literal null}.
	 * @param isGeoNearMethod indicate if this is a geo spatial query method
	 */
	public MongoParameters(ParametersSource parametersSource, boolean isGeoNearMethod) {
		this(parametersSource, new NearIndex(parametersSource, isGeoNearMethod));
	}

	/**
	 * Creates a new {@link MongoParameters} instance from the given {@link Method} and {@link MongoQueryMethod}.
	 *
	 * @param parametersSource must not be {@literal null}.
	 * @param nearIndex the near parameter index.
	 */
	private MongoParameters(ParametersSource parametersSource, NearIndex nearIndex) {

		super(parametersSource, methodParameter -> new MongoParameter(methodParameter,
				parametersSource.getDomainTypeInformation(), nearIndex.nearIndex));

		Method method = parametersSource.getMethod();
		List<Class<?>> parameterTypes = Arrays.asList(method.getParameterTypes());

		this.domainType = parametersSource.getDomainTypeInformation();
		this.fullTextIndex = parameterTypes.indexOf(TextCriteria.class);

		TypeInformation<?> declaringClassInfo = TypeInformation.of(parametersSource.getContainingClass());
		List<TypeInformation<?>> parameterTypeInfo = declaringClassInfo.getParameterTypes(method);

		this.rangeIndex = getTypeIndex(parameterTypeInfo, Range.class, Distance.class);
		this.maxDistanceIndex = this.rangeIndex == -1 ? getTypeIndex(parameterTypeInfo, Distance.class, null) : -1;
		this.collationIndex = getTypeIndex(parameterTypeInfo, Collation.class, null);
		this.updateIndex = QueryUtils.indexOfAssignableParameter(UpdateDefinition.class, parameterTypes);
		this.nearIndex = nearIndex.nearIndex;
	}

	private MongoParameters(List<MongoParameter> parameters, int maxDistanceIndex, @Nullable Integer nearIndex,
			@Nullable Integer fullTextIndex, int rangeIndex, @Nullable Integer collationIndex, int updateIndex,
			TypeInformation<?> domainType) {

		super(parameters);

		this.nearIndex = nearIndex;
		this.fullTextIndex = fullTextIndex;
		this.maxDistanceIndex = maxDistanceIndex;
		this.rangeIndex = rangeIndex;
		this.collationIndex = collationIndex;
		this.updateIndex = updateIndex;
		this.domainType = domainType;
	}

	static class NearIndex {

		private final @Nullable Integer nearIndex;

		public NearIndex(ParametersSource parametersSource, boolean isGeoNearMethod) {

			int index = findNearIndexInParameters(parametersSource.getMethod());
			if (index == -1 && isGeoNearMethod) {
				index = getNearIndex(Arrays.asList(parametersSource.getMethod().getParameterTypes()));
			}

			this.nearIndex = index;
		}
	}

	private static int getNearIndex(List<Class<?>> parameterTypes) {

		for (Class<?> reference : Arrays.asList(Point.class, double[].class)) {

			int nearIndex = parameterTypes.indexOf(reference);

			if (nearIndex == -1) {
				continue;
			}

			if (nearIndex == parameterTypes.lastIndexOf(reference)) {
				return nearIndex;
			} else {
				throw new IllegalStateException("Multiple Point parameters found but none annotated with @Near");
			}
		}

		return -1;
	}

	static int findNearIndexInParameters(Method method) {

		int index = -1;
		for (java.lang.reflect.Parameter p : method.getParameters()) {

			MethodParameter methodParameter = MethodParameter.forParameter(p);

			if ((Point.class.isAssignableFrom(methodParameter.getParameterType())
					|| methodParameter.getParameterType().equals(double[].class))
					&& methodParameter.hasParameterAnnotation(Near.class)) {
				if (index == -1) {
					index = methodParameter.getParameterIndex();
				} else {
					throw new IllegalStateException(
							String.format("Found multiple @Near annotations ond method %s; Only one allowed", method));
				}

			}
		}
		return index;
	}

	public int getDistanceRangeIndex() {
		return -1;
	}

	/**
	 * Returns the index of the {@link Distance} parameter to be used for max distance in geo queries.
	 *
	 * @return
	 * @since 1.7
	 */
	public int getMaxDistanceIndex() {
		return maxDistanceIndex;
	}

	/**
	 * Returns the index of the parameter to be used to start a geo-near query from.
	 *
	 * @return
	 */
	public int getNearIndex() {
		return nearIndex;
	}

	/**
	 * Returns the index of the parameter to be used as a text query param
	 *
	 * @return
	 * @since 1.6
	 */
	public int getFullTextParameterIndex() {
		return fullTextIndex != null ? fullTextIndex : -1;
	}

	/**
	 * @return
	 * @since 1.6
	 */
	public boolean hasFullTextParameter() {
		return this.fullTextIndex != null && this.fullTextIndex >= 0;
	}

	/**
	 * @return
	 * @since 1.7
	 */
	public int getRangeIndex() {
		return rangeIndex;
	}

	/**
	 * Returns the index of the {@link Collation} parameter or -1 if not present.
	 *
	 * @return -1 if not set.
	 * @since 2.2
	 */
	public int getCollationParameterIndex() {
		return collationIndex != null ? collationIndex : -1;
	}

	/**
	 * Returns the index of the {@link UpdateDefinition} parameter or -1 if not present.
	 *
	 * @return -1 if not present.
	 * @since 3.4
	 */
	public int getUpdateIndex() {
		return updateIndex;
	}

	@Override
	protected MongoParameters createFrom(List<MongoParameter> parameters) {
		return new MongoParameters(parameters, this.maxDistanceIndex, this.nearIndex, this.fullTextIndex, this.rangeIndex,
				this.collationIndex, this.updateIndex, this.domainType);
	}

	private int getTypeIndex(List<TypeInformation<?>> parameterTypes, Class<?> type, @Nullable Class<?> componentType) {

		for (int i = 0; i < parameterTypes.size(); i++) {

			TypeInformation<?> candidate = parameterTypes.get(i);

			if (candidate.getType().equals(type)) {

				if (componentType == null) {
					return i;
				} else if (componentType.equals(candidate.getRequiredComponentType().getType())) {
					return i;
				}
			}
		}

		return -1;
	}

	/**
	 * Custom {@link Parameter} implementation adding parameters of type {@link Distance} to the special ones.
	 *
	 * @author Oliver Gierke
	 */
	static class MongoParameter extends Parameter {

		private final MethodParameter parameter;
		private final @Nullable Integer nearIndex;

		/**
		 * Creates a new {@link MongoParameter}.
		 *
		 * @param parameter must not be {@literal null}.
		 * @param domainType must not be {@literal null}.
		 */
		MongoParameter(MethodParameter parameter, TypeInformation<?> domainType, @Nullable Integer nearIndex) {
			super(parameter, domainType);
			this.parameter = parameter;
			this.nearIndex = nearIndex;

			if (!isPoint() && hasNearAnnotation()) {
				throw new IllegalArgumentException("Near annotation is only allowed at Point parameter");
			}
		}

		@Override
		public boolean isSpecialParameter() {
			return super.isSpecialParameter() || Distance.class.isAssignableFrom(getType()) || isNearParameter()
					|| TextCriteria.class.isAssignableFrom(getType()) || Collation.class.isAssignableFrom(getType());
		}

		private boolean isNearParameter() {
			return nearIndex != null && nearIndex.equals(getIndex());
		}

		private boolean isManuallyAnnotatedNearParameter() {
			return isPoint() && hasNearAnnotation();
		}

		private boolean isPoint() {
			return Point.class.isAssignableFrom(getType()) || getType().equals(double[].class);
		}

		private boolean hasNearAnnotation() {
			return parameter.getParameterAnnotation(Near.class) != null;
		}
	}

}
