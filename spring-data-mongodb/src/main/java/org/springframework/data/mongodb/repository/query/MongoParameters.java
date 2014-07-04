/*
 * Copyright 2011-2013 the original author or authors.
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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.springframework.core.MethodParameter;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.Point;
import org.springframework.data.mongodb.core.text.FullTextPram;
import org.springframework.data.mongodb.repository.Near;
import org.springframework.data.mongodb.repository.query.MongoParameters.MongoParameter;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;

/**
 * Custom extension of {@link Parameters} discovering additional
 * 
 * @author Oliver Gierke
 */
public class MongoParameters extends Parameters<MongoParameters, MongoParameter> {

	private final Integer distanceIndex;
	private Integer nearIndex;

	private Integer fullTextIndex;

	/**
	 * Creates a new {@link MongoParameters} instance from the given {@link Method} and {@link MongoQueryMethod}.
	 * 
	 * @param method must not be {@literal null}.
	 * @param queryMethod must not be {@literal null}.
	 */
	public MongoParameters(Method method, boolean isGeoNearMethod) {

		super(method);
		List<Class<?>> parameterTypes = Arrays.asList(method.getParameterTypes());
		this.distanceIndex = parameterTypes.indexOf(Distance.class);

		if (this.nearIndex == null && isGeoNearMethod) {
			this.nearIndex = getNearIndex(parameterTypes);
		} else if (this.nearIndex == null) {
			this.nearIndex = -1;
		}
	}

	private MongoParameters(List<MongoParameter> parameters, Integer distanceIndex, Integer nearIndex) {

		super(parameters);

		this.distanceIndex = distanceIndex;
		this.nearIndex = nearIndex;

		for (MongoParameter param : parameters) {
			if (param.hasFullTextAnnotation()) {
				this.fullTextIndex = param.getIndex();
				break;
			}
		}
	}

	@SuppressWarnings({ "unchecked", "deprecation" })
	private final int getNearIndex(List<Class<?>> parameterTypes) {

		for (Class<?> reference : Arrays.asList(Point.class, org.springframework.data.mongodb.core.geo.Point.class,
				double[].class)) {

			int nearIndex = parameterTypes.indexOf(reference);

			if (nearIndex == -1) {
				continue;
			}

			if (nearIndex == parameterTypes.lastIndexOf(reference)) {
				return nearIndex;
			} else {
				throw new IllegalStateException("Multiple Point parameters found but none annotated with @Near!");
			}
		}

		return -1;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.Parameters#createParameter(org.springframework.core.MethodParameter)
	 */
	@Override
	protected MongoParameter createParameter(MethodParameter parameter) {

		MongoParameter mongoParameter = new MongoParameter(parameter);

		if (this.fullTextIndex == null && mongoParameter.hasFullTextAnnotation()) {
			this.fullTextIndex = mongoParameter.getIndex();
		}
		// Detect manually annotated @Near Point and reject multiple annotated ones
		if (this.nearIndex == null && mongoParameter.isManuallyAnnotatedNearParameter()) {
			this.nearIndex = mongoParameter.getIndex();
		} else if (mongoParameter.isManuallyAnnotatedNearParameter()) {
			throw new IllegalStateException(String.format(
					"Found multiple @Near annotations ond method %s! Only one allowed!", parameter.getMethod().toString()));
		}

		return mongoParameter;
	}

	/**
	 * Returns the index of a {@link Distance} parameter to be used for geo queries.
	 * 
	 * @return
	 */
	public int getDistanceIndex() {
		return distanceIndex;
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
	 * Returns ths inde of the parameter to be used as a textquery param
	 * 
	 * @return
	 * @since 1.6
	 */
	public int getFullTextParameterIndex() {
		return fullTextIndex != null ? fullTextIndex.intValue() : -1;
	}

	/**
	 * @return
	 * @since 1.6
	 */
	public boolean hasFullTextParameter() {
		return this.fullTextIndex != null && this.fullTextIndex.intValue() >= 0;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.Parameters#createFrom(java.util.List)
	 */
	@Override
	protected MongoParameters createFrom(List<MongoParameter> parameters) {
		return new MongoParameters(parameters, this.distanceIndex, this.nearIndex);
	}

	/**
	 * Custom {@link Parameter} implementation adding parameters of type {@link Distance} to the special ones.
	 * 
	 * @author Oliver Gierke
	 */
	class MongoParameter extends Parameter {

		private final MethodParameter parameter;

		/**
		 * Creates a new {@link MongoParameter}.
		 * 
		 * @param parameter must not be {@literal null}.
		 */
		MongoParameter(MethodParameter parameter) {
			super(parameter);
			this.parameter = parameter;

			if (!isPoint() && hasNearAnnotation()) {
				throw new IllegalArgumentException("Near annotation is only allowed at Point parameter!");
			}
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.Parameter#isSpecialParameter()
		 */
		@Override
		public boolean isSpecialParameter() {
			return super.isSpecialParameter() || Distance.class.isAssignableFrom(getType()) || isNearParameter()
					|| hasFullTextAnnotation();
		}

		private boolean isNearParameter() {
			Integer nearIndex = MongoParameters.this.nearIndex;
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

		private boolean hasFullTextAnnotation() {
			return parameter.getParameterAnnotation(FullTextPram.class) != null;
		}
	}

}
