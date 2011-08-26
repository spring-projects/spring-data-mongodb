/*
 * Copyright 2011 the original author or authors.
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
package org.springframework.data.mongodb.repository;

import java.lang.reflect.Method;
import java.util.Arrays;

import org.springframework.core.MethodParameter;
import org.springframework.data.mongodb.core.geo.Distance;
import org.springframework.data.repository.query.Parameter;
import org.springframework.data.repository.query.Parameters;

/**
 * Custom extension of {@link Parameters} discovering additional
 * 
 * @author Oliver Gierke
 */
public class MongoParameters extends Parameters {

	private int distanceIndex = -1;

	public MongoParameters(Method method) {

		super(method);
		this.distanceIndex = Arrays.asList(method.getParameterTypes()).indexOf(Distance.class);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.repository.query.Parameters#createParameter(org.springframework.core.MethodParameter)
	 */
	@Override
	protected Parameter createParameter(MethodParameter parameter) {
		return new MongoParameter(parameter, this);
	}

	public int getDistanceIndex() {
		return distanceIndex;
	}

	/**
	 * Custom {@link Parameter} implementation adding parameters of type {@link Distance} to the special ones.
	 * 
	 * @author Oliver Gierke
	 */
	static class MongoParameter extends Parameter {

		/**
		 * 
		 * @param parameter
		 * @param parameters
		 */
		MongoParameter(MethodParameter parameter, Parameters parameters) {
			super(parameter, parameters);
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.repository.query.Parameter#isSpecialParameter()
		 */
		@Override
		public boolean isSpecialParameter() {
			return super.isSpecialParameter() || getType().equals(Distance.class);
		}
	}
}
