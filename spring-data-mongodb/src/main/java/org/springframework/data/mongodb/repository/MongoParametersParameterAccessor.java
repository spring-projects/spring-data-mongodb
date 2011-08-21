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

import org.springframework.data.mongodb.core.geo.Distance;
import org.springframework.data.repository.query.ParametersParameterAccessor;

/**
 * Mongo-specific {@link ParametersParameterAccessor} to allow access to the {@link Distance} parameter.
 *
 * @author Oliver Gierke
 */
public class MongoParametersParameterAccessor extends ParametersParameterAccessor implements MongoParameterAccessor {

	private final MongoParameters parameters;
	
	/**
	 * @param parameters
	 * @param values
	 */
	public MongoParametersParameterAccessor(MongoParameters parameters, Object[] values) {
		super(parameters, values);
		this.parameters = parameters;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoParameterAccessor#getMaxDistance()
	 */
	public Distance getMaxDistance() {
		int index = parameters.getDistanceIndex();
		return index == -1 ? null : (Distance) getValue(index);
	}
}
