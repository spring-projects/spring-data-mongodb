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
import org.springframework.data.mongodb.core.geo.Point;
import org.springframework.data.repository.query.ParametersParameterAccessor;

/**
 * Mongo-specific {@link ParametersParameterAccessor} to allow access to the {@link Distance} parameter.
 *
 * @author Oliver Gierke
 */
public class MongoParametersParameterAccessor extends ParametersParameterAccessor implements MongoParameterAccessor {

	private final MongoQueryMethod method;
	
	/**
	 * Creates a new {@link MongoParametersParameterAccessor}.
	 * 
	 * @param method must not be {@literal null}.
	 * @param values must not be {@@iteral null}.
	 */
	public MongoParametersParameterAccessor(MongoQueryMethod method, Object[] values) {
		super(method.getParameters(), values);
		this.method = method;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoParameterAccessor#getMaxDistance()
	 */
	public Distance getMaxDistance() {
		int index = method.getParameters().getDistanceIndex();
		return index == -1 ? null : (Distance) getValue(index);
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.repository.MongoParameterAccessor#getGeoNearLocation()
	 */
	public Point getGeoNearLocation() {
		
		int nearIndex = method.getParameters().getNearIndex();
		
		if (nearIndex == -1) {
			return null;
		}
		
		Object value = getValue(nearIndex);
		
		if (value == null) {
			return null;
		}
		
		if (value instanceof double[]) {
			double[] typedValue = (double[]) value;
			if (typedValue.length != 2) {
				throw new IllegalArgumentException("The given double[] must have exactly 2 elements!");
			} else {
				return new Point(typedValue[0], typedValue[1]);
			}
		}
		
		return (Point) value;
	}
}
