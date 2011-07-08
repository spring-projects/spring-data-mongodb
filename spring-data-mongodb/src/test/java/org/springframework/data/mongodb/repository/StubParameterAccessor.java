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

import java.util.Arrays;
import java.util.Iterator;

import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.MongoWriter;
import org.springframework.data.mongodb.repository.ConvertingParameterAccessor;
import org.springframework.data.repository.query.ParameterAccessor;

/**
 * Simple {@link ParameterAccessor} that returns the given parameters unfiltered.
 * 
 * @author Oliver Gierke
 */
class StubParameterAccessor implements ParameterAccessor {

	private final Object[] values;

	/**
	 * Creates a new {@link ConvertingParameterAccessor} backed by a {@link StubParameterAccessor} simply returning the
	 * given parameters converted but unfiltered.
	 * 
	 * @param converter
	 * @param parameters
	 * @return
	 */
	public static ConvertingParameterAccessor getAccessor(MongoWriter<Object> converter, Object... parameters) {

		return new ConvertingParameterAccessor(converter, new StubParameterAccessor(parameters));
	}

	public StubParameterAccessor(Object... values) {
		this.values = values;
	}

	/*
	  * (non-Javadoc)
	  *
	  * @see org.springframework.data.repository.query.ParameterAccessor#getPageable()
	  */
	public Pageable getPageable() {
		return null;
	}

	/* (non-Javadoc)
	  * @see org.springframework.data.repository.query.ParameterAccessor#getBindableParameter(int)
	  */
	public Object getBindableValue(int index) {
		return values[index];
	}

	/*
	  * (non-Javadoc)
	  *
	  * @see org.springframework.data.repository.query.ParameterAccessor#getSort()
	  */
	public Sort getSort() {
		return null;
	}

	/*
	  * (non-Javadoc)
	  *
	  * @see java.lang.Iterable#iterator()
	  */
	public Iterator<Object> iterator() {
		return Arrays.asList(values).iterator();
	}
}