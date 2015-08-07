/*
 * Copyright 2011-2015 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping.event;

/**
 * Event being thrown before a domain object is converted to be persisted.
 * 
 * @author Jon Brisbin
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class BeforeConvertEvent<T> extends MongoMappingEvent<T> {

	private static final long serialVersionUID = 252614269008845243L;

	/**
	 * Creates new {@link BeforeConvertEvent}.
	 * 
	 * @param source must not be {@literal null}.
	 * @deprecated since 1.8. Please use {@link #BeforeConvertEvent(Object, String)}.
	 */
	@Deprecated
	public BeforeConvertEvent(T source) {
		this(source, null);
	}

	/**
	 * Creates new {@link BeforeConvertEvent}.
	 * 
	 * @param source must not be {@literal null}.
	 * @param collectionName can be {@literal null}.
	 * @since 1.8
	 */
	public BeforeConvertEvent(T source, String collectionName) {
		super(source, null, collectionName);
	}
}
