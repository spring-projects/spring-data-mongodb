/*
 * Copyright 2011-2018 the original author or authors.
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

import org.bson.Document;

/**
 * {@link MongoMappingEvent} thrown after convert of a document.
 *
 * @author Jon Brisbin
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class AfterConvertEvent<E> extends MongoMappingEvent<E> {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates new {@link AfterConvertEvent}.
	 *
	 * @param document must not be {@literal null}.
	 * @param source must not be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @since 1.8
	 */
	public AfterConvertEvent(Document document, E source, String collectionName) {
		super(source, document, collectionName);
	}

}
