/*
 * Copyright 2013-2015 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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
 * Base class for delete events.
 * 
 * @author Martin Baumgartner
 * @author Christoph Strobl
 */
public abstract class AbstractDeleteEvent<T> extends MongoMappingEvent<Document> {

	private static final long serialVersionUID = 1L;
	private final Class<T> type;

	/**
	 * Creates a new {@link AbstractDeleteEvent} for the given {@link Document} and type.
	 * 
	 * @param dbo must not be {@literal null}.
	 * @param type can be {@literal null}.
	 * @deprecated since 1.8. Please use {@link #AbstractDeleteEvent(Document, Class, String)}.
	 */
	@Deprecated
	public AbstractDeleteEvent(Document dbo, Class<T> type) {
		this(dbo, type, null);
	}

	/**
	 * Creates a new {@link AbstractDeleteEvent} for the given {@link Document} and type.
	 * 
	 * @param dbo must not be {@literal null}.
	 * @param type can be {@literal null}.
	 * @param collectionName can be {@literal null}.
	 * @since 1.8
	 */
	public AbstractDeleteEvent(Document dbo, Class<T> type, String collectionName) {

		super(dbo, dbo, collectionName);
		this.type = type;
	}

	/**
	 * Returns the type for which the {@link AbstractDeleteEvent} shall be invoked for.
	 * 
	 * @return
	 */
	public Class<T> getType() {
		return type;
	}
}
