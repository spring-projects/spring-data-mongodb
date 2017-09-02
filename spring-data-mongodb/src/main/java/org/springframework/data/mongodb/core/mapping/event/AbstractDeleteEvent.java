/*
 * Copyright 2013-2017 by the original author(s).
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
import org.springframework.lang.Nullable;

/**
 * Base class for delete events.
 *
 * @author Martin Baumgartner
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public abstract class AbstractDeleteEvent<T> extends MongoMappingEvent<Document> {

	private static final long serialVersionUID = 1L;
	private final @Nullable Class<T> type;

	/**
	 * Creates a new {@link AbstractDeleteEvent} for the given {@link Document} and type.
	 *
	 * @param document must not be {@literal null}.
	 * @param type may be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @since 1.8
	 */
	public AbstractDeleteEvent(Document document, @Nullable Class<T> type, String collectionName) {

		super(document, document, collectionName);
		this.type = type;
	}

	/**
	 * Returns the type for which the {@link AbstractDeleteEvent} shall be invoked for.
	 *
	 * @return
	 */
	@Nullable
	public Class<T> getType() {
		return type;
	}
}
