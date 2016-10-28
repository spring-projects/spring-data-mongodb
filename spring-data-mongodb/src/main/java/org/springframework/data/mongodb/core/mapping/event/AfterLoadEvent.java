/*
 * Copyright (c) 2011-2016 by the original author(s).
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
import org.springframework.util.Assert;

/**
 * Event to be triggered after loading {@link Document}s to be mapped onto a given type.
 * 
 * @author Oliver Gierke
 * @author Jon Brisbin
 * @author Christoph Leiter
 * @author Christoph Strobl
 */
public class AfterLoadEvent<T> extends MongoMappingEvent<Document> {

	private static final long serialVersionUID = 1L;
	private final Class<T> type;

	/**
	 * Creates a new {@link AfterLoadEvent} for the given {@link Document}, type and collectionName.
	 * 
	 * @param document must not be {@literal null}.
	 * @param type must not be {@literal null}.
	 * @param collectionName can be {@literal null}.
	 * @since 1.8
	 */
	public AfterLoadEvent(Document document, Class<T> type, String collectionName) {

		super(document, document, collectionName);

		Assert.notNull(type, "Type must not be null!");
		this.type = type;
	}

	/**
	 * Returns the type for which the {@link AfterLoadEvent} shall be invoked for.
	 * 
	 * @return
	 */
	public Class<T> getType() {
		return type;
	}
}
