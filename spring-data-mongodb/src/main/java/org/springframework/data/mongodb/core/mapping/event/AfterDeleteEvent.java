/*
 * Copyright 2013-2016 by the original author(s).
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
 * Event being thrown after a single or a set of documents has/have been deleted. The {@link Document} held in the event
 * will be the query document <em>after</am> it has been mapped onto the domain type handled.
 *
 * @author Martin Baumgartner
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class AfterDeleteEvent<T> extends AbstractDeleteEvent<T> {

	private static final long serialVersionUID = 1L;

	/**
	 * Creates a new {@link AfterDeleteEvent} for the given {@link Document}, type and collectionName.
	 *
	 * @param dbo must not be {@literal null}.
	 * @param type may be {@literal null}.
	 * @param collectionName must not be {@literal null}.
	 * @since 1.8
	 */
	public AfterDeleteEvent(Document document, @Nullable Class<T> type, String collectionName) {
		super(document, type, collectionName);
	}
}
