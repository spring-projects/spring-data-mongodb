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

import com.mongodb.DBObject;

/**
 * Event being thrown before a document is deleted. The {@link DBObject} held in the event will represent the query
 * document <em>before</em> being mapped based on the domain class handled.
 * 
 * @author Martin Baumgartner
 * @author Christoph Strobl
 */
public class BeforeDeleteEvent<T> extends AbstractDeleteEvent<T> {

	private static final long serialVersionUID = -2627547705679734497L;

	/**
	 * Creates a new {@link BeforeDeleteEvent} for the given {@link DBObject} and type.
	 * 
	 * @param dbo must not be {@literal null}.
	 * @param type can be {@literal null}.
	 * @deprecated since 1.8. Please use {@link #BeforeDeleteEvent(DBObject, Class, String)}.
	 */
	@Deprecated
	public BeforeDeleteEvent(DBObject dbo, Class<T> type) {
		this(dbo, type, null);
	}

	/**
	 * Creates a new {@link BeforeDeleteEvent} for the given {@link DBObject}, type and collectionName.
	 * 
	 * @param dbo must not be {@literal null}.
	 * @param type can be {@literal null}.
	 * @param collectionName can be {@literal null}
	 * @since 1.8
	 */
	public BeforeDeleteEvent(DBObject dbo, Class<T> type, String collectionName) {
		super(dbo, type, collectionName);
	}
}
