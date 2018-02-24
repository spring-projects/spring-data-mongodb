/*
 * Copyright 2002-2018 the original author or authors.
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
package org.springframework.data.mongodb.core;

import org.bson.Document;

import com.mongodb.client.FindIterable;

/**
 * Simple callback interface to allow customization of a {@link FindIterable}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
interface CursorPreparer {

	/**
	 * Prepare the given cursor (apply limits, skips and so on). Returns the prepared cursor.
	 *
	 * @param cursor
	 */
	FindIterable<Document> prepare(FindIterable<Document> cursor);
}
