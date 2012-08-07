/*
 * Copyright 2010-2012 the original author or authors.
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

import com.mongodb.DBObject;

/**
 * The implementation of the interface acts as a listener for the capped collection being tailed.
 * The method <i>processDocument</i> would be called for a new document found at the tail
 * of the collection.
 *
 * @author Amol Nayak
 *
 * @since 1.1
 *
 */
public interface TailingCursorDocumentListener {

	/**
	 * Invoked to notify with a new document being added to the tail of the collection
	 *
	 * @param doc The {@link DBObject} representing the document added to the tail of the collection
	 *
	 */
	void processDocument(DBObject doc);
}
