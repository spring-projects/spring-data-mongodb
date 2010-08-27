/*
 * Copyright 2010 the original author or authors.
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

package org.springframework.datastore.document;


public abstract class AbstractDocumentStoreTemplate<C> {
	
	
	public abstract DocumentStoreConnectionFactory<C> getDocumentStoreConnectionFactory();

	public <T> T execute(DocumentStoreConnectionCallback<C, T> action) {
		try {
			return action.doInConnection(getDocumentStoreConnectionFactory().getConnection());
		}
		catch (Exception e) {
			throw new UncategorizedDocumentStoreException("Failure executing using datastore connection", e);
		}
	}
	
}
