/*
 * Copyright 2026. the original author or authors.
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

import com.mongodb.MongoNamespace;

/**
 * @author Christoph Strobl
 */
public interface Namespace {

	String database();
	String collection();

	static Namespace namespace(String namespace) {
		return of(new MongoNamespace(namespace));
	}

	static Namespace of(MongoNamespace namespace) {
		return new Namespace() {

			@Override
			public String database() {
				return namespace.getDatabaseName();
			}

			@Override
			public String collection() {
				return namespace.getCollectionName();
			}
		};
	}

}
