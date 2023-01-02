/*
 * Copyright 2020-2023 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.test.util;

import org.junit.jupiter.api.extension.ExtensionContext.Namespace;

/**
 * @author Christoph Strobl
 */
class MongoExtensions {

	static class Client {

		static final Namespace NAMESPACE = Namespace.create(MongoClientExtension.class);
		static final String SYNC_KEY = "mongo.client.sync";
		static final String REACTIVE_KEY = "mongo.client.reactive";
		static final String SYNC_REPLSET_KEY = "mongo.client.replset.sync";
		static final String REACTIVE_REPLSET_KEY = "mongo.client.replset.reactive";
	}

	static class Termplate {

		static final Namespace NAMESPACE = Namespace.create(MongoTemplateExtension.class);
		static final String SYNC = "mongo.template.sync";
		static final String REACTIVE = "mongo.template.reactive";
	}

}
