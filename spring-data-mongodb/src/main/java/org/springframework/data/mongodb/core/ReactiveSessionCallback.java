/*
 * Copyright 2018-2025 the original author or authors.
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
package org.springframework.data.mongodb.core;

import org.reactivestreams.Publisher;
import org.springframework.data.mongodb.core.query.Query;

/**
 * Callback interface for executing operations within a {@link com.mongodb.reactivestreams.client.ClientSession} using
 * reactive infrastructure.
 *
 * @author Christoph Strobl
 * @since 2.1
 * @see com.mongodb.reactivestreams.client.ClientSession
 */
@FunctionalInterface
public interface ReactiveSessionCallback<T> {

	/**
	 * Execute operations against a MongoDB instance via session bound {@link ReactiveMongoOperations}. The session is
	 * inferred directly into the operation so that no further interaction is necessary.
	 * <br />
	 * Please note that only Spring Data-specific abstractions like {@link ReactiveMongoOperations#find(Query, Class)} and
	 * others are enhanced with the {@link com.mongodb.session.ClientSession}. When obtaining plain MongoDB gateway
	 * objects like {@link com.mongodb.reactivestreams.client.MongoCollection} or
	 * {@link com.mongodb.reactivestreams.client.MongoDatabase} via eg.
	 * {@link ReactiveMongoOperations#getCollection(String)} we leave responsibility for
	 * {@link com.mongodb.session.ClientSession} again up to the caller.
	 *
	 * @param operations will never be {@literal null}.
	 * @return never {@literal null}.
	 */
	Publisher<T> doInSession(ReactiveMongoOperations operations);
}
