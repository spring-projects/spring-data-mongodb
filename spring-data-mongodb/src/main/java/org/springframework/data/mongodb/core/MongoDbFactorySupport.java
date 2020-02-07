/*
 * Copyright 2018-2020 the original author or authors.
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

import org.springframework.dao.support.PersistenceExceptionTranslator;

/**
 * Common base class for usage with both {@link com.mongodb.client.MongoClients} defining common properties such as
 * database name and exception translator.
 * <p/>
 * Not intended to be used directly.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @param <C> Client type.
 * @since 2.1
 * @see SimpleMongoClientDatabaseFactory
 * @deprecated since 3.0, use {@link MongoDatabaseFactorySupport} instead.
 */
@Deprecated
public abstract class MongoDbFactorySupport<C> extends MongoDatabaseFactorySupport<C> {

	/**
	 * Create a new {@link MongoDbFactorySupport} object given {@code mongoClient}, {@code databaseName},
	 * {@code mongoInstanceCreated} and {@link PersistenceExceptionTranslator}.
	 *
	 * @param mongoClient must not be {@literal null}.
	 * @param databaseName must not be {@literal null} or empty.
	 * @param mongoInstanceCreated {@literal true} if the client instance was created by a subclass of
	 *          {@link MongoDbFactorySupport} to close the client on {@link #destroy()}.
	 * @param exceptionTranslator must not be {@literal null}.
	 */
	protected MongoDbFactorySupport(C mongoClient, String databaseName, boolean mongoInstanceCreated,
			PersistenceExceptionTranslator exceptionTranslator) {
		super(mongoClient, databaseName, mongoInstanceCreated, exceptionTranslator);
	}
}
