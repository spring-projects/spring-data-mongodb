/*
 * Copyright 2018-2019 the original author or authors.
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
package org.springframework.data.mongodb.core.messaging;

import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.messaging.SubscriptionRequest.RequestOptions;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * The actual {@link SubscriptionRequest} sent to the {@link MessageListenerContainer}. This wrapper type allows passing
 * in {@link RequestOptions additional information} to the Container which can be used for creating the actual
 * {@link Task} running. <br />
 * The {@link MessageListener} provides the callback interface when pushing {@link Message messages}.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public interface SubscriptionRequest<S, T, O extends RequestOptions> {

	/**
	 * Obtain the {@link MessageListener} to publish {@link Message messages} to.
	 *
	 * @return never {@literal null}.
	 */
	MessageListener<S, ? super T> getMessageListener();

	/**
	 * Get the {@link RequestOptions} specifying the requests behaviour.
	 *
	 * @return never {@literal null}.
	 */
	O getRequestOptions();

	/**
	 * Options for specifying the behaviour of the {@link SubscriptionRequest}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	interface RequestOptions {

		/**
		 * Get the database name of the db.
		 *
		 * @return the name of the database to subscribe to. Can be {@literal null} in which case the default
		 *         {@link MongoDbFactory#getMongoDatabase() database} is used.
		 */
		@Nullable
		default String getDatabaseName() {
			return null;
		}

		/**
		 * Get the collection name.
		 *
		 * @return the name of the collection to subscribe to. Can be {@literal null}.
		 */
		@Nullable
		String getCollectionName();

		/**
		 * Create empty options.
		 *
		 * @return new instance of empty {@link RequestOptions}.
		 */
		static RequestOptions none() {
			return () -> null;
		}

		/**
		 * Create options with the provided database.
		 *
		 * @param database must not be {@literal null}.
		 * @return new instance of empty {@link RequestOptions}.
		 */
		static RequestOptions justDatabase(String database) {

			Assert.notNull(database, "Database must not be null!");

			return new RequestOptions() {

				@Override
				public String getCollectionName() {
					return null;
				}

				@Override
				public String getDatabaseName() {
					return database;
				}
			};
		}

		/**
		 * Create options with the provided collection.
		 *
		 * @param collection must not be {@literal null}.
		 * @return new instance of empty {@link RequestOptions}.
		 */
		static RequestOptions justCollection(String collection) {

			Assert.notNull(collection, "Collection must not be null!");
			return () -> collection;
		}

		/**
		 * Create options with the provided database and collection.
		 *
		 * @param database must not be {@literal null}.
		 * @param collection must not be {@literal null}.
		 * @return new instance of empty {@link RequestOptions}.
		 */
		static RequestOptions of(String database, String collection) {

			Assert.notNull(database, "Database must not be null!");
			Assert.notNull(collection, "Collection must not be null!");

			return new RequestOptions() {

				@Override
				public String getCollectionName() {
					return collection;
				}

				@Override
				public String getDatabaseName() {
					return database;
				}
			};
		}
	}
}
