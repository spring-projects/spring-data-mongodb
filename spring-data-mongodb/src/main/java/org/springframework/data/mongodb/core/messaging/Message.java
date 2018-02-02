/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.core.messaging;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * General message abstraction for any type of Event / Message published by MongoDB server to the client. This might be
 * <a href="https://docs.mongodb.com/manual/reference/change-events/">Change Stream Events</a>, or
 * {@link org.bson.Document Documents} published by a
 * <a href="https://docs.mongodb.com/manual/core/tailable-cursors/">tailable cursor</a>. The original message received
 * is preserved in the raw parameter. Additional information about the origin of the {@link Message} is contained in
 * {@link MessageProperties}. <br />
 * For convenience the {@link #getBody()} of the message gets lazily converted into the target domain type if necessary
 * using the mapping infrastructure.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @see MessageProperties
 * @since 2.1
 */
public interface Message<S, T> {

	/**
	 * The raw message source as emitted by the origin.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	S getRaw();

	/**
	 * The converted message body if available.
	 *
	 * @return can be {@literal null}.
	 */
	@Nullable
	T getBody();

	/**
	 * {@link MessageProperties} containing information about the {@link Message} origin and other metadata.
	 *
	 * @return never {@literal null}.
	 */
	MessageProperties getProperties();

	/**
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	@ToString
	@EqualsAndHashCode
	class MessageProperties {

		private static final MessageProperties EMPTY = new MessageProperties();

		private @Nullable String databaseName;
		private @Nullable String collectionName;

		/**
		 * The database name the message originates from.
		 *
		 * @return
		 */
		@Nullable
		public String getDatabaseName() {
			return databaseName;
		}

		/**
		 * The collection name the message originates from.
		 *
		 * @return
		 */
		@Nullable
		public String getCollectionName() {
			return collectionName;
		}

		/**
		 * @return empty {@link MessageProperties}.
		 */
		public static MessageProperties empty() {
			return EMPTY;
		}

		/**
		 * Obtain a shiny new {@link MessagePropertiesBuilder} and start defining options in this fancy fluent way. Just
		 * don't forget to call {@link MessagePropertiesBuilder#build() build()} when your're done.
		 *
		 * @return new instance of {@link MessagePropertiesBuilder}.
		 */
		public static MessagePropertiesBuilder builder() {
			return new MessagePropertiesBuilder();
		}

		/**
		 * Builder for {@link MessageProperties}.
		 *
		 * @author Christoph Strobl
		 * @since 2.1
		 */
		public static class MessagePropertiesBuilder {

			private @Nullable String databaseName;
			private @Nullable String collectionName;

			/**
			 * @param dbName must not be {@literal null}.
			 * @return this.
			 */
			public MessagePropertiesBuilder databaseName(String dbName) {

				Assert.notNull(dbName, "Database name must not be null!");

				this.databaseName = dbName;
				return this;
			}

			/**
			 * @param collectionName must not be {@literal null}.
			 * @return this
			 */
			public MessagePropertiesBuilder collectionName(String collectionName) {

				Assert.notNull(collectionName, "Collection name must not be null!");

				this.collectionName = collectionName;
				return this;
			}

			/**
			 * @return the built {@link MessageProperties}.
			 */
			public MessageProperties build() {

				MessageProperties properties = new MessageProperties();

				properties.collectionName = collectionName;
				properties.databaseName = databaseName;

				return properties;
			}
		}
	}
}
