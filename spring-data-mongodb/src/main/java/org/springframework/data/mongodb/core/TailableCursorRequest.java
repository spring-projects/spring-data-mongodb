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
package org.springframework.data.mongodb.core;

import java.util.Optional;

import org.bson.Document;
import org.springframework.data.mongodb.core.SubscriptionRequest.RequestOptions;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

/**
 * {@link SubscriptionRequest} implementation to be used to listen to query results in a
 * <a href="https://docs.mongodb.com/manual/core/capped-collections/">Capped Collection</a> using a
 * <a href="https://docs.mongodb.com/manual/core/tailable-cursors/">Tailable Cursor</a>.
 * <p />
 * The most trivial use case is subscribing to all events of a specific {@link com.mongodb.client.MongoCollection
 * collection}.
 *
 * <pre>
 * <code>
 *     TailableCursorRequest<Document> request = new TailableCursorRequest<>(System.out::println, () -> "collection-name");
 * </code>
 * </pre>
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public class TailableCursorRequest<T> implements SubscriptionRequest<Message<Document, T>, RequestOptions> {

	private final MessageListener<Message<Document, T>> messageListener;
	private final TailableCursorRequestOptions options;

	/**
	 * Create a new {@link TailableCursorRequest} with options, passing {@link Message messages} to the given
	 * {@link MessageListener}.
	 *
	 * @param messageListener must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 */
	public TailableCursorRequest(MessageListener<Message<Document, T>> messageListener, RequestOptions options) {
		this.messageListener = messageListener;
		this.options = options instanceof TailableCursorRequestOptions ? (TailableCursorRequestOptions) options
				: TailableCursorRequestOptions.of(options);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.monitor.SubscriptionRequest#getMessageListener()
	 */
	@Override
	public MessageListener<Message<Document, T>> getMessageListener() {
		return messageListener;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.monitor.SubscriptionRequest#getRequestOptions()
	 */
	@Override
	public TailableCursorRequestOptions getRequestOptions() {
		return options;
	}

	/**
	 * {@link SubscriptionRequest.RequestOptions} implementation specific to a {@link TailableCursorRequest}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class TailableCursorRequestOptions implements SubscriptionRequest.RequestOptions {

		private String collectionName;
		private @Nullable Query query;

		public TailableCursorRequestOptions() {}

		static TailableCursorRequestOptions of(RequestOptions options) {
			return builder().collection(options.getCollectionName()).build();
		}

		/**
		 * Obtain a shiny new {@link TailableCursorRequestOptionsBuilder} and start defining options in this fancy fluent
		 * way. Just don't forget to call {@link TailableCursorRequestOptionsBuilder#build() build()} when your're done.
		 *
		 * @return new instance of {@link ChangeStreamRequestOptionsBuilder}.
		 */
		public static TailableCursorRequestOptionsBuilder builder() {
			return new TailableCursorRequestOptionsBuilder();
		}

		@Override
		public String getCollectionName() {
			return collectionName;
		}

		public Optional<Query> getQuery() {
			return Optional.ofNullable(query);
		}

		/**
		 * Builder for creating {@link TailableCursorRequestOptions}.
		 *
		 * @author Christoph Strobl
		 * @since 2.1
		 */
		public static class TailableCursorRequestOptionsBuilder {

			TailableCursorRequestOptions options = new TailableCursorRequestOptions();

			/**
			 * Set the collection name to listen to.
			 *
			 * @param collection must not be {@literal null} nor {@literal empty}.
			 * @return this.
			 */
			public TailableCursorRequestOptionsBuilder collection(String collection) {

				Assert.hasText(collection, "Collection must not be null nor empty!");

				options.collectionName = collection;
				return this;
			}

			/**
			 * Set the filter to apply.
			 *
			 * @param filter the {@link Query } to apply for filtering events. Must not be {@literal null}.
			 * @return this.
			 */
			public TailableCursorRequestOptionsBuilder filter(Query filter) {

				Assert.notNull(filter, "Filter must not be null!");

				options.query = filter;
				return this;
			}

			public TailableCursorRequestOptions build() {

				TailableCursorRequestOptions tmp = options;
				options = new TailableCursorRequestOptions();
				return tmp;
			}

		}
	}
}
