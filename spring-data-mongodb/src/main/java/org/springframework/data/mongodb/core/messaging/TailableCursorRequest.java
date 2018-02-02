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

import java.util.Optional;

import org.bson.Document;
import org.springframework.data.mongodb.core.messaging.SubscriptionRequest.RequestOptions;
import org.springframework.data.mongodb.core.messaging.TailableCursorRequest.TailableCursorRequestOptions.TailableCursorRequestOptionsBuilder;
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
 * {@link TailableCursorRequestBuilder} offers a fluent API for creating {@link TailableCursorRequest} with
 * {@link TailableCursorRequestOptions} in one go.
 *
 * <pre>
 *   <code>
 *       TailableCursorRequest<Document> request = TailableCursorRequest.builder()
 *           .collection("collection-name")
 *           .publishTo(System.out::println)
 *           .build();
 *   </code>
 * </pre>
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
public class TailableCursorRequest<T> implements SubscriptionRequest<Document, T, RequestOptions> {

	private final MessageListener<Document, ? super T> messageListener;
	private final TailableCursorRequestOptions options;

	/**
	 * Create a new {@link TailableCursorRequest} with options, passing {@link Message messages} to the given
	 * {@link MessageListener}.
	 *
	 * @param messageListener must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 */
	public TailableCursorRequest(MessageListener<Document, ? super T> messageListener, RequestOptions options) {

		Assert.notNull(messageListener, "MessageListener must not be null!");
		Assert.notNull(options, "Options must not be null!");

		this.messageListener = messageListener;
		this.options = options instanceof TailableCursorRequestOptions ? (TailableCursorRequestOptions) options
				: TailableCursorRequestOptions.of(options);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.monitor.SubscriptionRequest#getMessageListener()
	 */
	@Override
	public MessageListener<Document, ? super T> getMessageListener() {
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
	 * Obtain a shiny new {@link TailableCursorRequestBuilder} and start defining options in this fancy fluent way. Just
	 * don't forget to call {@link TailableCursorRequestBuilder#build() build()} when your're done.
	 *
	 * @return new instance of {@link TailableCursorRequestBuilder}.
	 */
	public static TailableCursorRequestBuilder builder() {
		return new TailableCursorRequestBuilder();
	}

	/**
	 * Obtain a shiny new {@link TailableCursorRequestBuilder} and start defining options in this fancy fluent way. Just
	 * don't forget to call {@link TailableCursorRequestBuilder#build() build()} when your're done.
	 *
	 * @return new instance of {@link TailableCursorRequestBuilder}.
	 */
	public static <T> TailableCursorRequestBuilder<T> builder(MessageListener<Document, ? super T> listener) {

		TailableCursorRequestBuilder<T> builder = new TailableCursorRequestBuilder<>();
		return builder.publishTo(listener);
	}

	/**
	 * {@link SubscriptionRequest.RequestOptions} implementation specific to a {@link TailableCursorRequest}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class TailableCursorRequestOptions implements SubscriptionRequest.RequestOptions {

		private @Nullable String collectionName;
		private @Nullable Query query;

		TailableCursorRequestOptions() {}

		public static TailableCursorRequestOptions of(RequestOptions options) {
			return builder().collection(options.getCollectionName()).build();
		}

		/**
		 * Obtain a shiny new {@link TailableCursorRequestOptionsBuilder} and start defining options in this fancy fluent
		 * way. Just don't forget to call {@link TailableCursorRequestOptionsBuilder#build() build()} when your're done.
		 *
		 * @return new instance of {@link TailableCursorRequestOptionsBuilder}.
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

			private @Nullable String collectionName;
			private @Nullable Query query;

			private TailableCursorRequestOptionsBuilder() {}

			/**
			 * Set the collection name to tail.
			 *
			 * @param collection must not be {@literal null} nor {@literal empty}.
			 * @return this.
			 */
			public TailableCursorRequestOptionsBuilder collection(String collection) {

				Assert.hasText(collection, "Collection must not be null nor empty!");

				this.collectionName = collection;
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

				this.query = filter;
				return this;
			}

			/**
			 * @return the built {@link TailableCursorRequestOptions}.
			 */
			public TailableCursorRequestOptions build() {

				TailableCursorRequestOptions options = new TailableCursorRequestOptions();

				options.collectionName = collectionName;
				options.query = query;

				return options;
			}
		}
	}

	/**
	 * Builder for creating {@link TailableCursorRequest}.
	 *
	 * @author Mark Paluch
	 * @since 2.1
	 * @see TailableCursorRequestOptions
	 */
	public static class TailableCursorRequestBuilder<T> {

		private @Nullable MessageListener<Document, ? super T> listener;
		private TailableCursorRequestOptionsBuilder delegate = TailableCursorRequestOptions.builder();

		private TailableCursorRequestBuilder() {}

		/**
		 * Set the name of the {@link com.mongodb.client.MongoCollection} to listen to.
		 *
		 * @param collectionName must not be {@literal null} nor empty.
		 * @return this.
		 */
		public TailableCursorRequestBuilder<T> collection(String collectionName) {

			Assert.hasText(collectionName, "CollectionName must not be null!");

			delegate.collection(collectionName);
			return this;
		}

		/**
		 * Set the {@link MessageListener} event {@link Message messages} will be published to.
		 *
		 * @param messageListener must not be {@literal null}.
		 * @return this.
		 */
		public TailableCursorRequestBuilder<T> publishTo(MessageListener<Document, ? super T> messageListener) {

			Assert.notNull(messageListener, "MessageListener must not be null!");

			this.listener = messageListener;
			return this;
		}

		/**
		 * Set the filter to apply.
		 *
		 * @param filter the {@link Query } to apply for filtering events. Must not be {@literal null}.
		 * @return this.
		 */
		public TailableCursorRequestBuilder<T> filter(Query filter) {

			Assert.notNull(filter, "Filter must not be null!");

			delegate.filter(filter);
			return this;
		}

		/**
		 * @return the build {@link ChangeStreamRequest}.
		 */
		public TailableCursorRequest<T> build() {

			Assert.notNull(listener, "MessageListener must not be null!");

			return new TailableCursorRequest<>(listener, delegate.build());
		}
	}
}
