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

import java.time.Instant;

import org.bson.BsonValue;
import org.bson.Document;
import org.springframework.data.mongodb.core.ChangeStreamOptions;
import org.springframework.data.mongodb.core.ChangeStreamOptions.ChangeStreamOptionsBuilder;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.messaging.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.data.mongodb.core.query.Collation;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;

/**
 * {@link SubscriptionRequest} implementation to be used for listening to
 * <a href="https://docs.mongodb.com/manual/changeStreams/">Change Streams</a> via a {@link MessageListenerContainer}
 * using the synchronous MongoDB Java driver.
 * <p/>
 * The most trivial use case is subscribing to all events of a specific {@link com.mongodb.client.MongoCollection
 * collection}
 *
 * <pre>
 * <code>
 *     ChangeStreamRequest<Document> request = new ChangeStreamRequest<>(System.out::println, () -> "collection-name");
 * </code>
 * </pre>
 *
 * or {@link com.mongodb.client.MongoDatabase} which receives events from all {@link com.mongodb.client.MongoCollection
 * collections} in that database.
 *
 * <pre>
 * <code>
 *     ChangeStreamRequest<Document> request = new ChangeStreamRequest<>(System.out::println, RequestOptions.justDatabase("test"));
 * </code>
 * </pre>
 *
 * For more advanced scenarios {@link ChangeStreamOptions} offers abstractions for options like filtering, resuming,...
 *
 * <pre>
 * <code>
 *     ChangeStreamOptions options = ChangeStreamOptions.builder()
 *         .filter(newAggregation(match(where("age").is(7))))
 *         .returnFullDocumentOnUpdate()
 *         .build();
 *
 *     ChangeStreamRequest<Document> request = new ChangeStreamRequest<>(System.out::println, new ChangeStreamRequestOptions("collection-name", options));
 * </code>
 * </pre>
 *
 * {@link ChangeStreamRequestBuilder} offers a fluent API for creating {@link ChangeStreamRequest} with
 * {@link ChangeStreamOptions} in one go.
 *
 * <pre>
 * <code>
 *     ChangeStreamRequest<Document> request = ChangeStreamRequest.builder()
 *         .collection("collection-name")
 *         .publishTo(System.out::println)
 *         .filter(newAggregation(match(where("age").is(7))))
 *         .fullDocumentLookup(UPDATE_LOOKUP)
 *         .build();
 * </code>
 * </pre>
 *
 * {@link Message Messges} passed to the {@link MessageListener} contain the {@link ChangeStreamDocument} within their
 * {@link Message#getRaw() raw value} while the {@code fullDocument} is extracted into the {@link Message#getBody()
 * messages body}. Unless otherwise specified (via {@link ChangeStreamOptions#getFullDocumentLookup()} the
 * {@link Message#getBody() message body} for {@code update events} will be empty for a {@link Document} target type.
 * {@link Message#getBody()} Message bodies} that map to a different target type automatically enforce an
 * {@link FullDocument#UPDATE_LOOKUP}.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
public class ChangeStreamRequest<T>
		implements SubscriptionRequest<ChangeStreamDocument<Document>, T, ChangeStreamRequestOptions> {

	private final MessageListener<ChangeStreamDocument<Document>, ? super T> messageListener;
	private final ChangeStreamRequestOptions options;

	/**
	 * Create a new {@link ChangeStreamRequest} with options, passing {@link Message messages} to the given
	 * {@link MessageListener}.
	 *
	 * @param messageListener must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 */
	public ChangeStreamRequest(MessageListener<ChangeStreamDocument<Document>, ? super T> messageListener,
			RequestOptions options) {

		Assert.notNull(messageListener, "MessageListener must not be null!");
		Assert.notNull(options, "Options must not be null!");

		this.options = options instanceof ChangeStreamRequestOptions ? (ChangeStreamRequestOptions) options
				: ChangeStreamRequestOptions.of(options);

		this.messageListener = messageListener;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.monitor.SubscriptionRequest#getMessageListener()
	 */
	@Override
	public MessageListener<ChangeStreamDocument<Document>, ? super T> getMessageListener() {
		return messageListener;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.mongodb.monitor.SubscriptionRequest#getRequestOptions()
	 */
	@Override
	public ChangeStreamRequestOptions getRequestOptions() {
		return options;
	}

	/**
	 * Obtain a shiny new {@link ChangeStreamRequestBuilder} and start defining your {@link ChangeStreamRequest} in this
	 * fancy fluent way. Just don't forget to call {@link ChangeStreamRequestBuilder#build() build()} when your're done.
	 *
	 * @return new instance of {@link ChangeStreamRequest}.
	 */
	public static ChangeStreamRequestBuilder builder() {
		return new ChangeStreamRequestBuilder();
	}

	/**
	 * Obtain a shiny new {@link ChangeStreamRequestBuilder} and start defining your {@link ChangeStreamRequest} in this
	 * fancy fluent way. Just don't forget to call {@link ChangeStreamRequestBuilder#build() build()} when your're done.
	 *
	 * @return new instance of {@link ChangeStreamRequest}.
	 */
	public static <T> ChangeStreamRequestBuilder<T> builder(
			MessageListener<ChangeStreamDocument<Document>, ? super T> listener) {

		ChangeStreamRequestBuilder<T> builder = new ChangeStreamRequestBuilder<>();
		return builder.publishTo(listener);
	}

	/**
	 * {@link SubscriptionRequest.RequestOptions} implementation specific to a {@link ChangeStreamRequest}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class ChangeStreamRequestOptions implements SubscriptionRequest.RequestOptions {

		private final @Nullable String databaseName;
		private final @Nullable String collectionName;
		private final ChangeStreamOptions options;

		/**
		 * Create new {@link ChangeStreamRequestOptions}.
		 *
		 * @param collectionName can be {@literal null}.
		 * @param options must not be {@literal null}.
		 */
		public ChangeStreamRequestOptions(@Nullable String databaseName, @Nullable String collectionName,
				ChangeStreamOptions options) {

			Assert.notNull(options, "Options must not be null!");

			this.collectionName = collectionName;
			this.databaseName = databaseName;
			this.options = options;
		}

		public static ChangeStreamRequestOptions of(RequestOptions options) {

			Assert.notNull(options, "Options must not be null!");

			return new ChangeStreamRequestOptions(options.getDatabaseName(), options.getCollectionName(),
					ChangeStreamOptions.builder().build());
		}

		/**
		 * Get the {@link ChangeStreamOptions} defined.
		 *
		 * @return never {@literal null}.
		 */
		public ChangeStreamOptions getChangeStreamOptions() {
			return options;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.monitor.SubscriptionRequest.RequestOptions#getCollectionName()
		 */
		@Override
		public String getCollectionName() {
			return collectionName;
		}

		/*
		 * (non-Javadoc)
		 * @see org.springframework.data.mongodb.monitor.SubscriptionRequest.RequestOptions#getDatabaseName()
		 */
		@Override
		public String getDatabaseName() {
			return databaseName;
		}
	}

	/**
	 * Builder for creating {@link ChangeStreamRequest}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 * @see ChangeStreamOptions
	 */
	public static class ChangeStreamRequestBuilder<T> {

		private @Nullable String databaseName;
		private @Nullable String collectionName;
		private @Nullable MessageListener<ChangeStreamDocument<Document>, ? super T> listener;
		private ChangeStreamOptionsBuilder delegate = ChangeStreamOptions.builder();

		private ChangeStreamRequestBuilder() {}

		/**
		 * Set the name of the {@link com.mongodb.client.MongoDatabase} to listen to.
		 *
		 * @param databaseName must not be {@literal null} nor empty.
		 * @return this.
		 */
		public ChangeStreamRequestBuilder<T> database(String databaseName) {

			Assert.hasText(databaseName, "DatabaseName must not be null!");

			this.databaseName = databaseName;
			return this;
		}

		/**
		 * Set the name of the {@link com.mongodb.client.MongoCollection} to listen to.
		 *
		 * @param collectionName must not be {@literal null} nor empty.
		 * @return this.
		 */
		public ChangeStreamRequestBuilder<T> collection(String collectionName) {

			Assert.hasText(collectionName, "CollectionName must not be null!");

			this.collectionName = collectionName;
			return this;
		}

		/**
		 * Set the {@link MessageListener} event {@link Message messages} will be published to.
		 *
		 * @param messageListener must not be {@literal null}.
		 * @return this.
		 */
		public ChangeStreamRequestBuilder<T> publishTo(
				MessageListener<ChangeStreamDocument<Document>, ? super T> messageListener) {

			Assert.notNull(messageListener, "MessageListener must not be null!");

			this.listener = messageListener;
			return this;
		}

		/**
		 * Set the filter to apply.
		 * <p/>
		 * Fields on aggregation expression root level are prefixed to map to fields contained in
		 * {@link ChangeStreamDocument#getFullDocument() fullDocument}. However {@literal operationType}, {@literal ns},
		 * {@literal documentKey} and {@literal fullDocument} are reserved words that will be omitted, and therefore taken
		 * as given, during the mapping procedure. You may want to have a look at the
		 * <a href="https://docs.mongodb.com/manual/reference/change-events/">structure of Change Events</a>.
		 * <p/>
		 * Use {@link org.springframework.data.mongodb.core.aggregation.TypedAggregation} to ensure filter expressions are
		 * mapped to domain type fields.
		 *
		 * @param aggregation the {@link Aggregation Aggregation pipeline} to apply for filtering events. Must not be
		 *          {@literal null}.
		 * @return this.
		 * @see ChangeStreamOptions#getFilter()
		 * @see ChangeStreamOptionsBuilder#filter(Aggregation)
		 */
		public ChangeStreamRequestBuilder<T> filter(Aggregation aggregation) {

			Assert.notNull(aggregation, "Aggregation must not be null!");

			this.delegate.filter(aggregation);
			return this;
		}

		/**
		 * Set the plain filter chain to apply.
		 *
		 * @param pipeline must not be {@literal null} nor contain {@literal null} values.
		 * @return this.
		 * @see ChangeStreamOptions#getFilter()
		 */
		public ChangeStreamRequestBuilder<T> filter(Document... pipeline) {

			Assert.notNull(pipeline, "Aggregation pipeline must not be null!");
			Assert.noNullElements(pipeline, "Aggregation pipeline must not contain null elements!");

			this.delegate.filter(pipeline);
			return this;
		}

		/**
		 * Set the collation to use.
		 *
		 * @param collation must not be {@literal null} nor {@literal empty}.
		 * @return this.
		 * @see ChangeStreamOptions#getCollation()
		 * @see ChangeStreamOptionsBuilder#collation(Collation)
		 */
		public ChangeStreamRequestBuilder collation(Collation collation) {

			Assert.notNull(collation, "Collation must not be null!");

			this.delegate.collation(collation);
			return this;
		}

		/**
		 * Set the resume token (typically a {@link org.bson.BsonDocument} containing a {@link org.bson.BsonBinary binary
		 * token}) after which to start with listening.
		 *
		 * @param resumeToken must not be {@literal null}.
		 * @return this.
		 * @see ChangeStreamOptions#getResumeToken()
		 * @see ChangeStreamOptionsBuilder#resumeToken(org.bson.BsonValue)
		 */
		public ChangeStreamRequestBuilder<T> resumeToken(BsonValue resumeToken) {

			Assert.notNull(resumeToken, "Resume token not be null!");

			this.delegate.resumeToken(resumeToken);
			return this;
		}

		/**
		 * Set the cluster time at which to resume listening.
		 *
		 * @param clusterTime must not be {@literal null}.
		 * @return this.
		 * @see ChangeStreamOptions#getResumeTimestamp()
		 * @see ChangeStreamOptionsBuilder#resumeAt(java.time.Instant)
		 */
		public ChangeStreamRequestBuilder<T> resumeAt(Instant clusterTime) {

			Assert.notNull(clusterTime, "ClusterTime must not be null!");

			this.delegate.resumeAt(clusterTime);
			return this;
		}

		/**
		 * Set the {@link FullDocument} lookup to {@link FullDocument#UPDATE_LOOKUP}.
		 *
		 * @return this.
		 * @see #fullDocumentLookup(FullDocument)
		 * @see ChangeStreamOptions#getFullDocumentLookup()
		 * @see ChangeStreamOptionsBuilder#fullDocumentLookup(FullDocument)
		 */
		public ChangeStreamRequestBuilder<T> fullDocumentLookup(FullDocument lookup) {

			Assert.notNull(lookup, "FullDocument not be null!");

			this.delegate.fullDocumentLookup(lookup);
			return this;
		}

		/**
		 * @return the build {@link ChangeStreamRequest}.
		 */
		public ChangeStreamRequest<T> build() {

			Assert.notNull(listener, "MessageListener must not be null!");

			return new ChangeStreamRequest<>(listener,
					new ChangeStreamRequestOptions(databaseName, collectionName, delegate.build()));
		}
	}
}
