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

import org.bson.Document;
import org.springframework.data.mongodb.core.ChangeStreamRequest.ChangeStreamRequestOptions;
import org.springframework.util.Assert;

import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;

/**
 * {@link SubscriptionRequest} implementation to be used for listening to
 * <a href="https://docs.mongodb.com/manual/changeStreams/">Change Streams</a> via a {@link MessageListenerContainer}
 * using the synchronous MongoDB Java driver.
 * <p/>
 * The most trivial use case is subscribing to all events of a specific {@link com.mongodb.client.MongoCollection
 * collection}.
 * 
 * <pre>
 * <code>
 *     ChangeStreamRequest<Document> request = new ChangeStreamRequest<>(System.out::println, () -> "collection-name");
 * </code>
 * </pre>
 * 
 * For more advanced scenarios {@link ChangeStreamOptions} offers abstractions for options like filtering, resuming,...
 * 
 * <pre>
 * <code>
 *     ChangeStreamOptions options = ChangeStreamOptions.builder()
 *     		.returnFullDocumentOnUpdate()
 *     		.build();
 *     	
 *     ChangeStreamRequest<Document> request = new ChangeStreamRequest<>(System.out::println, new ChangeStreamRequestOptions("collection-name", options));
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
 * @since 2.1
 */
public class ChangeStreamRequest<T>
		implements SubscriptionRequest<Message<ChangeStreamDocument<Document>, T>, ChangeStreamRequestOptions> {

	private final MessageListener<Message<ChangeStreamDocument<Document>, T>> messageListener;
	private final ChangeStreamRequestOptions options;

	/**
	 * Create a new {@link ChangeStreamRequest} with options, passing {@link Message messages} to the given
	 * {@link MessageListener}.
	 *
	 * @param messageListener must not be {@literal null}.
	 * @param options must not be {@literal null}.
	 */
	public ChangeStreamRequest(MessageListener<Message<ChangeStreamDocument<Document>, T>> messageListener,
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
	public MessageListener<Message<ChangeStreamDocument<Document>, T>> getMessageListener() {
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
	 * {@link SubscriptionRequest.RequestOptions} implementation specific to a {@link ChangeStreamRequest}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	public static class ChangeStreamRequestOptions implements SubscriptionRequest.RequestOptions {

		private final String collectionName;
		private final ChangeStreamOptions options;

		/**
		 * Create new {@link ChangeStreamRequestOptions}.
		 *
		 * @param collectionName must not be {@literal null}.
		 * @param options must not be {@literal null}.
		 */
		public ChangeStreamRequestOptions(String collectionName, ChangeStreamOptions options) {

			Assert.notNull(collectionName, "CollectionName must not be null!");
			Assert.notNull(options, "Options must not be null!");

			this.collectionName = collectionName;
			this.options = options;
		}

		static ChangeStreamRequestOptions of(RequestOptions options) {

			Assert.notNull(options, "Options must not be null!");

			return new ChangeStreamRequestOptions(options.getCollectionName(), ChangeStreamOptions.builder().build());
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
	}
}
