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

import org.springframework.context.SmartLifecycle;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.messaging.SubscriptionRequest.RequestOptions;
import org.springframework.util.ErrorHandler;

/**
 * Internal abstraction used by the framework representing a message listener container. <strong>Not</strong> meant to
 * be implemented externally.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public interface MessageListenerContainer extends SmartLifecycle {

	/**
	 * Create a new {@link MessageListenerContainer} given {@link MongoTemplate}.
	 *
	 * @param template must not be {@literal null}.
	 * @return a new {@link MessageListenerContainer} using {@link MongoTemplate}.
	 */
	static MessageListenerContainer create(MongoTemplate template) {
		return new DefaultMessageListenerContainer(template);
	}

	/**
	 * Register a new {@link SubscriptionRequest} in the container. If the {@link MessageListenerContainer#isRunning() is
	 * already running} the {@link Subscription} will be added and run immediately, otherwise it'll be scheduled and
	 * started once the container is actually {@link MessageListenerContainer#start() started}.
	 *
	 * <pre>
	 * <code>
	 *     MessageListenerContainer container = ...
	 *
	 *     MessageListener<ChangeStreamDocument<Document>, Object> messageListener = (message) -> message....
	 *     ChangeStreamRequest<Object> request = new ChangeStreamRequest<>(messageListener, () -> "collection-name");
	 *
	 *     Subscription subscription = container.register(request);
	 * </code>
	 * </pre>
	 *
	 * Errors during {@link Message} retrieval lead to {@link Subscription#cancel() cannelation} of the underlying task.
	 *
	 * @param request must not be {@literal null}.
	 * @return never {@literal null}.
	 */
	default <T> Subscription register(SubscriptionRequest<T, Object, ? extends RequestOptions> request) {
		return register(request, Object.class);
	}

	/**
	 * Register a new {@link SubscriptionRequest} in the container. If the {@link MessageListenerContainer#isRunning() is
	 * already running} the {@link Subscription} will be added and run immediately, otherwise it'll be scheduled and
	 * started once the container is actually {@link MessageListenerContainer#start() started}.
	 *
	 * <pre>
	 * <code>
	 *     MessageListenerContainer container = ...
	 *
	 *     MessageListener<ChangeStreamDocument<Document>, Document> messageListener = (message) -> message.getBody().toJson();
	 *     ChangeStreamRequest<Document> request = new ChangeStreamRequest<>(messageListener, () -> "collection-name");
	 *
	 *     Subscription subscription = container.register(request, Document.class);
	 * </code>
	 * </pre>
	 *
	 * On {@link MessageListenerContainer#stop()} all {@link Subscription subscriptions} are cancelled prior to shutting
	 * down the container itself.
	 * <p />
	 * Registering the very same {@link SubscriptionRequest} more than once simply returns the already existing
	 * {@link Subscription}.
	 * <p />
	 * Unless a {@link Subscription} is {@link #remove(Subscription) removed} form the container, the {@link Subscription}
	 * is restarted once the container itself is restarted.
	 * <p />
	 * Errors during {@link Message} retrieval lead to {@link Subscription#cancel() cannelation} of the underlying task.
	 *
	 * @param request must not be {@literal null}.
	 * @param type the exact target or a more concrete type of the {@link Message#getBody()}.
	 * @return never {@literal null}.
	 */
	<S, T> Subscription register(SubscriptionRequest<S, ? super T, ? extends RequestOptions> request, Class<T> bodyType);

	/**
	 * Register a new {@link SubscriptionRequest} in the container. If the {@link MessageListenerContainer#isRunning() is
	 * already running} the {@link Subscription} will be added and run immediately, otherwise it'll be scheduled and
	 * started once the container is actually {@link MessageListenerContainer#start() started}.
	 *
	 * <pre>
	 * <code>
	 *     MessageListenerContainer container = ...
	 *
	 *     MessageListener<ChangeStreamDocument<Document>, Document> messageListener = (message) -> message.getBody().toJson();
	 *     ChangeStreamRequest<Document> request = new ChangeStreamRequest<>(messageListener, () -> "collection-name");
	 *
	 *     Subscription subscription = container.register(request, Document.class);
	 * </code>
	 * </pre>
	 *
	 * On {@link MessageListenerContainer#stop()} all {@link Subscription subscriptions} are cancelled prior to shutting
	 * down the container itself.
	 * <p />
	 * Registering the very same {@link SubscriptionRequest} more than once simply returns the already existing
	 * {@link Subscription}.
	 * <p />
	 * Unless a {@link Subscription} is {@link #remove(Subscription) removed} form the container, the {@link Subscription}
	 * is restarted once the container itself is restarted.
	 * <p />
	 * Errors during {@link Message} retrieval are delegated to the given {@link ErrorHandler}.
	 *
	 * @param request must not be {@literal null}.
	 * @param type the exact target or a more concrete type of the {@link Message#getBody()}. Must not be {@literal null}.
	 * @param errorHandler the callback to invoke when retrieving the {@link Message} from the data source fails for some
	 *          reason.
	 * @return never {@literal null}.
	 */
	<S, T> Subscription register(SubscriptionRequest<S, ? super T, ? extends RequestOptions> request, Class<T> bodyType,
			ErrorHandler errorHandler);

	/**
	 * Unregister a given {@link Subscription} from the container. This prevents the {@link Subscription} to be restarted
	 * in a potential {@link SmartLifecycle#stop() stop}/{@link SmartLifecycle#start() start} scenario.<br />
	 * An {@link Subscription#isActive() active} {@link Subscription subcription} is {@link Subscription#cancel()
	 * cancelled} prior to removal.
	 *
	 * @param subscription must not be {@literal null}.
	 */
	void remove(Subscription subscription);

	/**
	 * Lookup the given {@link SubscriptionRequest} within the container and return the associated {@link Subscription} if
	 * present.
	 *
	 * @param request must not be {@literal null}.
	 * @return {@link Optional#empty()} if not set.
	 */
	Optional<Subscription> lookup(SubscriptionRequest<?, ?, ?> request);
}
