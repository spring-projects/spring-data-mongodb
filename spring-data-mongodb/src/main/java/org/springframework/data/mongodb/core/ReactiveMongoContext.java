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

import org.reactivestreams.Publisher;
import org.springframework.util.Assert;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;

import com.mongodb.reactivestreams.client.ClientSession;

/**
 * {@link ReactiveMongoContext} utilizes and enriches the Reactor {@link Context} with information potentially required
 * for e.g. {@link ClientSession} handling and transactions.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 * @see Mono#subscriberContext()
 * @see Context
 */
public class ReactiveMongoContext {

	private static final Class<?> SESSION_KEY = ClientSession.class;

	/**
	 * Gets the {@code Mono<ClientSession>} from Reactor {@link reactor.util.context.Context}. The resulting {@link Mono}
	 * emits the {@link ClientSession} if a session is associated with the current {@link reactor.util.context.Context
	 * subscriber context}. If the context does not contain a session, the resulting {@link Mono} terminates empty (i.e.
	 * without emitting a value).
	 *
	 * @return the {@link Mono} emitting the client session if present; otherwise the {@link Mono} terminates empty.
	 */
	public static Mono<ClientSession> getSession() {

		return Mono.subscriberContext().filter(ctx -> ctx.hasKey(SESSION_KEY))
				.flatMap(ctx -> ctx.<Mono<ClientSession>> get(SESSION_KEY));
	}

	/**
	 * Sets the {@link ClientSession} into the Reactor {@link reactor.util.context.Context}.
	 *
	 * @param context must not be {@literal null}.
	 * @param session must not be {@literal null}.
	 * @return a new {@link Context}.
	 * @see Context#put(Object, Object)
	 */
	public static Context setSession(Context context, Publisher<ClientSession> session) {

		Assert.notNull(context, "Context must not be null!");
		Assert.notNull(session, "Session publisher must not be null!");

		return context.put(SESSION_KEY, Mono.from(session));
	}
}
