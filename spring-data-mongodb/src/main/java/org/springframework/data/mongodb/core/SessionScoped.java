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

import java.util.function.Consumer;

import org.springframework.lang.Nullable;

import com.mongodb.client.ClientSession;

/**
 * Gateway interface to execute {@link ClientSession} bound operations against MongoDB via a {@link SessionCallback}.
 * <p />
 * The very same bound {@link ClientSession} is used for all invocations of {@code execute} on the instance.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 2.1
 */
public interface SessionScoped {

	/**
	 * Executes the given {@link SessionCallback} within the {@link com.mongodb.session.ClientSession}.
	 * <p/>
	 * It is up to the caller to make sure the {@link com.mongodb.session.ClientSession} is {@link ClientSession#close()
	 * closed} when done.
	 *
	 * @param action callback object that specifies the MongoDB action the callback action. Must not be {@literal null}.
	 * @param <T> return type.
	 * @return a result object returned by the action. Can be {@literal null}.
	 */
	@Nullable
	default <T> T execute(SessionCallback<T> action) {
		return execute(action, session -> {});
	}

	/**
	 * Executes the given {@link SessionCallback} within the {@link com.mongodb.session.ClientSession}.
	 * <p/>
	 * It is up to the caller to make sure the {@link com.mongodb.session.ClientSession} is {@link ClientSession#close()
	 * closed} when done.
	 *
	 * @param action callback object that specifies the MongoDB action the callback action. Must not be {@literal null}.
	 * @param doFinally callback object that accepts {@link ClientSession} after invoking {@link SessionCallback}. This
	 *          {@link Consumer} is guaranteed to be notified in any case (successful and exceptional outcome of
	 *          {@link SessionCallback}).
	 * @param <T> return type.
	 * @return a result object returned by the action. Can be {@literal null}.
	 */
	@Nullable
	<T> T execute(SessionCallback<T> action, Consumer<ClientSession> doFinally);
}
