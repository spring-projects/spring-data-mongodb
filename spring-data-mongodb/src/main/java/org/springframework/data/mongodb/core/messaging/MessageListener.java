/*
 * Copyright 2018-2024 the original author or authors.
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

/**
 * Listener interface to receive delivery of {@link Message Messages}.
 *
 * @author Christoph Strobl
 * @param <S> source message type.
 * @param <T> target message type.
 * @since 2.1
 */
@FunctionalInterface
public interface MessageListener<S, T> {

	/**
	 * Callback invoked on receiving {@link Message}.
	 *
	 * @param message never {@literal null}.
	 */
	void onMessage(Message<S, T> message);
}
