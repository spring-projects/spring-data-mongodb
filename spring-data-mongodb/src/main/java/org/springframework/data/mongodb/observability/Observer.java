/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.mongodb.observability;

import io.micrometer.common.KeyValues;
import io.micrometer.common.docs.KeyName;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import org.jspecify.annotations.Nullable;

/**
 * An observer abstraction that can observe a context and contribute {@literal KeyValue}s for propagation into
 * observability systems.
 *
 * @author Mark Paluch
 * @since 4.4.9
 */
class Observer {

	private final List<MongoKeyName.MongoKeyValue> keyValues = new ArrayList<>();

	/**
	 * Create a new {@link Observer}.
	 *
	 * @return a new {@link Observer}.
	 */
	static Observer create() {
		return new Observer();
	}

	/**
	 * Create a new {@link Observer} given an optional context and a consumer that will contribute key-value tuples from
	 * the given context.
	 *
	 * @param context the context to observe, can be {@literal null}.
	 * @param consumer consumer for a functional declaration that supplies key-value tuples.
	 * @return the stateful {@link Observer}.
	 * @param <C> context type.
	 */
	static <C> Observer fromContext(@Nullable C context, Consumer<? super ContextualObserver<C>> consumer) {

		Observer contributor = create();

		consumer.accept(contributor.contextual(context));

		return contributor;
	}

	/**
	 * Contribute a single {@link MongoKeyName.MongoKeyValue} to the observer.
	 *
	 * @param keyValue
	 * @return
	 */
	Observer contribute(MongoKeyName.MongoKeyValue keyValue) {

		keyValues.add(keyValue);

		return this;
	}

	/**
	 * Create a nested, contextual {@link ContextualObserver} that can contribute key-value tuples based on the given
	 * context.
	 *
	 * @param context the context to observe, can be {@literal null}.
	 * @return the nested contextual {@link ContextualObserver} that can contribute key-value tuples.
	 * @param <C>
	 */
	<C> ContextualObserver<C> contextual(@Nullable C context) {

		if (context == null) {
			return new EmptyContextualObserver<>(keyValues);
		}

		return new DefaultContextualObserver<>(context, keyValues);
	}

	KeyValues toKeyValues() {
		return KeyValues.of(keyValues);
	}

	KeyName[] toKeyNames() {

		KeyName[] keyNames = new KeyName[keyValues.size()];

		for (int i = 0; i < keyValues.size(); i++) {
			MongoKeyName.MongoKeyValue keyValue = keyValues.get(i);
			keyNames[i] = keyValue;
		}

		return keyNames;
	}

	/**
	 * Contextual observer interface to contribute key-value tuples based on a context. The context can be transformed
	 * into a nested context using {@link #nested(Function)}.
	 *
	 * @param <T>
	 */
	interface ContextualObserver<T> {

		/**
		 * Create a nested {@link ContextualObserver} that can contribute key-value tuples based on the transformation of
		 * the current context. If the {@code mapper} function returns {@literal null}, the nested observer will operate
		 * without a context contributing {@literal MonKoKeyName.absent()} values simplifying nullability handling.
		 *
		 * @param mapper context mapper function that transforms the current context into a nested context.
		 * @return the nested contextual observer.
		 * @param <N> nested context type.
		 */
		<N> ContextualObserver<N> nested(Function<? super T, ? extends @Nullable N> mapper);

		/**
		 * Functional-style contribution of a {@link ContextualObserver} callback.
		 *
		 * @param consumer the consumer that will be invoked with this {@link ContextualObserver}.
		 * @return {@code this} {@link ContextualObserver} for further chaining.
		 */
		default ContextualObserver<T> contribute(Consumer<? super ContextualObserver<T>> consumer) {
			consumer.accept(this);
			return this;
		}

		/**
		 * Contribute a {@link MongoKeyName.MongoKeyValue} to the observer.
		 *
		 * @param keyValue
		 * @return {@code this} {@link ContextualObserver} for further chaining.
		 */
		ContextualObserver<T> contribute(MongoKeyName.MongoKeyValue keyValue);

		/**
		 * Contribute a {@link MongoKeyName} to the observer.
		 *
		 * @param keyName
		 * @return {@code this} {@link ContextualObserver} for further chaining.
		 */
		default ContextualObserver<T> contribute(MongoKeyName<T> keyName) {
			return contribute(List.of(keyName));
		}

		/**
		 * Contribute a collection of {@link MongoKeyName}s to the observer.
		 *
		 * @param keyName0
		 * @param keyName1
		 * @return {@code this} {@link ContextualObserver} for further chaining.
		 */
		default ContextualObserver<T> contribute(MongoKeyName<T> keyName0, MongoKeyName<T> keyName1) {
			return contribute(List.of(keyName0, keyName1));
		}

		/**
		 * Contribute a collection of {@link MongoKeyName}s to the observer.
		 *
		 * @param keyName0
		 * @param keyName1
		 * @param keyName2
		 * @return {@code this} {@link ContextualObserver} for further chaining.
		 */
		default ContextualObserver<T> contribute(MongoKeyName<T> keyName0, MongoKeyName<T> keyName1,
				MongoKeyName<T> keyName2) {
			return contribute(List.of(keyName0, keyName1, keyName2));
		}

		/**
		 * Contribute a collection of {@link MongoKeyName}s to the observer.
		 *
		 * @param keyNames
		 * @return {@code this} {@link ContextualObserver} for further chaining.
		 */
		ContextualObserver<T> contribute(Iterable<MongoKeyName<T>> keyNames);

	}

	/**
	 * A default {@link ContextualObserver} that observes a target and contributes key-value tuples by providing the
	 * context to {@link MongoKeyName}.
	 *
	 * @param target
	 * @param keyValues
	 * @param <T>
	 */
	private record DefaultContextualObserver<T>(T target,
			List<MongoKeyName.MongoKeyValue> keyValues) implements ContextualObserver<T> {

		public <N> ContextualObserver<N> nested(Function<? super T, ? extends @Nullable N> mapper) {

			N nestedTarget = mapper.apply(target);

			if (nestedTarget == null) {
				return new EmptyContextualObserver<>(keyValues);
			}

			return new DefaultContextualObserver<>(nestedTarget, keyValues);
		}

		@Override
		public ContextualObserver<T> contribute(MongoKeyName.MongoKeyValue keyValue) {

			keyValues.add(keyValue);

			return this;
		}

		@Override
		public ContextualObserver<T> contribute(MongoKeyName<T> keyName) {

			keyValues.add(keyName.valueOf(target));

			return this;
		}

		@Override
		public ContextualObserver<T> contribute(Iterable<MongoKeyName<T>> keyNames) {

			for (MongoKeyName<T> name : keyNames) {
				keyValues.add(name.valueOf(target));
			}

			return this;
		}

	}

	/**
	 * Empty {@link ContextualObserver} that is not associated with a context and therefore, it only contributes
	 * {@link MongoKeyName#absent()} values.
	 *
	 * @param keyValues
	 * @param <T>
	 */
	private record EmptyContextualObserver<T>(
			List<MongoKeyName.MongoKeyValue> keyValues) implements ContextualObserver<T> {

		public <N> ContextualObserver<N> nested(Function<? super T, ? extends @Nullable N> mapper) {
			return new EmptyContextualObserver<>(keyValues);
		}

		@Override
		public ContextualObserver<T> contribute(MongoKeyName.MongoKeyValue keyValue) {

			keyValues.add(keyValue);

			return this;
		}

		@Override
		public ContextualObserver<T> contribute(Iterable<MongoKeyName<T>> keyNames) {

			for (MongoKeyName<T> name : keyNames) {
				keyValues.add(name.absent());
			}

			return this;
		}

	}

}
