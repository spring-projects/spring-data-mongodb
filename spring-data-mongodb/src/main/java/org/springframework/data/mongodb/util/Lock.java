/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.mongodb.util;

import java.util.concurrent.locks.Condition;
import java.util.function.Supplier;

import org.springframework.util.Assert;

/**
 * {@code Lock} provides more extensive locking operations than can be obtained using {@code synchronized} methods and
 * {@link java.util.concurrent.locks.Lock}. It allows more flexible structuring and may support multiple associated
 * {@link Condition} objects.
 *
 * @author Mark Paluch
 * @since 4.2
 */
public interface Lock {

	/**
	 * Create a new {@link Lock} adapter for the given {@link java.util.concurrent.locks.Lock delegate}.
	 *
	 * @param delegate must not be {@literal  null}.
	 * @return a new {@link Lock} adapter.
	 */
	static Lock of(java.util.concurrent.locks.Lock delegate) {

		Assert.notNull(delegate, "Lock delegate must not be null");

		return new DefaultLock(delegate);
	}

	/**
	 * Acquires the lock.
	 * <p>
	 * If the lock is not available then the current thread becomes disabled for thread scheduling purposes and lies
	 * dormant until the lock has been acquired.
	 *
	 * @see java.util.concurrent.locks.Lock#lock()
	 */
	AcquiredLock lock();

	/**
	 * Execute the action specified by the given callback object guarded by a lock and return its result.
	 *
	 * @param action the action to run.
	 * @return the result of the action.
	 * @param <T> type of the result.
	 * @throws RuntimeException if thrown by the action
	 */
	default <T> T execute(Supplier<T> action) {
		try (AcquiredLock l = lock()) {
			return action.get();
		}
	}

	/**
	 * Execute the action specified by the given callback object guarded by a lock.
	 *
	 * @param action the action to run.
	 * @throws RuntimeException if thrown by the action
	 */
	default void executeWithoutResult(Runnable action) {
		try (AcquiredLock l = lock()) {
			action.run();
		}
	}

	/**
	 * An acquired lock can be used with try-with-resources for easier releasing.
	 */
	interface AcquiredLock extends AutoCloseable {

		/**
		 * Releases the lock.
		 *
		 * @see java.util.concurrent.locks.Lock#unlock()
		 */
		@Override
		void close();
	}

}
