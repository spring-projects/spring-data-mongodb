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

import org.springframework.data.mongodb.util.Lock.AcquiredLock;

/**
 * Default {@link Lock} implementation.
 *
 * @author Mark Paluch
 * @since 3.2
 */
class DefaultLock implements Lock, AcquiredLock {

	private final java.util.concurrent.locks.Lock delegate;

	DefaultLock(java.util.concurrent.locks.Lock delegate) {
		this.delegate = delegate;
	}

	@Override
	public AcquiredLock lock() {
		delegate.lock();
		return this;
	}

	@Override
	public void close() {
		delegate.unlock();
	}
}
