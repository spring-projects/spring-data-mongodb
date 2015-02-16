/*
 * Copyright 2015 the original author or authors.
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
package org.springframework.data.mongodb.repository.query;

import java.io.IOException;
import java.util.Iterator;

import org.springframework.data.mongodb.util.AutoCloseableIterator;
import org.springframework.data.mongodb.util.CloseableIterator;

/**
 * An {@link CloseableIterator} that can be used with TRW in Java 7.
 * 
 * @author Thomas Darimont
 * @param <T>
 */
class ForwardingAutoCloseableIterator<T> implements AutoCloseableIterator<T> {

	private final CloseableIterator<T> target;

	public ForwardingAutoCloseableIterator(CloseableIterator<T> target) {
		this.target = target;
	}

	@Override
	public boolean hasNext() {
		return target.hasNext();
	}

	@Override
	public T next() {
		return target.next();
	}

	@Override
	public void close() throws IOException {
		this.target.close();
	}

	@Override
	public Iterator<T> iterator() {
		return this;
	}
}
