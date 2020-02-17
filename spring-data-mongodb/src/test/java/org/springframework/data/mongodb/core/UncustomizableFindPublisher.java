/*
 * Copyright 2020 the original author or authors.
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
package org.springframework.data.mongodb.core;

import com.mongodb.CursorType;
import com.mongodb.reactivestreams.client.FindPublisher;
import lombok.RequiredArgsConstructor;
import org.bson.conversions.Bson;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import java.util.concurrent.TimeUnit;

/**
 * {@link FindPublisher} implementation which does nothing in all FindPublisher's eigen methods, but delegates
 * {@link #subscribe(Subscriber)} call to a {@link Publisher} passed in the constructor.
 *
 * @author Roman Puchkovskiy
 */
@RequiredArgsConstructor
class UncustomizableFindPublisher<T> implements FindPublisher<T> {
	private final Publisher<T> realPublisher;

	@Override
	public Publisher<T> first() {
		return this;
	}

	@Override
	public FindPublisher<T> filter(Bson bson) {
		return this;
	}

	@Override
	public FindPublisher<T> limit(int i) {
		return this;
	}

	@Override
	public FindPublisher<T> skip(int i) {
		return this;
	}

	@Override
	public FindPublisher<T> maxTime(long l, TimeUnit timeUnit) {
		return this;
	}

	@Override
	public FindPublisher<T> maxAwaitTime(long l, TimeUnit timeUnit) {
		return this;
	}

	@Override
	public FindPublisher<T> projection(Bson bson) {
		return this;
	}

	@Override
	public FindPublisher<T> sort(Bson bson) {
		return this;
	}

	@Override
	public FindPublisher<T> noCursorTimeout(boolean b) {
		return this;
	}

	@Override
	public FindPublisher<T> oplogReplay(boolean b) {
		return this;
	}

	@Override
	public FindPublisher<T> partial(boolean b) {
		return this;
	}

	@Override
	public FindPublisher<T> cursorType(CursorType cursorType) {
		return this;
	}

	@Override
	public FindPublisher<T> collation(com.mongodb.client.model.Collation collation) {
		return this;
	}

	@Override
	public FindPublisher<T> comment(String s) {
		return this;
	}

	@Override
	public FindPublisher<T> hint(Bson bson) {
		return this;
	}

	@Override
	public FindPublisher<T> hintString(String s) {
		return this;
	}

	@Override
	public FindPublisher<T> max(Bson bson) {
		return this;
	}

	@Override
	public FindPublisher<T> min(Bson bson) {
		return this;
	}

	@Override
	public FindPublisher<T> returnKey(boolean b) {
		return this;
	}

	@Override
	public FindPublisher<T> showRecordId(boolean b) {
		return this;
	}

	@Override
	public FindPublisher<T> batchSize(int i) {
		return this;
	}

	@Override
	public void subscribe(Subscriber<? super T> subscriber) {
		realPublisher.subscribe(subscriber);
	}
}
