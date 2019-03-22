/*
 * Copyright 2019 the original author or authors.
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
package org.springframework.data.mongodb.gridfs;

import lombok.RequiredArgsConstructor;
import reactor.core.CoreSubscriber;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.function.BiConsumer;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;
import org.springframework.core.io.buffer.DefaultDataBufferFactory;

import com.mongodb.reactivestreams.client.Success;
import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;

/**
 * Adapter accepting a binary stream {@link Publisher} and emitting its through {@link AsyncInputStream}.
 * <p>
 * This adapter subscribes to the binary {@link Publisher} as soon as the first chunk gets {@link #read(ByteBuffer)
 * requested}. Requests are queued and binary chunks are requested from the {@link Publisher}. As soon as the
 * {@link Publisher} emits items, chunks are provided to the read request which completes the number-of-written-bytes
 * {@link Publisher}.
 * <p>
 * {@link AsyncInputStream} is supposed to work as sequential callback API that is called until reaching EOF.
 * {@link #close()} is propagated as cancellation signal to the binary {@link Publisher}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
@RequiredArgsConstructor
class AsyncInputStreamAdapter implements AsyncInputStream {

	private static final AtomicLongFieldUpdater<AsyncInputStreamAdapter> DEMAND = AtomicLongFieldUpdater
			.newUpdater(AsyncInputStreamAdapter.class, "demand");

	private static final AtomicIntegerFieldUpdater<AsyncInputStreamAdapter> SUBSCRIBED = AtomicIntegerFieldUpdater
			.newUpdater(AsyncInputStreamAdapter.class, "subscribed");

	private static final int SUBSCRIPTION_NOT_SUBSCRIBED = 0;
	private static final int SUBSCRIPTION_SUBSCRIBED = 1;

	private final Publisher<? extends DataBuffer> buffers;
	private final Context subscriberContext;
	private final DefaultDataBufferFactory factory = new DefaultDataBufferFactory();

	private volatile Subscription subscription;
	private volatile boolean cancelled;
	private volatile boolean complete;
	private volatile Throwable error;
	private final Queue<BiConsumer<DataBuffer, Integer>> readRequests = Queues.<BiConsumer<DataBuffer, Integer>> small()
			.get();

	// see DEMAND
	volatile long demand;

	// see SUBSCRIBED
	volatile int subscribed = SUBSCRIPTION_NOT_SUBSCRIBED;

	/*
	 * (non-Javadoc)
	 * @see com.mongodb.reactivestreams.client.gridfs.AsyncInputStream#read(java.nio.ByteBuffer)
	 */
	@Override
	public Publisher<Integer> read(ByteBuffer dst) {

		return Mono.create(sink -> {

			readRequests.offer((db, bytecount) -> {

				try {

					if (error != null) {

						sink.error(error);
						return;
					}

					if (bytecount == -1) {

						sink.success(-1);
						return;
					}

					ByteBuffer byteBuffer = db.asByteBuffer();
					int toWrite = byteBuffer.remaining();

					dst.put(byteBuffer);
					sink.success(toWrite);

				} catch (Exception e) {
					sink.error(e);
				} finally {
					DataBufferUtils.release(db);
				}
			});

			request(1);
		});
	}

	/*
	 * (non-Javadoc)
	 * @see com.mongodb.reactivestreams.client.gridfs.AsyncInputStream#skip(long)
	 */
	@Override
	public Publisher<Long> skip(long bytesToSkip) {
		throw new UnsupportedOperationException("Skip is currently not implemented");
	}

	/*
	 * (non-Javadoc)
	 * @see com.mongodb.reactivestreams.client.gridfs.AsyncInputStream#close()
	 */
	@Override
	public Publisher<Success> close() {

		return Mono.create(sink -> {

			cancelled = true;

			if (error != null) {
				sink.error(error);
				return;
			}

			sink.success(Success.SUCCESS);
		});
	}

	protected void request(int n) {

		if (complete) {

			terminatePendingReads();
			return;
		}

		Operators.addCap(DEMAND, this, n);

		if (SUBSCRIBED.get(this) == SUBSCRIPTION_NOT_SUBSCRIBED) {

			if (SUBSCRIBED.compareAndSet(this, SUBSCRIPTION_NOT_SUBSCRIBED, SUBSCRIPTION_SUBSCRIBED)) {
				buffers.subscribe(new DataBufferCoreSubscriber());
			}

		} else {

			Subscription subscription = this.subscription;

			if (subscription != null) {
				requestFromSubscription(subscription);
			}
		}
	}

	void requestFromSubscription(Subscription subscription) {

		long demand = DEMAND.get(AsyncInputStreamAdapter.this);

		if (cancelled) {
			subscription.cancel();
		}

		if (demand > 0 && DEMAND.compareAndSet(AsyncInputStreamAdapter.this, demand, demand - 1)) {
			subscription.request(1);
		}
	}

	/**
	 * Terminates pending reads with empty buffers.
	 */
	void terminatePendingReads() {

		BiConsumer<DataBuffer, Integer> readers;

		while ((readers = readRequests.poll()) != null) {
			readers.accept(factory.wrap(new byte[0]), -1);
		}
	}

	private class DataBufferCoreSubscriber implements CoreSubscriber<DataBuffer> {

		@Override
		public Context currentContext() {
			return AsyncInputStreamAdapter.this.subscriberContext;
		}

		@Override
		public void onSubscribe(Subscription s) {

			AsyncInputStreamAdapter.this.subscription = s;

			Operators.addCap(DEMAND, AsyncInputStreamAdapter.this, -1);
			s.request(1);
		}

		@Override
		public void onNext(DataBuffer dataBuffer) {

			if (cancelled || complete) {
				DataBufferUtils.release(dataBuffer);
				Operators.onNextDropped(dataBuffer, AsyncInputStreamAdapter.this.subscriberContext);
				return;
			}

			BiConsumer<DataBuffer, Integer> poll = AsyncInputStreamAdapter.this.readRequests.poll();

			if (poll == null) {

				DataBufferUtils.release(dataBuffer);
				Operators.onNextDropped(dataBuffer, AsyncInputStreamAdapter.this.subscriberContext);
				subscription.cancel();
				return;
			}

			poll.accept(dataBuffer, dataBuffer.readableByteCount());

			requestFromSubscription(subscription);
		}

		@Override
		public void onError(Throwable t) {

			if (AsyncInputStreamAdapter.this.cancelled || AsyncInputStreamAdapter.this.complete) {
				Operators.onErrorDropped(t, AsyncInputStreamAdapter.this.subscriberContext);
				return;
			}

			AsyncInputStreamAdapter.this.error = t;
			AsyncInputStreamAdapter.this.complete = true;
			terminatePendingReads();
		}

		@Override
		public void onComplete() {

			AsyncInputStreamAdapter.this.complete = true;
			terminatePendingReads();
		}
	}
}
