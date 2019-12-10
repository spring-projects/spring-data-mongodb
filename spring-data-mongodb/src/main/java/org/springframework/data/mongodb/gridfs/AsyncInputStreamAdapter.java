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
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Operators;
import reactor.util.concurrent.Queues;
import reactor.util.context.Context;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferUtils;

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

	private volatile Subscription subscription;
	private volatile boolean cancelled;
	private volatile boolean allDataBuffersReceived;
	private volatile Throwable error;
	private final Queue<ReadRequest> readRequests = Queues.<ReadRequest> small().get();

	private final Queue<DataBuffer> bufferQueue = Queues.<DataBuffer> small().get();

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

		return Flux.create(sink -> {

			readRequests.offer(new ReadRequest(sink, dst));

			sink.onCancel(this::terminatePendingReads);
			sink.onDispose(this::terminatePendingReads);
			sink.onRequest(this::request);
		});
	}

	void onError(FluxSink<Integer> sink, Throwable e) {

		readRequests.poll();
		sink.error(e);
	}

	void onComplete(FluxSink<Integer> sink, int writtenBytes) {

		readRequests.poll();
		DEMAND.decrementAndGet(this);
		sink.next(writtenBytes);
		sink.complete();
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
	public Publisher<Void> close() {

		return Mono.create(sink -> {

			cancelled = true;

			if (error != null) {
				terminatePendingReads();
				sink.error(error);
				return;
			}

			terminatePendingReads();
			sink.success();
		});
	}

	protected void request(long n) {

		if (allDataBuffersReceived && bufferQueue.isEmpty()) {

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

		if (cancelled) {
			subscription.cancel();
		}

		drainLoop();
	}

	void drainLoop() {

		while (DEMAND.get(AsyncInputStreamAdapter.this) > 0) {

			DataBuffer wip = bufferQueue.peek();

			if (wip == null) {
				break;
			}

			if (wip.readableByteCount() == 0) {
				bufferQueue.poll();
				continue;
			}

			ReadRequest consumer = AsyncInputStreamAdapter.this.readRequests.peek();
			if (consumer == null) {
				break;
			}

			consumer.transferBytes(wip, wip.readableByteCount());
		}

		if (bufferQueue.isEmpty()) {

			if (allDataBuffersReceived) {
				terminatePendingReads();
				return;
			}

			if (demand > 0) {
				subscription.request(1);
			}
		}
	}

	/**
	 * Terminates pending reads with empty buffers.
	 */
	void terminatePendingReads() {

		ReadRequest readers;

		while ((readers = readRequests.poll()) != null) {
			readers.onComplete();
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
			s.request(1);
		}

		@Override
		public void onNext(DataBuffer dataBuffer) {

			if (cancelled || allDataBuffersReceived) {
				DataBufferUtils.release(dataBuffer);
				Operators.onNextDropped(dataBuffer, AsyncInputStreamAdapter.this.subscriberContext);
				return;
			}

			ReadRequest readRequest = AsyncInputStreamAdapter.this.readRequests.peek();

			if (readRequest == null) {

				DataBufferUtils.release(dataBuffer);
				Operators.onNextDropped(dataBuffer, AsyncInputStreamAdapter.this.subscriberContext);
				subscription.cancel();
				return;
			}

			bufferQueue.offer(dataBuffer);

			drainLoop();
		}

		@Override
		public void onError(Throwable t) {

			if (AsyncInputStreamAdapter.this.cancelled || AsyncInputStreamAdapter.this.allDataBuffersReceived) {
				Operators.onErrorDropped(t, AsyncInputStreamAdapter.this.subscriberContext);
				return;
			}

			AsyncInputStreamAdapter.this.error = t;
			AsyncInputStreamAdapter.this.allDataBuffersReceived = true;
			terminatePendingReads();
		}

		@Override
		public void onComplete() {

			AsyncInputStreamAdapter.this.allDataBuffersReceived = true;
			if (bufferQueue.isEmpty()) {
				terminatePendingReads();
			}
		}
	}

	/**
	 * Request to read bytes and transfer these to the associated {@link ByteBuffer}.
	 */
	class ReadRequest {

		private final FluxSink<Integer> sink;
		private final ByteBuffer dst;

		private int writtenBytes;

		ReadRequest(FluxSink<Integer> sink, ByteBuffer dst) {
			this.sink = sink;
			this.dst = dst;
			this.writtenBytes = -1;
		}

		public void onComplete() {

			if (error != null) {
				AsyncInputStreamAdapter.this.onError(sink, error);
				return;
			}

			AsyncInputStreamAdapter.this.onComplete(sink, writtenBytes);
		}

		public void transferBytes(DataBuffer db, int bytes) {

			try {

				if (error != null) {
					AsyncInputStreamAdapter.this.onError(sink, error);
					return;
				}

				ByteBuffer byteBuffer = db.asByteBuffer();
				int remaining = byteBuffer.remaining();
				int writeCapacity = Math.min(dst.remaining(), remaining);
				int limit = Math.min(byteBuffer.position() + writeCapacity, byteBuffer.capacity());
				int toWrite = limit - byteBuffer.position();

				if (toWrite == 0) {

					AsyncInputStreamAdapter.this.onComplete(sink, writtenBytes);
					return;
				}

				int oldPosition = byteBuffer.position();

				byteBuffer.limit(toWrite);
				dst.put(byteBuffer);
				byteBuffer.limit(byteBuffer.capacity());
				byteBuffer.position(oldPosition);
				db.readPosition(db.readPosition() + toWrite);

				if (writtenBytes == -1) {
					writtenBytes = bytes;
				} else {
					writtenBytes += bytes;
				}

			} catch (Exception e) {
				AsyncInputStreamAdapter.this.onError(sink, e);
			} finally {

				if (db.readableByteCount() == 0) {
					DataBufferUtils.release(db);
				}
			}
		}
	}
}
