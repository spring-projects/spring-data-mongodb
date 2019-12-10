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
import reactor.util.context.Context;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import org.springframework.core.io.buffer.DataBuffer;
import org.springframework.core.io.buffer.DataBufferFactory;

import com.mongodb.reactivestreams.client.gridfs.AsyncInputStream;

/**
 * Utility to adapt a {@link AsyncInputStream} to a {@link Publisher} emitting {@link DataBuffer}.
 *
 * @author Mark Paluch
 * @author Christoph Strobl
 * @since 2.2
 */
class DataBufferPublisherAdapter {

	/**
	 * Creates a {@link Publisher} emitting {@link DataBuffer}s by reading binary chunks from {@link AsyncInputStream}.
	 * Closes the {@link AsyncInputStream} once the {@link Publisher} terminates.
	 *
	 * @param inputStream must not be {@literal null}.
	 * @param dataBufferFactory must not be {@literal null}.
	 * @param bufferSize read {@code n} bytes per iteration.
	 * @return the resulting {@link Publisher}.
	 */
	static Flux<DataBuffer> createBinaryStream(AsyncInputStream inputStream, DataBufferFactory dataBufferFactory,
			int bufferSize) {

		return Flux.usingWhen(Mono.just(new DelegatingAsyncInputStream(inputStream, dataBufferFactory, bufferSize)),
				DataBufferPublisherAdapter::doRead, AsyncInputStream::close, (it, err) -> it.close(), AsyncInputStream::close);
	}

	/**
	 * Use an {@link AsyncInputStreamHandler} to read data from the given {@link AsyncInputStream}.
	 *
	 * @param inputStream the source stream.
	 * @return a {@link Flux} emitting data chunks one by one.
	 * @since 2.2.1
	 */
	private static Flux<DataBuffer> doRead(DelegatingAsyncInputStream inputStream) {

		AsyncInputStreamHandler streamHandler = new AsyncInputStreamHandler(inputStream, inputStream.dataBufferFactory,
				inputStream.bufferSize);
		return Flux.create((sink) -> {

			sink.onDispose(streamHandler::close);
			sink.onCancel(streamHandler::close);

			sink.onRequest(n -> {
				streamHandler.request(sink, n);
			});
		});
	}

	/**
	 * An {@link AsyncInputStream} also holding a {@link DataBufferFactory} and default {@literal bufferSize} for reading
	 * from it, delegating operations on the {@link AsyncInputStream} to the reference instance. <br />
	 * Used to pass on the {@link AsyncInputStream} and parameters to avoid capturing lambdas.
	 *
	 * @author Christoph Strobl
	 * @since 2.2.1
	 */
	private static class DelegatingAsyncInputStream implements AsyncInputStream {

		private final AsyncInputStream inputStream;
		private final DataBufferFactory dataBufferFactory;
		private int bufferSize;

		/**
		 * @param inputStream the source input stream.
		 * @param dataBufferFactory
		 * @param bufferSize
		 */
		DelegatingAsyncInputStream(AsyncInputStream inputStream, DataBufferFactory dataBufferFactory, int bufferSize) {

			this.inputStream = inputStream;
			this.dataBufferFactory = dataBufferFactory;
			this.bufferSize = bufferSize;
		}

		/*
		 * (non-Javadoc)
		 * @see com.mongodb.reactivestreams.client.gridfs.AsyncInputStream#read(java.nio.ByteBuffer)
		 */
		@Override
		public Publisher<Integer> read(ByteBuffer dst) {
			return inputStream.read(dst);
		}

		/*
		 * (non-Javadoc)
		 * @see com.mongodb.reactivestreams.client.gridfs.AsyncInputStream#skip(long)
		 */
		@Override
		public Publisher<Long> skip(long bytesToSkip) {
			return inputStream.skip(bytesToSkip);
		}

		/*
		 * (non-Javadoc)
		 * @see com.mongodb.reactivestreams.client.gridfs.AsyncInputStream#close()
		 */
		@Override
		public Publisher<Void> close() {
			return inputStream.close();
		}
	}

	@RequiredArgsConstructor
	static class AsyncInputStreamHandler {

		private static final AtomicLongFieldUpdater<AsyncInputStreamHandler> DEMAND = AtomicLongFieldUpdater
				.newUpdater(AsyncInputStreamHandler.class, "demand");

		private static final AtomicIntegerFieldUpdater<AsyncInputStreamHandler> STATE = AtomicIntegerFieldUpdater
				.newUpdater(AsyncInputStreamHandler.class, "state");

		private static final AtomicIntegerFieldUpdater<AsyncInputStreamHandler> DRAIN = AtomicIntegerFieldUpdater
				.newUpdater(AsyncInputStreamHandler.class, "drain");

		private static final AtomicIntegerFieldUpdater<AsyncInputStreamHandler> READ = AtomicIntegerFieldUpdater
				.newUpdater(AsyncInputStreamHandler.class, "read");

		private static final int STATE_OPEN = 0;
		private static final int STATE_CLOSED = 1;

		private static final int DRAIN_NONE = 0;
		private static final int DRAIN_COMPLETION = 1;

		private static final int READ_NONE = 0;
		private static final int READ_IN_PROGRESS = 1;

		final AsyncInputStream inputStream;
		final DataBufferFactory dataBufferFactory;
		final int bufferSize;

		// see DEMAND
		volatile long demand;

		// see STATE
		volatile int state = STATE_OPEN;

		// see DRAIN
		volatile int drain = DRAIN_NONE;

		// see READ_IN_PROGRESS
		volatile int read = READ_NONE;

		void request(FluxSink<DataBuffer> sink, long n) {

			Operators.addCap(DEMAND, this, n);
			drainLoop(sink);
		}

		/**
		 * Loops while we have demand and while no read is in progress.
		 *
		 * @param sink
		 */
		void drainLoop(FluxSink<DataBuffer> sink) {
			while (onShouldRead()) {
				emitNext(sink);
			}
		}

		boolean onShouldRead() {
			return !isClosed() && getDemand() > 0 && onWantRead();
		}

		boolean onWantRead() {
			return READ.compareAndSet(this, READ_NONE, READ_IN_PROGRESS);
		}

		void onReadDone() {
			READ.compareAndSet(this, READ_IN_PROGRESS, READ_NONE);
		}

		long getDemand() {
			return DEMAND.get(this);
		}

		void decrementDemand() {
			DEMAND.decrementAndGet(this);
		}

		void close() {
			STATE.compareAndSet(this, STATE_OPEN, STATE_CLOSED);
		}

		boolean enterDrainLoop() {
			return DRAIN.compareAndSet(this, DRAIN_NONE, DRAIN_COMPLETION);
		}

		void leaveDrainLoop() {
			DRAIN.set(this, DRAIN_NONE);
		}

		boolean isClosed() {
			return STATE.get(this) == STATE_CLOSED;
		}

		/**
		 * Emit the next {@link DataBuffer}.
		 *
		 * @param sink
		 * @return
		 */
		private void emitNext(FluxSink<DataBuffer> sink) {

			ByteBuffer transport = ByteBuffer.allocate(bufferSize);
			BufferCoreSubscriber bufferCoreSubscriber = new BufferCoreSubscriber(sink, dataBufferFactory, transport);
			try {
				inputStream.read(transport).subscribe(bufferCoreSubscriber);
			} catch (Throwable e) {
				sink.error(e);
			}
		}

		private class BufferCoreSubscriber implements CoreSubscriber<Integer> {

			private final FluxSink<DataBuffer> sink;
			private final DataBufferFactory factory;
			private final ByteBuffer transport;
			private volatile Subscription subscription;

			BufferCoreSubscriber(FluxSink<DataBuffer> sink, DataBufferFactory factory, ByteBuffer transport) {

				this.sink = sink;
				this.factory = factory;
				this.transport = transport;
			}

			@Override
			public Context currentContext() {
				return sink.currentContext();
			}

			@Override
			public void onSubscribe(Subscription s) {

				this.subscription = s;
				s.request(1);
			}

			@Override
			public void onNext(Integer bytes) {

				if (isClosed()) {
					return;
				}

				if (bytes > 0) {

					DataBuffer buffer = readNextChunk();
					sink.next(buffer);
					decrementDemand();
				}

				if (bytes == -1) {
					sink.complete();
					return;
				}

				subscription.request(1);
			}

			private DataBuffer readNextChunk() {

				transport.flip();

				DataBuffer dataBuffer = factory.allocateBuffer(transport.remaining());
				dataBuffer.write(transport);

				transport.clear();

				return dataBuffer;
			}

			@Override
			public void onError(Throwable t) {

				if (isClosed()) {

					Operators.onErrorDropped(t, sink.currentContext());
					return;
				}

				close();
				sink.error(t);
			}

			@Override
			public void onComplete() {

				onReadDone();

				if (!isClosed()) {

					if (enterDrainLoop()) {
						try {
							drainLoop(sink);
						} finally {
							leaveDrainLoop();
						}
					}

				}
			}
		}
	}
}
