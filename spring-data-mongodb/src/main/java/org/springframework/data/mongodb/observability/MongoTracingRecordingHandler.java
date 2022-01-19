/*
 * Copyright 2013-2021 the original author or authors.
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

import java.net.InetSocketAddress;
import java.time.Duration;

import com.mongodb.MongoSocketException;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.event.CommandStartedEvent;
import io.micrometer.core.instrument.Timer;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.TracingRecordingHandler;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.springframework.data.mongodb.core.convert.MappingMongoConverter;

public class MongoTracingRecordingHandler implements TracingRecordingHandler<MongoHandlerContext> {

	private static final Log LOGGER = LogFactory.getLog(MappingMongoConverter.class);

	private final Tracer tracer;

	private boolean setRemoteIpAndPortEnabled;

	public MongoTracingRecordingHandler(Tracer tracer) {
		this.tracer = tracer;
	}

	@Override public Tracer getTracer() {
		return this.tracer;
	}

	@Override public void onStart(Timer.Sample sample, MongoHandlerContext context) {
		CommandStartedEvent event = context.getCommandStartedEvent();
		String databaseName = event.getDatabaseName();
		Span.Builder builder = this.tracer.spanBuilder().kind(Span.Kind.CLIENT)
				.remoteServiceName("mongodb-" + databaseName);
		if (isSetRemoteIpAndPortEnabled()) {
			setRemoteIpAndPort(event, builder);
		}
		getTracingContext(context).setSpan(builder.start());
	}

	private void setRemoteIpAndPort(CommandStartedEvent event, Span.Builder spanBuilder) {
		ConnectionDescription connectionDescription = event.getConnectionDescription();
		if (connectionDescription != null) {
			try {
				InetSocketAddress socketAddress = connectionDescription.getServerAddress().getSocketAddress();
				spanBuilder.remoteIpAndPort(socketAddress.getAddress().getHostAddress(), socketAddress.getPort());
			} catch (MongoSocketException ignored) {
				if (LOGGER.isDebugEnabled()) {
					LOGGER.debug("Ignored exception when setting remote ip and port", ignored);
				}
			}
		}
	}

	@Override public void onError(Timer.Sample sample, MongoHandlerContext context, Throwable throwable) {
		getTracingContext(context).getSpan().error(throwable);
	}

	@Override public void onStop(Timer.Sample sample, MongoHandlerContext context, Timer timer, Duration duration) {
		TracingContext tracingContext = getTracingContext(context);
		Span span = tracingContext.getSpan();
		span.name(timer.getId().getName());
		tagSpan(context, timer.getId(), span);
		span.end();
	}

	public boolean isSetRemoteIpAndPortEnabled() {
		return this.setRemoteIpAndPortEnabled;
	}

	public void setSetRemoteIpAndPortEnabled(boolean setRemoteIpAndPortEnabled) {
		this.setRemoteIpAndPortEnabled = setRemoteIpAndPortEnabled;
	}
}
