/*
 * Copyright 2013-2022 the original author or authors.
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

import io.micrometer.observation.Observation;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.handler.TracingObservationHandler;

import java.net.InetSocketAddress;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.MongoSocketException;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.event.CommandStartedEvent;

/**
 * A {@link TracingObservationHandler} that handles {@link MongoHandlerContext}. It configures a span specific to Mongo
 * operations.
 *
 * @author Marcin Grzejszczak
 * @author Greg Turnquist
 * @since 4.0.0
 */
public class MongoTracingObservationHandler implements TracingObservationHandler<MongoHandlerContext> {

	private static final Log log = LogFactory.getLog(MongoTracingObservationHandler.class);

	private final Tracer tracer;

	private boolean setRemoteIpAndPortEnabled;

	public MongoTracingObservationHandler(Tracer tracer) {
		this.tracer = tracer;
	}

	@Override
	public Tracer getTracer() {
		return this.tracer;
	}

	@Override
	public void onStart(MongoHandlerContext context) {

		CommandStartedEvent event = context.getCommandStartedEvent();

		Span.Builder builder = this.tracer.spanBuilder() //
				.name(context.getContextualName()) //
				.kind(Span.Kind.CLIENT) //
				.remoteServiceName("mongodb-" + event.getDatabaseName());

		if (this.setRemoteIpAndPortEnabled) {

			ConnectionDescription connectionDescription = event.getConnectionDescription();

			if (connectionDescription != null) {

				try {

					InetSocketAddress socketAddress = connectionDescription.getServerAddress().getSocketAddress();
					builder.remoteIpAndPort(socketAddress.getAddress().getHostAddress(), socketAddress.getPort());
				} catch (MongoSocketException e) {
					if (log.isDebugEnabled()) {
						log.debug("Ignored exception when setting remote ip and port", e);
					}
				}
			}
		}

		getTracingContext(context).setSpan(builder.start());
	}

	@Override
	public void onStop(MongoHandlerContext context) {

		Span span = getRequiredSpan(context);
		tagSpan(context, span);

		context.getRequestContext().delete(Observation.class);
		context.getRequestContext().delete(MongoHandlerContext.class);

		span.end();
	}

	@Override
	public boolean supportsContext(Observation.Context context) {
		return context instanceof MongoHandlerContext;
	}

	/**
	 * Should remote ip and port be set on the span.
	 *
	 * @return {@code true} when the remote ip and port should be set
	 */
	public boolean isSetRemoteIpAndPortEnabled() {
		return this.setRemoteIpAndPortEnabled;
	}

	public void setSetRemoteIpAndPortEnabled(boolean setRemoteIpAndPortEnabled) {
		this.setRemoteIpAndPortEnabled = setRemoteIpAndPortEnabled;
	}
}
