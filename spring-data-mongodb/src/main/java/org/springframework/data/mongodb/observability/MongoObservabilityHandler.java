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

import io.micrometer.api.instrument.observation.Observation;
import io.micrometer.api.instrument.observation.ObservationHandler;

/**
 * A {@link ObservationHandler} that handles {@link MongoHandlerContext}.
 *
 * @author Marcin Grzejszczak
 * @since 4.0.0
 */
public class MongoObservabilityHandler implements ObservationHandler<MongoHandlerContext> {

	@Override public void onStop(MongoHandlerContext context) {
		context.getRequestContext().delete(Observation.class);
		context.getRequestContext().delete(MongoHandlerContext.class);
	}

	@Override public boolean supportsContext(Observation.Context context) {
		return context instanceof MongoHandlerContext;
	}

}
