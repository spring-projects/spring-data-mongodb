/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.mongodb.core.timeseries;

import java.time.Duration;

/**
 * @author Christoph Strobl
 * @since 5.0
 */
public interface Span {

	/**
	 * Defines the time between timestamps in the same bucket in a range between {@literal 1-31.536.000} seconds.
	 */
	Duration time();

	/**
	 * Simple factory to create a {@link Span} for the given {@link Duration}.
	 * 
	 * @param duration time between timestamps 
	 * @return new instance of {@link Span}.
	 */
	static Span of(Duration duration) {
		return () -> duration;
	}
}
