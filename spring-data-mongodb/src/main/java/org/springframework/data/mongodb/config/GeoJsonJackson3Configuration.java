/*
 * Copyright 2015-2025 the original author or authors.
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
package org.springframework.data.mongodb.config;

import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.geo.GeoJsonJackson3Module;
import org.springframework.data.web.config.SpringDataJackson3Modules;

/**
 * Configuration class to expose {@link GeoJsonJackson3Module} as a Spring bean.
 *
 * @author Jens Schauder
 */
public class GeoJsonJackson3Configuration implements SpringDataJackson3Modules {

	@Bean
	public GeoJsonJackson3Module geoJsonModule() {
		return new GeoJsonJackson3Module();
	}
}
