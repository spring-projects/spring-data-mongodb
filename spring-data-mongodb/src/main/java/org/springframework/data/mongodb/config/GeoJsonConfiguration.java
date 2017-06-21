/*
 * Copyright 2015-2017 the original author or authors.
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
package org.springframework.data.mongodb.config;

import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.geo.GeoJsonModule;
import org.springframework.data.web.config.SpringDataJacksonModules;

/**
 * Configuration class to expose {@link GeoJsonModule} as a Spring bean.
 * 
 * @author Oliver Gierke
 * @author Jens Schauder
 */
public class GeoJsonConfiguration implements SpringDataJacksonModules {

	@Bean
	public GeoJsonModule geoJsonModule() {
		return new GeoJsonModule();
	}
}
