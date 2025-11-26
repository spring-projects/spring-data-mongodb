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

import static org.assertj.core.api.Assertions.*;

import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.geo.GeoJsonJackson3Module;
import org.springframework.data.mongodb.core.geo.GeoJsonModule;
import org.springframework.data.web.config.EnableSpringDataWebSupport;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Integration tests for {@link GeoJsonJackson3Configuration}.
 *
 * @author Bjorn Harvold
 * @author Jens Schauder
 */
@RunWith(SpringRunner.class)
@ContextConfiguration
public class GeoJsonJackson3ConfigurationIntegrationTests {

	@Configuration
	@EnableSpringDataWebSupport
	static class Config {}

	@Autowired
	GeoJsonJackson3Module geoJsonModule;

	@Test // GH-5100
	public void picksUpGeoJsonModuleConfigurationByDefault() {
		assertThat(geoJsonModule).isNotNull();
	}
}
