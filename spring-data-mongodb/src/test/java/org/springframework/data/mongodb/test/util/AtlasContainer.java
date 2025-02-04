/*
 * Copyright 2024 the original author or authors.
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
package org.springframework.data.mongodb.test.util;

import org.springframework.core.env.StandardEnvironment;

import org.testcontainers.mongodb.MongoDBAtlasLocalContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Extension to MongoDBAtlasLocalContainer.
 *
 * @author Christoph Strobl
 */
public class AtlasContainer extends MongoDBAtlasLocalContainer {

	private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse("mongodb/mongodb-atlas-local");
	private static final String DEFAULT_TAG = "8.0.0";
	private static final String LATEST = "latest";

	private AtlasContainer(String dockerImageName) {
		super(DockerImageName.parse(dockerImageName));
	}

	private AtlasContainer(DockerImageName dockerImageName) {
		super(dockerImageName);
	}

	public static AtlasContainer bestMatch() {
		return tagged(new StandardEnvironment().getProperty("mongodb.atlas.version", DEFAULT_TAG));
	}

	public static AtlasContainer latest() {
		return tagged(LATEST);
	}

	public static AtlasContainer version8() {
		return tagged(DEFAULT_TAG);
	}

	public static AtlasContainer tagged(String tag) {
		return new AtlasContainer(DEFAULT_IMAGE_NAME.withTag(tag));
	}

}
