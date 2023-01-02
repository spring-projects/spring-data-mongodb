/*
 * Copyright 2020-2023 the original author or authors.
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
package org.springframework.data.mongodb;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.data.util.Version;
import org.springframework.util.StringUtils;

import com.mongodb.MongoDriverInformation;

/**
 * Class that exposes the SpringData MongoDB specific information like the current {@link Version} or
 * {@link MongoDriverInformation driver information}.
 *
 * @author Christoph Strobl
 * @since 3.0
 */
public class SpringDataMongoDB {

	private static final Log LOGGER = LogFactory.getLog(SpringDataMongoDB.class);

	private static final Version FALLBACK_VERSION = new Version(3);
	private static final MongoDriverInformation DRIVER_INFORMATION = MongoDriverInformation
			.builder(MongoDriverInformation.builder().build()).driverName("spring-data").build();

	/**
	 * Obtain the SpringData MongoDB specific driver information.
	 *
	 * @return never {@literal null}.
	 */
	public static MongoDriverInformation driverInformation() {
		return DRIVER_INFORMATION;
	}

	/**
	 * Fetches the "Implementation-Version" manifest attribute from the jar file.
	 * <br />
	 * Note that some ClassLoaders do not expose the package metadata, hence this class might not be able to determine the
	 * version in all environments. In this case the current Major version is returned as a fallback.
	 *
	 * @return never {@literal null}.
	 */
	public static Version version() {

		Package pkg = SpringDataMongoDB.class.getPackage();
		String versionString = (pkg != null ? pkg.getImplementationVersion() : null);

		if (!StringUtils.hasText(versionString)) {

			LOGGER.debug("Unable to find Spring Data MongoDB version.");
			return FALLBACK_VERSION;
		}

		try {
			return Version.parse(versionString);
		} catch (Exception e) {
			LOGGER.debug(String.format("Cannot read Spring Data MongoDB version '%s'.", versionString));
		}

		return FALLBACK_VERSION;
	}

}
