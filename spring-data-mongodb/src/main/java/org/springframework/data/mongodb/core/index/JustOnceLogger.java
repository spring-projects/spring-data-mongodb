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
package org.springframework.data.mongodb.core.index;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Christoph Strobl
 * @since 2.2
 */
class JustOnceLogger {

	private static final Map<String, Set<String>> KNOWN_LOGS = new ConcurrentHashMap<>();
	private static final String AUTO_INDEX_CREATION_CONFIG_CHANGE;

	static {
		AUTO_INDEX_CREATION_CONFIG_CHANGE = "Automatic index creation will be disabled by default as of Spring Data MongoDB 3.x."
				+ System.lineSeparator()
				+ "\tPlease use 'MongoMappingContext#setAutoIndexCreation(boolean)' or override 'MongoConfigurationSupport#autoIndexCreation()' to be explicit."
				+ System.lineSeparator()
				+ "\tHowever, we recommend setting up indices manually in an application ready block. You may use index derivation there as well."
				+ System.lineSeparator() + System.lineSeparator() //
				+ "\t> -----------------------------------------------------------------------------------------"
				+ System.lineSeparator() //
				+ "\t> @EventListener(ApplicationReadyEvent.class)" + System.lineSeparator() //
				+ "\t> public void initIndicesAfterStartup() {" + System.lineSeparator() //
				+ "\t>" + System.lineSeparator() //
				+ "\t>     IndexOperations indexOps = mongoTemplate.indexOps(DomainType.class);" + System.lineSeparator()//
				+ "\t>" + System.lineSeparator() //
				+ "\t>     IndexResolver resolver = new MongoPersistentEntityIndexResolver(mongoMappingContext);"
				+ System.lineSeparator() //
				+ "\t>     resolver.resolveIndexFor(DomainType.class).forEach(indexOps::ensureIndex);" + System.lineSeparator() //
				+ "\t> }" + System.lineSeparator() //
				+ "\t> -----------------------------------------------------------------------------------------"
				+ System.lineSeparator();
	}

	static void logWarnIndexCreationConfigurationChange(String loggerName) {
		warnOnce(loggerName, AUTO_INDEX_CREATION_CONFIG_CHANGE);
	}

	static void warnOnce(String loggerName, String message) {

		Logger logger = LoggerFactory.getLogger(loggerName);
		if (!logger.isWarnEnabled()) {
			return;
		}

		if (!KNOWN_LOGS.containsKey(loggerName)) {

			KNOWN_LOGS.put(loggerName, new ConcurrentSkipListSet<>(Collections.singleton(message)));
			logger.warn(message);
		} else {

			Set<String> messages = KNOWN_LOGS.get(loggerName);
			if (messages.contains(message)) {
				return;
			}

			messages.add(message);
			logger.warn(message);
		}
	}
}
