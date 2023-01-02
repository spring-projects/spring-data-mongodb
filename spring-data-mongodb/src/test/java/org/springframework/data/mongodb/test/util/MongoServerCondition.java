/*
 * Copyright 2019-2023 the original author or authors.
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

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.util.Version;

/**
 * @author Christoph Strobl
 */
public class MongoServerCondition implements ExecutionCondition {

	private static final Namespace NAMESPACE = Namespace.create("mongodb", "server");

	private static final Version ANY = new Version(9999, 9999, 9999);
	private static final Version DEFAULT_HIGH = ANY;
	private static final Version DEFAULT_LOW = new Version(0, 0, 0);

	@Override
	public ConditionEvaluationResult evaluateExecutionCondition(ExtensionContext context) {

		if (context.getTags().contains("replSet")) {
			if (!serverIsPartOfReplicaSet(context)) {
				return ConditionEvaluationResult.disabled("Disabled for servers not running in replicaSet mode.");
			}
		}

		if (context.getTags().contains("version-specific") && context.getElement().isPresent()) {

			EnableIfMongoServerVersion version = AnnotatedElementUtils.findMergedAnnotation(context.getElement().get(),
					EnableIfMongoServerVersion.class);

			Version serverVersion = serverVersion(context);

			if (version != null && !serverVersion.equals(ANY)) {

				Version expectedMinVersion = Version.parse(version.isGreaterThanEqual());
				if (!expectedMinVersion.equals(ANY) && !expectedMinVersion.equals(DEFAULT_LOW)) {
					if (serverVersion.isLessThan(expectedMinVersion)) {
						return ConditionEvaluationResult.disabled(String
								.format("Disabled for server version %s; Requires at least %s.", serverVersion, expectedMinVersion));
					}
				}

				Version expectedMaxVersion = Version.parse(version.isLessThan());
				if (!expectedMaxVersion.equals(ANY) && !expectedMaxVersion.equals(DEFAULT_HIGH)) {
					if (serverVersion.isGreaterThanOrEqualTo(expectedMaxVersion)) {
						return ConditionEvaluationResult.disabled(String
								.format("Disabled for server version %s; Only supported until %s.", serverVersion, expectedMaxVersion));
					}
				}
			}
		}

		return ConditionEvaluationResult.enabled("Enabled by default");
	}

	private boolean serverIsPartOfReplicaSet(ExtensionContext context) {

		return context.getStore(NAMESPACE).getOrComputeIfAbsent("--replSet", (key) -> MongoTestUtils.serverIsReplSet(),
				Boolean.class);
	}

	private Version serverVersion(ExtensionContext context) {

		return context.getStore(NAMESPACE).getOrComputeIfAbsent(Version.class, (key) -> MongoTestUtils.serverVersion(),
				Version.class);
	}
}
