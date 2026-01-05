/*
 * Copyright 2019-present the original author or authors.
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

import java.time.Duration;

import org.junit.jupiter.api.extension.ConditionEvaluationResult;
import org.junit.jupiter.api.extension.ExecutionCondition;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.data.mongodb.MongoCollectionUtils;
import org.springframework.data.util.Version;
import org.springframework.util.NumberUtils;
import org.springframework.util.StringUtils;
import org.testcontainers.shaded.org.awaitility.Awaitility;

import com.mongodb.Function;
import com.mongodb.client.MongoClient;

/**
 * @author Christoph Strobl
 */
class MongoServerCondition implements ExecutionCondition {

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

		if (context.getTags().contains("vector-search")) {

			if (!atlasEnvironment(context)) {
				return ConditionEvaluationResult.disabled("Disabled for servers not supporting Vector Search.");
			}

			if (!isSearchIndexAvailable(context)) {
				return ConditionEvaluationResult.disabled("Search index unavailable.");
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

	private boolean isSearchIndexAvailable(ExtensionContext context) {

		EnableIfVectorSearchAvailable vectorSearchAvailable = AnnotatedElementUtils
				.findMergedAnnotation(context.getElement().get(), EnableIfVectorSearchAvailable.class);

		if (vectorSearchAvailable == null) {
			return true;
		}

		String collectionName = StringUtils.hasText(vectorSearchAvailable.collectionName())
				? vectorSearchAvailable.collectionName()
				: MongoCollectionUtils.getPreferredCollectionName(vectorSearchAvailable.collection());

		return context.getStore(NAMESPACE).getOrComputeIfAbsent("search-index-%s-available".formatted(collectionName),
				(key) -> {
					try {
						doWithClient(client -> {
							Awaitility.await().atMost(Duration.ofSeconds(60)).pollInterval(Duration.ofMillis(200)).until(() -> {
								return MongoTestUtils.isSearchIndexReady(client, null, collectionName);
							});
							return "done waiting for search index";
						});
					} catch (Exception e) {
						return false;
					}
					return true;
				}, Boolean.class);

	}

	private boolean atlasEnvironment(ExtensionContext context) {

		return context.getStore(NAMESPACE).getOrComputeIfAbsent("mongodb-atlas",
				(key) -> doWithClient(MongoTestUtils::isVectorSearchEnabled), Boolean.class);
	}

	private <T> T doWithClient(Function<MongoClient, T> function) {

		String host = System.getProperty(AtlasContainer.ATLAS_HOST);
		String port = System.getProperty(AtlasContainer.ATLAS_PORT);

		if (StringUtils.hasText(host) && StringUtils.hasText(port)) {
			try (MongoClient client = MongoTestUtils.client(host, NumberUtils.parseNumber(port, Integer.class))) {
				return function.apply(client);
			}
		}

		try (MongoClient client = MongoTestUtils.client()) {
			return function.apply(client);
		}
	}
}
