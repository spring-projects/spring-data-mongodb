/*
 * Copyright 2014-2024 the original author or authors.
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

import java.util.concurrent.atomic.AtomicReference;

import org.bson.Document;
import org.junit.AssumptionViolatedException;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.springframework.data.util.Version;
import org.springframework.test.context.junit4.SpringRunner;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;

/**
 * {@link TestRule} verifying server tests are executed against match a given version. This one can be used as
 * {@link ClassRule} eg. in context depending tests run with {@link SpringRunner} when the context would fail to start
 * in case of invalid version, or as simple {@link Rule} on specific tests.
 *
 * @author Christoph Strobl
 * @author Mark Paluch
 * @since 1.6
 * @deprecated Use {@link MongoServerCondition} instead.
 */
@Deprecated
public class MongoVersionRule implements TestRule {

	private static final Version ANY = new Version(9999, 9999, 9999);
	private static final Version DEFAULT_HIGH = ANY;
	private static final Version DEFAULT_LOW = new Version(0, 0, 0);

	public static MongoVersionRule REQUIRES_4_2 = MongoVersionRule
			.atLeast(org.springframework.data.util.Version.parse("4.2"));

	private final AtomicReference<Version> currentVersion = new AtomicReference<>(null);
	private final Version minVersion;
	private final Version maxVersion;

	private String host = "127.0.0.1";
	private int port = 27017;

	public MongoVersionRule(Version min, Version max) {

		this.minVersion = min;
		this.maxVersion = max;
	}

	public static MongoVersionRule any() {
		return new MongoVersionRule(ANY, ANY);
	}

	public static MongoVersionRule atLeast(Version minVersion) {
		return new MongoVersionRule(minVersion, DEFAULT_HIGH);
	}

	public static MongoVersionRule atMost(Version maxVersion) {
		return new MongoVersionRule(DEFAULT_LOW, maxVersion);
	}

	public MongoVersionRule withServerRunningAt(String host, int port) {

		this.host = host;
		this.port = port;

		return this;
	}

	/**
	 * @see Version#isGreaterThan(Version)
	 */
	public boolean isGreaterThan(Version version) {
		return getCurrentVersion().isGreaterThan(version);
	}

	/**
	 * @see Version#isGreaterThanOrEqualTo(Version)
	 */
	public boolean isGreaterThanOrEqualTo(Version version) {
		return getCurrentVersion().isGreaterThanOrEqualTo(version);
	}

	/**
	 * @see Version#is(Version)
	 */
	public boolean is(Version version) {
		return getCurrentVersion().equals(version);
	}

	/**
	 * @see Version#isLessThan(Version)
	 */
	public boolean isLessThan(Version version) {
		return getCurrentVersion().isLessThan(version);
	}

	/**
	 * @see Version#isLessThanOrEqualTo(Version)
	 */
	public boolean isLessThanOrEqualTo(Version version) {
		return getCurrentVersion().isGreaterThanOrEqualTo(version);
	}

	@Override
	public Statement apply(final Statement base, Description description) {

		return new Statement() {

			@Override
			public void evaluate() throws Throwable {

				if (!getCurrentVersion().equals(ANY)) {

					Version minVersion = MongoVersionRule.this.minVersion.equals(ANY) ? DEFAULT_LOW
							: MongoVersionRule.this.minVersion;
					Version maxVersion = MongoVersionRule.this.maxVersion.equals(ANY) ? DEFAULT_HIGH
							: MongoVersionRule.this.maxVersion;

					if (description.getAnnotation(MongoVersion.class) != null) {
						MongoVersion version = description.getAnnotation(MongoVersion.class);
						if (version != null) {

							Version expectedMinVersion = Version.parse(version.asOf());
							if (!expectedMinVersion.equals(ANY) && !expectedMinVersion.equals(DEFAULT_LOW)) {
								minVersion = expectedMinVersion;
							}

							Version expectedMaxVersion = Version.parse(version.until());
							if (!expectedMaxVersion.equals(ANY) && !expectedMaxVersion.equals(DEFAULT_HIGH)) {
								maxVersion = expectedMaxVersion;
							}
						}
					}

					validateVersion(minVersion, maxVersion);
				}

				base.evaluate();
			}
		};
	}

	private void validateVersion(Version min, Version max) {

		if (getCurrentVersion().isLessThan(min) || getCurrentVersion().isGreaterThanOrEqualTo(max)) {

			throw new AssumptionViolatedException(
					String.format("Expected MongoDB server to be in range (%s, %s] but found %s", min, max, currentVersion));
		}

	}

	private Version getCurrentVersion() {

		if (currentVersion.get() == null) {
			currentVersion.compareAndSet(null, fetchCurrentVersion());
		}

		return currentVersion.get();
	}

	private Version fetchCurrentVersion() {

		try {

			MongoClient client = MongoTestUtils.client(host, port);
			MongoDatabase database = client.getDatabase("test");
			Document result = database.runCommand(new Document("buildInfo", 1));

			return Version.parse(result.get("version", String.class));
		} catch (Exception e) {
			return ANY;
		}
	}

	@Override
	public String toString() {
		return getCurrentVersion().toString();
	}
}
