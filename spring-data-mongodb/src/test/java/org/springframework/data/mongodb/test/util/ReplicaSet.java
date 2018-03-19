/*
 * Copyright 2018 the original author or authors.
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
package org.springframework.data.mongodb.test.util;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.bson.Document;
import org.junit.AssumptionViolatedException;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.springframework.test.annotation.IfProfileValue;

import com.mongodb.MongoClient;

/**
 * {@link TestRule} evaluating if MongoDB Server is running with {@code --replSet} flag.
 *
 * @author Christoph Strobl
 */
public class ReplicaSet implements TestRule {

	boolean required = false;
	AtomicReference<Boolean> runsAsReplicaSet = new AtomicReference<>();

	private ReplicaSet(boolean required) {
		this.required = required;
	}

	/**
	 * A MongoDB server running with {@code --replSet} flag is required to execute tests.
	 *
	 * @return new instance of {@link ReplicaSet}.
	 */
	public static ReplicaSet required() {
		return new ReplicaSet(true);
	}

	/**
	 * A MongoDB server running with {@code --replSet} flag might be required to execute some tests. Those tests are
	 * marked with {@code @IfProfileValue(name="replSet", value="true")}.
	 *
	 * @return new instance of {@link ReplicaSet}.
	 */
	public static ReplicaSet none() {
		return new ReplicaSet(false);
	}

	@Override
	public Statement apply(Statement base, Description description) {

		return new Statement() {

			@Override
			public void evaluate() throws Throwable {

				if (!required) {

					IfProfileValue profileValue = description.getAnnotation(IfProfileValue.class);
					if (profileValue == null || !profileValue.name().equalsIgnoreCase("replSet")) {
						base.evaluate();
						return;
					}

					if (!Boolean.valueOf(profileValue.value())) {
						base.evaluate();
						return;
					}
				}

				if (!runsAsReplicaSet()) {
					throw new AssumptionViolatedException("Not running in repl set mode");
				}
				base.evaluate();
			}
		};
	}

	public boolean runsAsReplicaSet() {

		if (runsAsReplicaSet.get() == null) {

			try (MongoClient client = new MongoClient()) {

				boolean tmp = client.getDatabase("admin").runCommand(new Document("getCmdLineOpts", "1"))
						.get("argv", List.class).contains("--replSet");
				runsAsReplicaSet.compareAndSet(null, tmp);
			}
		}
		return runsAsReplicaSet.get();
	}
}
