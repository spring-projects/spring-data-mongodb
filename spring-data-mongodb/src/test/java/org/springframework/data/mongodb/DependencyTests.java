/*
 * Copyright 2017-2024 the original author or authors.
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

import static de.schauderhaft.degraph.check.JCheck.*;
import static org.hamcrest.MatcherAssert.*;

import de.schauderhaft.degraph.configuration.NamedPattern;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

/**
 * Tests package dependency constraints.
 *
 * @author Jens Schauder
 * @author Oliver Gierke
 */
@Disabled("Needs to be tansitioned to ArchUnit")
class DependencyTests {

	@Test
	void noInternalPackageCycles() {

		assertThat(classpath() //
				.noJars() //
				.including("org.springframework.data.mongodb.**") //
				.filterClasspath("*target/classes") //
				.printOnFailure("degraph.graphml"), //
				violationFree() //
		);
	}

	@Test
	void onlyConfigMayUseRepository() {

		assertThat(classpath() //
				.including("org.springframework.data.**") //
				.filterClasspath("*target/classes") //
				.printOnFailure("onlyConfigMayUseRepository.graphml") //
				.withSlicing("slices", //
						"**.(config).**", //
						new NamedPattern("**.cdi.**", "config"), //
						"**.(repository).**", //
						new NamedPattern("**", "other"))
				.allow("config", "repository", "other"), //
				violationFree() //
		);
	}

	@Test
	void commonsInternaly() {

		assertThat(classpath() //
				.noJars() //
				.including("org.springframework.data.**") //
				.excluding("org.springframework.data.mongodb.**") //
				.filterClasspath("*target/classes") //
				.printTo("commons.graphml"), //
				violationFree() //
		);
	}

}
