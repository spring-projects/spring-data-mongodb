/*
 * Copyright 2012-2023 the original author or authors.
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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.mongodb.WriteConcern;

/**
 * Unit tests for {@link WriteConcernPropertyEditor}.
 *
 * @author Oliver Gierke
 * @author Christoph Strobl
 */
public class WriteConcernPropertyEditorUnitTests {

	WriteConcernPropertyEditor editor;

	@BeforeEach
	public void setUp() {
		editor = new WriteConcernPropertyEditor();
	}

	@Test // DATAMONGO-2199
	public void createsWriteConcernForWellKnownConstants() {

		editor.setAsText("JOURNALED");
		assertThat(editor.getValue()).isEqualTo(WriteConcern.JOURNALED);
	}

	@Test
	public void createsWriteConcernForUnknownConstants() {

		editor.setAsText("-1");
		assertThat(editor.getValue()).isEqualTo(new WriteConcern("-1"));
	}
}
