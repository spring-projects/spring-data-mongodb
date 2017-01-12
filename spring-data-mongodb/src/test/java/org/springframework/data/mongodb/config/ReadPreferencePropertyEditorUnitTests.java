/*
 * Copyright 2015-2017 the original author or authors.
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
package org.springframework.data.mongodb.config;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.mongodb.ReadPreference;

/**
 * Unit tests for {@link ReadPreferencePropertyEditor}.
 * 
 * @author Christoph Strobl
 */
public class ReadPreferencePropertyEditorUnitTests {

	@Rule public ExpectedException expectedException = ExpectedException.none();

	ReadPreferencePropertyEditor editor;

	@Before
	public void setUp() {
		editor = new ReadPreferencePropertyEditor();
	}

	@Test // DATAMONGO-1158
	public void shouldThrowExceptionOnUndefinedPreferenceString() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("ReadPreference");
		expectedException.expectMessage("foo");

		editor.setAsText("foo");
	}

	@Test // DATAMONGO-1158
	public void shouldAllowUsageNativePreferenceStrings() {

		editor.setAsText("secondary");

		assertThat(editor.getValue(), is((Object) ReadPreference.secondary()));
	}

	@Test // DATAMONGO-1158
	public void shouldAllowUsageOfUppcaseEnumStringsForPreferences() {

		editor.setAsText("NEAREST");

		assertThat(editor.getValue(), is((Object) ReadPreference.nearest()));
	}
}
