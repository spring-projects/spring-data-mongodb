/*
 * Copyright 2015 the original author or authors.
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

import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.mongodb.ReadPreference;

/**
 * @author Christoph Strobl
 */
public class ReadPreferencePropertyEditorUnitTests {

	@Rule public ExpectedException expectedException = ExpectedException.none();

	ReadPreferencePropertyEditor editor;

	@Before
	public void setUp() {
		editor = new ReadPreferencePropertyEditor();
	}

	/**
	 * @see DATAMONGO-1158
	 */
	@Test
	public void shouldThrowExceptionOnUndefinedPreferenceString() {

		expectedException.expect(IllegalArgumentException.class);
		expectedException.expectMessage("ReadPreference");
		expectedException.expectMessage("foo");

		editor.setAsText("foo");
	}

	/**
	 * @see DATAMONGO-1158
	 */
	@Test
	public void shouldAllowUsageNativePreferenceStrings() {

		editor.setAsText("secondary");

		Assert.assertThat(editor.getValue(), Is.<Object> is(ReadPreference.secondary()));
	}

	/**
	 * @see DATAMONGO-1158
	 */
	@Test
	public void shouldAllowUsageOfUppcaseEnumStringsForPreferences() {

		editor.setAsText("NEAREST");

		Assert.assertThat(editor.getValue(), Is.<Object> is(ReadPreference.nearest()));
	}
}
