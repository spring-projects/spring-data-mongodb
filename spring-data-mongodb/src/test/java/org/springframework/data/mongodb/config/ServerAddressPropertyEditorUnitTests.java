/*
 * Copyright 2012-2013 the original author or authors.
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

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Before;
import org.junit.Test;

import com.mongodb.ServerAddress;

/**
 * Unit tests for {@link ServerAddressPropertyEditor}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class ServerAddressPropertyEditorUnitTests {

	ServerAddressPropertyEditor editor;

	@Before
	public void setUp() {
		editor = new ServerAddressPropertyEditor();
	}

	/**
	 * @see DATAMONGO-454
	 */
	@Test(expected = IllegalArgumentException.class)
	public void rejectsAddressConfigWithoutASingleParsableServerAddress() {

		editor.setAsText("foo, bar");
	}

	/**
	 * @see DATAMONGO-454
	 */
	@Test
	public void skipsUnparsableAddressIfAtLeastOneIsParsable() throws UnknownHostException {

		editor.setAsText("foo, localhost");
		assertSingleAddressOfLocalhost(editor.getValue());
	}

	/**
	 * @see DATAMONGO-454
	 */
	@Test
	public void handlesEmptyAddressAsParseError() throws UnknownHostException {

		editor.setAsText(", localhost");
		assertSingleAddressOfLocalhost(editor.getValue());
	}

	/**
	 * @see DATAMONGO-693
	 */
	@Test
	public void interpretEmptyStringAsNull() {

		editor.setAsText("");
		assertNull(editor.getValue());
	}

	private static void assertSingleAddressOfLocalhost(Object result) throws UnknownHostException {

		assertThat(result, is(instanceOf(ServerAddress[].class)));
		Collection<ServerAddress> addresses = Arrays.asList((ServerAddress[]) result);
		assertThat(addresses, hasSize(1));
		assertThat(addresses, hasItem(new ServerAddress("localhost")));
	}
}
