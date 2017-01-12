/*
 * Copyright 2012-2017 the original author or authors.
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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.mongodb.ServerAddress;

/**
 * Unit tests for {@link ServerAddressPropertyEditor}.
 * 
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class ServerAddressPropertyEditorUnitTests {

	@Rule public ExpectedException expectedException = ExpectedException.none();

	ServerAddressPropertyEditor editor;

	@Before
	public void setUp() {
		editor = new ServerAddressPropertyEditor();
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-454, DATAMONGO-1062
	public void rejectsAddressConfigWithoutASingleParsableAndResolvableServerAddress() {

		String unknownHost1 = "gugu.nonexistant.example.org";
		String unknownHost2 = "gaga.nonexistant.example.org";

		assertUnresolveableHostnames(unknownHost1, unknownHost2);

		editor.setAsText(unknownHost1 + "," + unknownHost2);
	}

	@Test // DATAMONGO-454
	public void skipsUnparsableAddressIfAtLeastOneIsParsable() throws UnknownHostException {

		editor.setAsText("foo, localhost");
		assertSingleAddressOfLocalhost(editor.getValue());
	}

	@Test // DATAMONGO-454
	public void handlesEmptyAddressAsParseError() throws UnknownHostException {

		editor.setAsText(", localhost");
		assertSingleAddressOfLocalhost(editor.getValue());
	}

	@Test // DATAMONGO-693
	public void interpretEmptyStringAsNull() {

		editor.setAsText("");
		assertNull(editor.getValue());
	}

	@Test // DATAMONGO-808
	public void handleIPv6HostaddressLoopbackShort() throws UnknownHostException {

		String hostAddress = "::1";
		editor.setAsText(hostAddress);

		assertSingleAddressWithPort(hostAddress, null, editor.getValue());
	}

	@Test // DATAMONGO-808
	public void handleIPv6HostaddressLoopbackShortWithPort() throws UnknownHostException {

		String hostAddress = "::1";
		int port = 27017;
		editor.setAsText(hostAddress + ":" + port);

		assertSingleAddressWithPort(hostAddress, port, editor.getValue());
	}

	/**
	 * Here we detect no port since the last segment of the address contains leading zeros.
	 */
	@Test // DATAMONGO-808
	public void handleIPv6HostaddressLoopbackLong() throws UnknownHostException {

		String hostAddress = "0000:0000:0000:0000:0000:0000:0000:0001";
		editor.setAsText(hostAddress);

		assertSingleAddressWithPort(hostAddress, null, editor.getValue());
	}

	@Test // DATAMONGO-808
	public void handleIPv6HostaddressLoopbackLongWithBrackets() throws UnknownHostException {

		String hostAddress = "[0000:0000:0000:0000:0000:0000:0000:0001]";
		editor.setAsText(hostAddress);

		assertSingleAddressWithPort(hostAddress, null, editor.getValue());
	}

	/**
	 * We can't tell whether the last part of the hostAddress represents a port or not.
	 */
	@Test // DATAMONGO-808
	public void shouldFailToHandleAmbiguousIPv6HostaddressLongWithoutPortAndWithoutBrackets() throws UnknownHostException {

		expectedException.expect(IllegalArgumentException.class);

		String hostAddress = "0000:0000:0000:0000:0000:0000:0000:128";
		editor.setAsText(hostAddress);
	}

	@Test // DATAMONGO-808
	public void handleIPv6HostaddressExampleAddressWithPort() throws UnknownHostException {

		String hostAddress = "0000:0000:0000:0000:0000:0000:0000:0001";
		int port = 27017;
		editor.setAsText(hostAddress + ":" + port);

		assertSingleAddressWithPort(hostAddress, port, editor.getValue());
	}

	@Test // DATAMONGO-808
	public void handleIPv6HostaddressExampleAddressInBracketsWithPort() throws UnknownHostException {

		String hostAddress = "[0000:0000:0000:0000:0000:0000:0000:0001]";
		int port = 27017;
		editor.setAsText(hostAddress + ":" + port);

		assertSingleAddressWithPort(hostAddress, port, editor.getValue());
	}

	private static void assertSingleAddressOfLocalhost(Object result) throws UnknownHostException {
		assertSingleAddressWithPort("localhost", null, result);
	}

	private static void assertSingleAddressWithPort(String hostAddress, Integer port, Object result)
			throws UnknownHostException {

		assertThat(result, is(instanceOf(ServerAddress[].class)));
		Collection<ServerAddress> addresses = Arrays.asList((ServerAddress[]) result);
		assertThat(addresses, hasSize(1));
		if (port == null) {
			assertThat(addresses, hasItem(new ServerAddress(InetAddress.getByName(hostAddress))));
		} else {
			assertThat(addresses, hasItem(new ServerAddress(InetAddress.getByName(hostAddress), port)));
		}
	}

	private void assertUnresolveableHostnames(String... hostnames) {

		for (String hostname : hostnames) {
			try {
				InetAddress.getByName(hostname);
				Assert.fail("Supposedly unresolveable hostname '" + hostname + "' can be resolved.");
			} catch (UnknownHostException expected) {
				// ok
			}
		}
	}
}
