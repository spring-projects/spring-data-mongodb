/*
 * Copyright 2012-2024 the original author or authors.
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import com.mongodb.ServerAddress;

/**
 * Unit tests for {@link ServerAddressPropertyEditor}.
 *
 * @author Oliver Gierke
 * @author Thomas Darimont
 */
public class ServerAddressPropertyEditorUnitTests {

	ServerAddressPropertyEditor editor;

	@BeforeEach
	public void setUp() {
		editor = new ServerAddressPropertyEditor();
	}

	@Test // DATAMONGO-454, DATAMONGO-1062
	public void rejectsAddressConfigWithoutASingleParsableAndResolvableServerAddress() {

		String unknownHost1 = "gugu.nonexistant.example.org";
		String unknownHost2 = "gaga.nonexistant.example.org";

		assertUnresolveableHostnames(unknownHost1, unknownHost2);

		assertThatExceptionOfType(IllegalArgumentException.class)
				.isThrownBy(() -> editor.setAsText(unknownHost1 + "," + unknownHost2));
	}

	@Test // DATAMONGO-454
	@EnabledIfSystemProperty(named = "user.name", matches = "jenkins")
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
		assertThat(editor.getValue()).isNull();
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
	public void shouldFailToHandleAmbiguousIPv6HostaddressLongWithoutPortAndWithoutBrackets() {

		String hostAddress = "0000:0000:0000:0000:0000:0000:0000:128";

		assertThatIllegalArgumentException().isThrownBy(() -> editor.setAsText(hostAddress));
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

		assertThat(result).isInstanceOf(ServerAddress[].class);
		Collection<ServerAddress> addresses = Arrays.asList((ServerAddress[]) result);
		assertThat(addresses).hasSize(1);
		if (port == null) {
			assertThat(addresses).contains(new ServerAddress(InetAddress.getByName(hostAddress)));
		} else {
			assertThat(addresses).contains(new ServerAddress(InetAddress.getByName(hostAddress), port));
		}
	}

	private void assertUnresolveableHostnames(String... hostnames) {

		for (String hostname : hostnames) {
			try {
				InetAddress.getByName(hostname).isReachable(1500);
				fail("Supposedly unresolveable hostname '" + hostname + "' can be resolved.");
			} catch (IOException expected) {
				// ok
			}
		}
	}
}
