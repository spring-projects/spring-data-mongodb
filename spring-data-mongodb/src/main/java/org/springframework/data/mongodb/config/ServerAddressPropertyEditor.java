/*
 * Copyright 2011-2017 the original author or authors.
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

import java.beans.PropertyEditorSupport;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import com.mongodb.ServerAddress;

/**
 * Parse a {@link String} to a {@link ServerAddress} array. The format is host1:port1,host2:port2,host3:port3.
 *
 * @author Mark Pollack
 * @author Oliver Gierke
 * @author Thomas Darimont
 * @author Christoph Strobl
 */
public class ServerAddressPropertyEditor extends PropertyEditorSupport {

	/**
	 * A port is a number without a leading 0 at the end of the address that is proceeded by just a single :.
	 */
	private static final String HOST_PORT_SPLIT_PATTERN = "(?<!:):(?=[123456789]\\d*$)";
	private static final String COULD_NOT_PARSE_ADDRESS_MESSAGE = "Could not parse address {} '{}'. Check your replica set configuration!";
	private static final Logger LOG = LoggerFactory.getLogger(ServerAddressPropertyEditor.class);

	/*
	 * (non-Javadoc)
	 * @see java.beans.PropertyEditorSupport#setAsText(java.lang.String)
	 */
	@Override
	public void setAsText(@Nullable String replicaSetString) {

		if (!StringUtils.hasText(replicaSetString)) {
			setValue(null);
			return;
		}

		String[] replicaSetStringArray = StringUtils.commaDelimitedListToStringArray(replicaSetString);
		Set<ServerAddress> serverAddresses = new HashSet<ServerAddress>(replicaSetStringArray.length);

		for (String element : replicaSetStringArray) {

			ServerAddress address = parseServerAddress(element);

			if (address != null) {
				serverAddresses.add(address);
			}
		}

		if (serverAddresses.isEmpty()) {
			throw new IllegalArgumentException(
					"Could not resolve at least one server of the replica set configuration! Validate your config!");
		}

		setValue(serverAddresses.toArray(new ServerAddress[serverAddresses.size()]));
	}

	/**
	 * Parses the given source into a {@link ServerAddress}.
	 *
	 * @param source
	 * @return the
	 */
	@Nullable
	private ServerAddress parseServerAddress(String source) {

		if (!StringUtils.hasText(source)) {
			LOG.warn(COULD_NOT_PARSE_ADDRESS_MESSAGE, "source", source);
			return null;
		}

		String[] hostAndPort = extractHostAddressAndPort(source.trim());

		if (hostAndPort.length > 2) {
			LOG.warn(COULD_NOT_PARSE_ADDRESS_MESSAGE, "source", source);
			return null;
		}

		try {
			InetAddress hostAddress = InetAddress.getByName(hostAndPort[0]);
			Integer port = hostAndPort.length == 1 ? null : Integer.parseInt(hostAndPort[1]);

			return port == null ? new ServerAddress(hostAddress) : new ServerAddress(hostAddress, port);
		} catch (UnknownHostException e) {
			LOG.warn(COULD_NOT_PARSE_ADDRESS_MESSAGE, "host", hostAndPort[0]);
		} catch (NumberFormatException e) {
			LOG.warn(COULD_NOT_PARSE_ADDRESS_MESSAGE, "port", hostAndPort[1]);
		}

		return null;
	}

	/**
	 * Extract the host and port from the given {@link String}.
	 *
	 * @param addressAndPortSource must not be {@literal null}.
	 * @return
	 */
	private String[] extractHostAddressAndPort(String addressAndPortSource) {

		Assert.notNull(addressAndPortSource, "Address and port source must not be null!");

		String[] hostAndPort = addressAndPortSource.split(HOST_PORT_SPLIT_PATTERN);
		String hostAddress = hostAndPort[0];

		if (isHostAddressInIPv6BracketNotation(hostAddress)) {
			hostAndPort[0] = hostAddress.substring(1, hostAddress.length() - 1);
		}

		return hostAndPort;
	}

	private boolean isHostAddressInIPv6BracketNotation(String hostAddress) {
		return hostAddress.startsWith("[") && hostAddress.endsWith("]");
	}
}
