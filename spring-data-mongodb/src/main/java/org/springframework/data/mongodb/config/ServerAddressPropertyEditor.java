/*
 * Copyright 2011 the original author or authors.
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
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.util.StringUtils;

import com.mongodb.ServerAddress;

/**
 * Parse a {@link String} to a {@link ServerAddress} array. The format is host1:port1,host2:port2,host3:port3.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public class ServerAddressPropertyEditor extends PropertyEditorSupport {

	private static final Log LOG = LogFactory.getLog(ServerAddressPropertyEditor.class);

	/*
	 * (non-Javadoc)
	 * @see java.beans.PropertyEditorSupport#setAsText(java.lang.String)
	 */
	@Override
	public void setAsText(String replicaSetString) {

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
	private ServerAddress parseServerAddress(String source) {

		String[] hostAndPort = StringUtils.delimitedListToStringArray(source.trim(), ":");

		if (!StringUtils.hasText(source) || hostAndPort.length > 2) {
			LOG.warn(String.format("Could not parse address source '%s'. Check your replica set configuration!", source));
			return null;
		}

		try {
			return hostAndPort.length == 1 ? new ServerAddress(hostAndPort[0]) : new ServerAddress(hostAndPort[0],
					Integer.parseInt(hostAndPort[1]));
		} catch (UnknownHostException e) {
			LOG.warn(String.format("Could not parse host '%s'. Check your replica set configuration!", hostAndPort[0]));
		} catch (NumberFormatException e) {
			LOG.warn(String.format("Could not parse port '%s'. Check your replica set configuration!", hostAndPort[1]));
		}

		return null;
	}
}
