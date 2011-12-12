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

import org.springframework.util.StringUtils;

import com.mongodb.ServerAddress;

/**
 * Parse a {@link String} to a {@link ServerAddress} array. The format is host1:port1,host2:port2,host3:port3.
 * 
 * @author Mark Pollack
 * @author Oliver Gierke
 */
public class ServerAddressPropertyEditor extends PropertyEditorSupport {

	/*
	 * (non-Javadoc)
	 * @see java.beans.PropertyEditorSupport#setAsText(java.lang.String)
	 */
	@Override
	public void setAsText(String replicaSetString) {

		String[] replicaSetStringArray = StringUtils.commaDelimitedListToStringArray(replicaSetString);
		ServerAddress[] serverAddresses = new ServerAddress[replicaSetStringArray.length];

		for (int i = 0; i < replicaSetStringArray.length; i++) {

			String[] hostAndPort = StringUtils.delimitedListToStringArray(replicaSetStringArray[i], ":");

			try {
				serverAddresses[i] = new ServerAddress(hostAndPort[0], Integer.parseInt(hostAndPort[1]));
			} catch (NumberFormatException e) {
				throw new IllegalArgumentException("Could not parse port " + hostAndPort[1], e);
			} catch (UnknownHostException e) {
				throw new IllegalArgumentException("Could not parse host " + hostAndPort[0], e);
			}
		}

		setValue(serverAddresses);
	}
}
