/*
 * Copyright 2015-2018 the original author or authors.
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
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.springframework.lang.Nullable;
import org.springframework.util.StringUtils;

import com.mongodb.MongoCredential;

/**
 * Parse a {@link String} to a Collection of {@link MongoCredential}.
 *
 * @author Christoph Strobl
 * @author Oliver Gierke
 * @since 1.7
 */
public class MongoCredentialPropertyEditor extends PropertyEditorSupport {

	private static final Pattern GROUP_PATTERN = Pattern.compile("(\\\\?')(.*?)\\1");

	private static final String AUTH_MECHANISM_KEY = "uri.authMechanism";
	private static final String USERNAME_PASSWORD_DELIMITER = ":";
	private static final String DATABASE_DELIMITER = "@";
	private static final String OPTIONS_DELIMITER = "?";
	private static final String OPTION_VALUE_DELIMITER = "&";

	/*
	 * (non-Javadoc)
	 * @see java.beans.PropertyEditorSupport#setAsText(java.lang.String)
	 */
	@Override
	public void setAsText(@Nullable String text) throws IllegalArgumentException {

		if (!StringUtils.hasText(text)) {
			return;
		}

		List<MongoCredential> credentials = new ArrayList<>();

		for (String credentialString : extractCredentialsString(text)) {

			String[] userNameAndPassword = extractUserNameAndPassword(credentialString);
			String database = extractDB(credentialString);
			Properties options = extractOptions(credentialString);

			if (!options.isEmpty()) {

				if (options.containsKey(AUTH_MECHANISM_KEY)) {

					String authMechanism = options.getProperty(AUTH_MECHANISM_KEY);

					if (MongoCredential.GSSAPI_MECHANISM.equals(authMechanism)) {

						verifyUserNamePresent(userNameAndPassword);
						credentials.add(MongoCredential.createGSSAPICredential(userNameAndPassword[0]));
					} else if (MongoCredential.MONGODB_CR_MECHANISM.equals(authMechanism)) {

						verifyUsernameAndPasswordPresent(userNameAndPassword);
						verifyDatabasePresent(database);
						credentials.add(MongoCredential.createMongoCRCredential(userNameAndPassword[0], database,
								userNameAndPassword[1].toCharArray()));
					} else if (MongoCredential.MONGODB_X509_MECHANISM.equals(authMechanism)) {

						verifyUserNamePresent(userNameAndPassword);
						credentials.add(MongoCredential.createMongoX509Credential(userNameAndPassword[0]));
					} else if (MongoCredential.PLAIN_MECHANISM.equals(authMechanism)) {

						verifyUsernameAndPasswordPresent(userNameAndPassword);
						verifyDatabasePresent(database);
						credentials.add(MongoCredential.createPlainCredential(userNameAndPassword[0], database,
								userNameAndPassword[1].toCharArray()));
					} else if (MongoCredential.SCRAM_SHA_1_MECHANISM.equals(authMechanism)) {

						verifyUsernameAndPasswordPresent(userNameAndPassword);
						verifyDatabasePresent(database);
						credentials.add(MongoCredential.createScramSha1Credential(userNameAndPassword[0], database,
								userNameAndPassword[1].toCharArray()));
					} else {
						throw new IllegalArgumentException(
								String.format("Cannot create MongoCredentials for unknown auth mechanism '%s'!", authMechanism));
					}
				}
			} else {

				verifyUsernameAndPasswordPresent(userNameAndPassword);
				verifyDatabasePresent(database);
				credentials.add(
						MongoCredential.createCredential(userNameAndPassword[0], database, userNameAndPassword[1].toCharArray()));
			}
		}

		setValue(credentials);
	}

	private List<String> extractCredentialsString(String source) {

		Matcher matcher = GROUP_PATTERN.matcher(source);
		List<String> list = new ArrayList<>();

		while (matcher.find()) {

			String value = StringUtils.trimLeadingCharacter(matcher.group(), '\'');
			list.add(StringUtils.trimTrailingCharacter(value, '\''));
		}

		if (!list.isEmpty()) {
			return list;
		}

		return Arrays.asList(source.split(","));
	}

	private static String[] extractUserNameAndPassword(String text) {

		int index = text.lastIndexOf(DATABASE_DELIMITER);

		index = index != -1 ? index : text.lastIndexOf(OPTIONS_DELIMITER);

		if (index == -1) {
			return new String[] {};
		}

		return Arrays.stream(text.substring(0, index).split(USERNAME_PASSWORD_DELIMITER))
				.map(MongoCredentialPropertyEditor::decodeParameter).toArray(String[]::new);
	}

	private static String extractDB(String text) {

		int dbSeparationIndex = text.lastIndexOf(DATABASE_DELIMITER);

		if (dbSeparationIndex == -1) {
			return "";
		}

		String tmp = text.substring(dbSeparationIndex + 1);
		int optionsSeparationIndex = tmp.lastIndexOf(OPTIONS_DELIMITER);

		return optionsSeparationIndex > -1 ? tmp.substring(0, optionsSeparationIndex) : tmp;
	}

	private static Properties extractOptions(String text) {

		int optionsSeparationIndex = text.lastIndexOf(OPTIONS_DELIMITER);
		int dbSeparationIndex = text.lastIndexOf(OPTIONS_DELIMITER);

		if (optionsSeparationIndex == -1 || dbSeparationIndex > optionsSeparationIndex) {
			return new Properties();
		}

		Properties properties = new Properties();

		for (String option : text.substring(optionsSeparationIndex + 1).split(OPTION_VALUE_DELIMITER)) {
			String[] optionArgs = option.split("=");
			properties.put(optionArgs[0], optionArgs[1]);
		}

		return properties;
	}

	private static void verifyUsernameAndPasswordPresent(String[] source) {

		verifyUserNamePresent(source);

		if (source.length != 2) {
			throw new IllegalArgumentException(
					"Credentials need to specify username and password like in 'username:password@database'!");
		}
	}

	private static void verifyDatabasePresent(String source) {

		if (!StringUtils.hasText(source)) {
			throw new IllegalArgumentException("Credentials need to specify database like in 'username:password@database'!");
		}
	}

	private static void verifyUserNamePresent(String[] source) {

		if (source.length == 0 || !StringUtils.hasText(source[0])) {
			throw new IllegalArgumentException("Credentials need to specify username!");
		}
	}

	private static String decodeParameter(String it) {
		try {
			return URLDecoder.decode(it, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new IllegalArgumentException("o_O UTF-8 not supported!", e);
		}
	}
}
