/*
 * Copyright 2015-2019 the original author or authors.
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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.springframework.util.StringUtils;

import com.mongodb.MongoCredential;

/**
 * Unit tests for {@link MongoCredentialPropertyEditor}.
 *
 * @author Christoph Strobl
 * @author Stephen Tyler Conrad
 */
public class MongoCredentialPropertyEditorUnitTests {

	static final String USER_1_NAME = "tyrion";
	static final String USER_1_PWD = "dwarf";
	static final String USER_1_DB = "lannister";

	static final String USER_2_NAME = "jon";
	static final String USER_2_PWD = "warg";
	static final String USER_2_DB = "snow";

	static final String USER_3_NAME = "CN=myName,OU=myOrgUnit,O=myOrg,L=myLocality,ST=myState,C=myCountry";
	static final String USER_3_DB = "stark";

	static final String USER_4_PLAIN_NAME = "m0ng0@dmin";
	static final String USER_4_ENCODED_NAME;
	static final String USER_4_PLAIN_PWD = "mo_res:bw6},Qsdxx@admin";
	static final String USER_4_ENCODED_PWD;
	static final String USER_4_DB = "targaryen";

	static final String USER_5_NAME = "lyanna";
	static final String USER_5_PWD = "random?password";
	static final String USER_5_DB = "mormont";

	static final String USER_1_AUTH_STRING = USER_1_NAME + ":" + USER_1_PWD + "@" + USER_1_DB;
	static final String USER_1_AUTH_STRING_WITH_PLAIN_AUTH_MECHANISM = USER_1_AUTH_STRING + "?uri.authMechanism=PLAIN";

	static final String USER_2_AUTH_STRING = USER_2_NAME + ":" + USER_2_PWD + "@" + USER_2_DB;
	static final String USER_2_AUTH_STRING_WITH_MONGODB_CR_AUTH_MECHANISM = USER_2_AUTH_STRING
			+ "?uri.authMechanism=MONGODB-CR";

	static final String USER_3_AUTH_STRING_WITH_X509_AUTH_MECHANISM = "'" + USER_3_NAME + "@" + USER_3_DB
			+ "?uri.authMechanism=MONGODB-X509'";

	static final String USER_4_AUTH_STRING;

	static final String USER_5_AUTH_STRING = USER_5_NAME + ":" + USER_5_PWD + "@" + USER_5_DB;
	static final String USER_5_AUTH_STRING_WITH_PLAIN_AUTH_MECHANISM = USER_5_AUTH_STRING + "?uri.authMechanism=PLAIN";
	static final String USER_5_AUTH_STRING_WITH_QUERY_ARGS = USER_5_AUTH_STRING + "?uri.authMechanism=PLAIN&foo=&bar";

	static final String SCRAM_SHA_256_AUTH_STRING = USER_1_NAME + ":" + USER_1_PWD + "@" + USER_1_DB
			+ "?uri.authMechanism=SCRAM-SHA-256";

	static final MongoCredential USER_1_CREDENTIALS = MongoCredential.createCredential(USER_1_NAME, USER_1_DB,
			USER_1_PWD.toCharArray());
	static final MongoCredential USER_1_CREDENTIALS_PLAIN_AUTH = MongoCredential.createPlainCredential(USER_1_NAME,
			USER_1_DB, USER_1_PWD.toCharArray());

	static final MongoCredential USER_2_CREDENTIALS = MongoCredential.createCredential(USER_2_NAME, USER_2_DB,
			USER_2_PWD.toCharArray());

	static final MongoCredential USER_3_CREDENTIALS_X509_AUTH = MongoCredential.createMongoX509Credential(USER_3_NAME);

	static final MongoCredential USER_4_CREDENTIALS = MongoCredential.createCredential(USER_4_PLAIN_NAME, USER_4_DB,
			USER_4_PLAIN_PWD.toCharArray());

	static final MongoCredential USER_5_CREDENTIALS = MongoCredential.createCredential(USER_5_NAME, USER_5_DB,
			USER_5_PWD.toCharArray());
	static final MongoCredential USER_5_CREDENTIALS_PLAIN_AUTH = MongoCredential.createPlainCredential(USER_5_NAME,
			USER_5_DB, USER_5_PWD.toCharArray());

	static final MongoCredential SCRAM_SHA_256_CREDENTIALS = MongoCredential.createScramSha256Credential(USER_1_NAME,
			USER_1_DB, USER_1_PWD.toCharArray());

	MongoCredentialPropertyEditor editor;

	static {

		String encodedUserName = null;
		String encodedUserPassword = null;
		try {
			encodedUserName = URLEncoder.encode(USER_4_PLAIN_NAME, "UTF-8");
			encodedUserPassword = URLEncoder.encode(USER_4_PLAIN_PWD, "UTF-8");
		} catch (UnsupportedEncodingException e) {}

		USER_4_ENCODED_NAME = encodedUserName;
		USER_4_ENCODED_PWD = encodedUserPassword;
		USER_4_AUTH_STRING = USER_4_ENCODED_NAME + ":" + USER_4_ENCODED_PWD + "@" + USER_4_DB;

	}

	@Before
	public void setUp() {
		this.editor = new MongoCredentialPropertyEditor();
	}

	@Test // DATAMONGO-1158
	public void shouldReturnNullValueForNullText() {

		editor.setAsText(null);

		assertThat(getValue()).isNull();
	}

	@Test // DATAMONGO-1158
	public void shouldReturnNullValueForEmptyText() {

		editor.setAsText(" ");

		assertThat(getValue()).isNull();
	}

	@Test // DATAMONGO-1158
	public void shouldThrowExceptionForMalformatedCredentialsString() {
		assertThatIllegalArgumentException().isThrownBy(() -> editor.setAsText("tyrion"));
	}

	@Test // DATAMONGO-1158
	public void shouldThrowExceptionForMalformatedAuthMechanism() {
		assertThatIllegalArgumentException()
				.isThrownBy(() -> editor.setAsText(USER_2_AUTH_STRING + "?uri.authMechanism=Targaryen"));
	}

	@Test // DATAMONGO-1158
	@SuppressWarnings("unchecked")
	public void shouldReturnCredentialsValueCorrectlyWhenGivenSingleUserNamePasswordStringWithDatabaseAndNoOptions() {

		editor.setAsText(USER_1_AUTH_STRING);

		assertThat(getValue()).contains(USER_1_CREDENTIALS);
	}

	@Test // DATAMONGO-1158
	@SuppressWarnings("unchecked")
	public void shouldReturnCredentialsValueCorrectlyWhenGivenSingleUserNamePasswordStringWithDatabaseAndAuthOptions() {

		editor.setAsText(USER_1_AUTH_STRING_WITH_PLAIN_AUTH_MECHANISM);

		assertThat(getValue()).contains(USER_1_CREDENTIALS_PLAIN_AUTH);
	}

	@Test // DATAMONGO-1158
	@SuppressWarnings("unchecked")
	public void shouldReturnCredentialsValueCorrectlyWhenGivenMultipleUserNamePasswordStringWithDatabaseAndNoOptions() {

		editor
				.setAsText(StringUtils.collectionToCommaDelimitedString(Arrays.asList(USER_1_AUTH_STRING, USER_2_AUTH_STRING)));

		assertThat(getValue()).contains(USER_1_CREDENTIALS, USER_2_CREDENTIALS);
	}

	@Test // DATAMONGO-1158
	@SuppressWarnings("unchecked")
	public void shouldReturnCredentialsValueCorrectlyWhenGivenMultipleUserNamePasswordStringWithDatabaseAndMixedOptions() {

		editor.setAsText(StringUtils.collectionToCommaDelimitedString(
				Arrays.asList(USER_1_AUTH_STRING_WITH_PLAIN_AUTH_MECHANISM, USER_2_AUTH_STRING)));

		assertThat(getValue()).contains(USER_1_CREDENTIALS_PLAIN_AUTH, USER_2_CREDENTIALS);
	}

	@Test // DATAMONGO-1257
	@SuppressWarnings("unchecked")
	public void shouldReturnCredentialsValueCorrectlyWhenGivenMultipleQuotedUserNamePasswordStringWithDatabaseAndNoOptions() {

		editor.setAsText(StringUtils.collectionToCommaDelimitedString(
				Arrays.asList("'" + USER_1_AUTH_STRING + "'", "'" + USER_2_AUTH_STRING + "'")));

		assertThat(getValue()).contains(USER_1_CREDENTIALS, USER_2_CREDENTIALS);
	}

	@Test // DATAMONGO-1257
	@SuppressWarnings("unchecked")
	public void shouldReturnCredentialsValueCorrectlyWhenGivenSingleQuotedUserNamePasswordStringWithDatabaseAndNoOptions() {

		editor.setAsText("'" + USER_1_AUTH_STRING + "'");

		assertThat(getValue()).contains(USER_1_CREDENTIALS);
	}

	@Test // DATAMONGO-1257
	@SuppressWarnings("unchecked")
	public void shouldReturnX509CredentialsCorrectly() {

		editor.setAsText(USER_3_AUTH_STRING_WITH_X509_AUTH_MECHANISM);

		assertThat(getValue()).contains(USER_3_CREDENTIALS_X509_AUTH);
	}

	@Test // DATAMONGO-1257
	@SuppressWarnings("unchecked")
	public void shouldReturnX509CredentialsCorrectlyWhenNoDbSpecified() {

		editor.setAsText("tyrion?uri.authMechanism=MONGODB-X509");

		assertThat(getValue()).contains(MongoCredential.createMongoX509Credential("tyrion"));
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1257
	public void shouldThrowExceptionWhenNoDbSpecifiedForMongodbCR() {

		editor.setAsText("tyrion?uri.authMechanism=MONGODB-CR");

		getValue();
	}

	@Test(expected = IllegalArgumentException.class) // DATAMONGO-1257
	public void shouldThrowExceptionWhenDbIsEmptyForMongodbCR() {

		editor.setAsText("tyrion@?uri.authMechanism=MONGODB-CR");

		getValue();
	}

	@Test // DATAMONGO-1317
	@SuppressWarnings("unchecked")
	public void encodedUserNameAndPasswordShouldBeDecoded() {

		editor.setAsText(USER_4_AUTH_STRING);

		assertThat(getValue()).contains(USER_4_CREDENTIALS);
	}

	@Test // DATAMONGO-2016
	@SuppressWarnings("unchecked")
	public void passwordWithQuestionMarkShouldNotBeInterpretedAsOptionString() {

		editor.setAsText(USER_5_AUTH_STRING);

		assertThat(getValue()).contains(USER_5_CREDENTIALS);
	}

	@Test // DATAMONGO-2016
	@SuppressWarnings("unchecked")
	public void passwordWithQuestionMarkShouldNotBreakParsingOfOptionString() {

		editor.setAsText(USER_5_AUTH_STRING_WITH_PLAIN_AUTH_MECHANISM);

		assertThat(getValue()).contains(USER_5_CREDENTIALS_PLAIN_AUTH);
	}

	@Test // DATAMONGO-2051
	public void shouldReturnScramSha256Credentials() {

		editor.setAsText(SCRAM_SHA_256_AUTH_STRING);

		assertThat(getValue()).contains(SCRAM_SHA_256_CREDENTIALS);
	}

	@Test // DATAMONGO-2016
	@SuppressWarnings("unchecked")
	public void failsGracefullyOnEmptyQueryArgument() {
		assertThatIllegalArgumentException().isThrownBy(() -> editor.setAsText(USER_5_AUTH_STRING_WITH_QUERY_ARGS));
	}

	@SuppressWarnings("unchecked")
	private List<MongoCredential> getValue() {
		return (List<MongoCredential>) editor.getValue();
	}
}
