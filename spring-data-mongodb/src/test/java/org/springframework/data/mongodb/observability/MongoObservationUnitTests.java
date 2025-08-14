/*
 * Copyright 2025-present the original author or authors.
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
package org.springframework.data.mongodb.observability;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.mongodb.ConnectionString;

/**
 * @author Christoph Strobl
 */
class MongoObservationUnitTests {

	@ParameterizedTest // GH-5020
	@MethodSource("connectionStrings")
	void connectionStringRendering(ConnectionString source, String expected) {
		assertThat(MongoObservation.connectionString(source)).isEqualTo(expected);
	}

	private static Stream<Arguments> connectionStrings() {

		return Stream.of(Arguments.of(new ConnectionString(
				"mongodb+srv://m0ngP%40oUser:m0ngP%40o@cluster0.example.mongodb.net/?retryWrites=true&w=majority"),
				"mongodb+srv://*****:*****@cluster0.example.mongodb.net/?retryWrites=true&w=majority"), //
				Arguments.of(new ConnectionString(
						"mongodb://mongodb:m0ngP%40o@cluster0.example.mongodb.net,cluster1.example.com:1234/?retryWrites=true"),
						"mongodb://*****:*****@cluster0.example.mongodb.net,cluster1.example.com:1234/?retryWrites=true"), //
				Arguments.of(
						new ConnectionString("mongodb://myDatabaseUser@cluster0.example.mongodb.net/?authMechanism=MONGODB-X509"),
						"mongodb://*****@cluster0.example.mongodb.net/?authMechanism=MONGODB-X509"), //
				Arguments.of(
						new ConnectionString("mongodb+srv://myDatabaseUser:mongodb@cluster0.example.mongodb.net/?w=acknowledged"),
						"mongodb+srv://*****:*****@cluster0.example.mongodb.net/?w=acknowledged"), //
				Arguments.of(
						new ConnectionString(
								new String("mongodb://mongodb:mongodb@localhost:27017".getBytes(), StandardCharsets.US_ASCII)),
						"mongodb://*****:*****@localhost:27017"),
				Arguments.of(new ConnectionString("mongodb+srv://cluster0.example.mongodb.net/?retryWrites=true&w=majority"),
						"mongodb+srv://cluster0.example.mongodb.net/?retryWrites=true&w=majority"), //
				Arguments.of(
						new ConnectionString(
								"mongodb+srv://mongodb:mongodb@cluster0.example.mongodb.net/?retryWrites=true&w=majority"),
						"mongodb+srv://*****:*****@cluster0.example.mongodb.net/?retryWrites=true&w=majority"));
	}

}
