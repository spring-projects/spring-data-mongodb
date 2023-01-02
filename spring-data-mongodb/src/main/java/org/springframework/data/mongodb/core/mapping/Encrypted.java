/*
 * Copyright 2021-2023 the original author or authors.
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
package org.springframework.data.mongodb.core.mapping;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * {@link Encrypted} provides data required for MongoDB Client Side Field Level Encryption that is applied during schema
 * resolution. It can be applied on top level (typically those types annotated with {@link Document} to provide the
 * {@literal encryptMetadata}.
 *
 * <pre class="code">
 * &#64;Document
 * &#64;Encrypted(keyId = "4fPYFM9qSgyRAjgQ2u+IMQ==")
 * public class Patient {
 * 	 private ObjectId id;
 * 	 private String name;
 *
 * 	 &#64;Field("publisher_ac")
 * 	 &#64;DocumentReference(lookup = "{ 'acronym' : ?#{#target} }") private Publisher publisher;
 * }
 *
 * "encryptMetadata": {
 *    "keyId": [
 *      {
 *        "$binary": {
 *          "base64": "4fPYFM9qSgyRAjgQ2u+IMQ==",
 *          "subType": "04"
 *        }
 *      }
 *    ]
 *  }
 * </pre>
 * 
 * <br />
 * On property level it is used for deriving field specific {@literal encrypt} settings.
 *
 * <pre class="code">
 * public class Patient {
 * 	 private ObjectId id;
 * 	 private String name;
 *
 * 	 &#64;Encrypted(keyId = "4fPYFM9qSgyRAjgQ2u+IMQ==", algorithm = "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic")
 * 	 private String ssn;
 * }
 *
 * "ssn" : {
 *   "encrypt": {
 *      "keyId": [
 *        {
 *          "$binary": {
 *            "base64": "4fPYFM9qSgyRAjgQ2u+IMQ==",
 *            "subType": "04"
 *          }
 *        }
 *      ],
 *      "algorithm" : "AEAD_AES_256_CBC_HMAC_SHA_512-Deterministic",
 *      "bsonType" : "string"
 *    }
 *  }
 * </pre>
 * 
 * @author Christoph Strobl
 * @since 3.3
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE, ElementType.FIELD })
public @interface Encrypted {

	/**
	 * Get the {@code keyId} to use. The value must resolve to either the UUID representation of the key or a base64
	 * encoded value representing the UUID value.
	 * <br />
	 * On {@link ElementType#TYPE} level the {@link #keyId()} can be left empty if explicitly set for fields. <br />
	 * On {@link ElementType#FIELD} level the {@link #keyId()} can be left empty if inherited from
	 * {@literal encryptMetadata}.
	 *
	 * @return the key id to use. May contain a parsable {@link org.springframework.expression.Expression expression}. In
	 *         this case the {@code #target} variable will hold the target element name.
	 */
	String[] keyId() default {};

	/**
	 * Set the algorithm to use.
	 * <br />
	 * On {@link ElementType#TYPE} level the {@link #algorithm()} can be left empty if explicitly set for fields. <br />
	 * On {@link ElementType#FIELD} level the {@link #algorithm()} can be left empty if inherited from
	 * {@literal encryptMetadata}.
	 *
	 * @return the encryption algorithm.
	 * @see org.springframework.data.mongodb.core.EncryptionAlgorithms
	 */
	String algorithm() default "";
}
