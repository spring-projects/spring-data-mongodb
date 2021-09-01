/*
 * Copyright 2021. the original author or authors.
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
	 * @return the key id to use. May contain a parsable {@link org.springframework.expression.Expression expression}.
	 */
	String[] keyId() default {};

	/**
	 * @return the algorithm.
	 */
	String algorithm() default "";
}
