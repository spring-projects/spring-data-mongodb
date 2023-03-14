/*
 * Copyright 2023 the original author or authors.
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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.convert.ValueConverter;
import org.springframework.data.mongodb.core.convert.encryption.EncryptingConverter;
import org.springframework.data.mongodb.core.convert.encryption.MongoEncryptionConverter;

/**
 * {@link ExplicitEncrypted} is a {@link ElementType#FIELD field} level {@link ValueConverter} annotation that indicates
 * the target element is subject to encryption during the mapping process, in which a given domain type is converted
 * into the store specific format.
 * <p>
 * The {@link #value()} attribute, defines the bean type to look up within the
 * {@link org.springframework.context.ApplicationContext} to obtain the {@link EncryptingConverter} responsible for the
 * actual {@literal en-/decryption} while {@link #algorithm()} and {@link #keyAltName()} can be used to define aspects
 * of the encryption process.
 *
 * <pre class="code">
 * public class Patient {
 * 	private ObjectId id;
 * 	private String name;
 *
 * 	&#64;ExplicitEncrypted(algorithm = AEAD_AES_256_CBC_HMAC_SHA_512_Random, keyAltName = "secred-key-alternative-name") //
 * 	private String ssn;
 * }
 * </pre>
 *
 * @author Christoph Strobl
 * @since 4.1
 * @see ValueConverter
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Encrypted
@ValueConverter
public @interface ExplicitEncrypted {

	/**
	 * Define the algorithm to use.
	 * <p>
	 * A {@literal Deterministic} algorithm ensures that a given input value always encrypts to the same output while a
	 * {@literal randomized} one will produce different results every time.
	 * <p>
	 * Please make sure to use an algorithm that is in line with MongoDB's encryption rules for simple types, complex
	 * objects and arrays as well as the query limitations that come with each of them.
	 *
	 * @return the string representation of the encryption algorithm to use.
	 * @see org.springframework.data.mongodb.core.EncryptionAlgorithms
	 */
	@AliasFor(annotation = Encrypted.class, value = "algorithm")
	String algorithm() default "";

	/**
	 * Set the {@literal Key Alternate Name} that references the {@literal Data Encryption Key} to be used.
	 * <p>
	 * An empty String indicates that no alternative key name was configured.
	 * <p>
	 * It is possible to use the {@literal "/"} character as a prefix to access a particular field value in the same
	 * domain type. In this case {@code "/name"} references the value of the {@literal name} field. Please note that
	 * update operations will require the full object to resolve those values.
	 *
	 * @return the {@literal Key Alternate Name} if set or an empty {@link String}.
	 */
	String keyAltName() default "";

	/**
	 * The {@link EncryptingConverter} type handling the {@literal en-/decryption} of the annotated property.
	 *
	 * @return the configured {@link EncryptingConverter}. A {@link MongoEncryptionConverter} by default.
	 */
	@AliasFor(annotation = ValueConverter.class, value = "value")
	Class<? extends PropertyValueConverter> value() default MongoEncryptionConverter.class;
}
