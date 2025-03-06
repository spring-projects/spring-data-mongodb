/*
 * Copyright 2025 the original author or authors.
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
 * @author Christoph Strobl
 * @since 2025/03
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Encrypted(algorithm = "Range")
@ValueConverter
public @interface RangeEncrypted {

    /**
     * Set the contention factor
     * <p>
     * Only required when using {@literal range} encryption.
     * @return the contention factor
     */
    long contentionFactor() default -1;

    /**
     * Set the {@literal range} options
     * <p>
     * Should be valid extended json representing the range options and including the following values:
     * {@code min}, {@code max}, {@code trimFactor} and {@code sparsity}.
     *
     * @return the json representation of range options
     */
    String rangeOptions() default "";

    /**
     * The {@link EncryptingConverter} type handling the {@literal en-/decryption} of the annotated property.
     *
     * @return the configured {@link EncryptingConverter}. A {@link MongoEncryptionConverter} by default.
     */
    @AliasFor(annotation = ValueConverter.class, value = "value")
    Class<? extends PropertyValueConverter> value() default MongoEncryptionConverter.class;

}
