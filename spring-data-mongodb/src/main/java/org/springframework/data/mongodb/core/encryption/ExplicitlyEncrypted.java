/*
 * Copyright 2023. the original author or authors.
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

/*
 * Copyright 2023 the original author or authors.
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
package org.springframework.data.mongodb.core.encryption;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.springframework.core.annotation.AliasFor;
import org.springframework.data.convert.PropertyValueConverter;
import org.springframework.data.convert.PropertyValueConverter.ObjectToObjectPropertyValueConverter;
import org.springframework.data.convert.ValueConverter;
import org.springframework.data.mongodb.core.mapping.Encrypted;

/**
 * @author Christoph Strobl
 * @since 2023/02
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
@Encrypted
@ValueConverter
public @interface ExplicitlyEncrypted {

	@AliasFor(annotation = Encrypted.class, value = "algorithm")
	String algorithm() default "";

	String altKeyName() default "";

	@AliasFor(annotation = ValueConverter.class, value = "value")
	Class<? extends PropertyValueConverter> value() default EncryptingConverter.class;
}
