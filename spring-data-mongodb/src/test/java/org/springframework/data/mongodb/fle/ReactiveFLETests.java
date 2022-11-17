/*
 * Copyright 2022 the original author or authors.
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
package org.springframework.data.mongodb.fle;

import static org.assertj.core.api.Assertions.*;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.Map.Entry;
import java.util.function.Supplier;

import org.bson.Document;
import org.junit.jupiter.api.Test;

import com.mongodb.reactivestreams.client.vault.ClientEncryption;

/**
 * @author Christoph Strobl
 * @since 2022/11
 */
public class ReactiveFLETests {

	ClientEncryption encryption;

	@Test
	void xxx() {

		Supplier<String> valueSupplier = new Supplier<String>() {
			@Override
			public String get() {
				System.out.println("invoked");
				return "v1";
			}
		};

		Document source = new Document("name", "value").append("mono", Mono.fromSupplier(() -> "from mono"))
				.append("nested", new Document("n1", Mono.fromSupplier(() -> "from nested mono")));

		resolveValues(Mono.just(source)) //
				.as(StepVerifier::create).consumeNextWith(resolved -> {
					assertThat(resolved).isEqualTo(Document
							.parse("{\"name\": \"value\", \"mono\": \"from mono\", \"nested\" : { \"n1\" : \"from nested mono\"}}"));
				}).verifyComplete();
	}

	private Mono<Document> resolveValues(Mono<Document> document) {
		return document.flatMap(source -> {
			for (Entry<String, Object> entry : source.entrySet()) {
				if (entry.getValue()instanceof Mono<?> valueMono) {
					return valueMono.flatMap(value -> {
						source.put(entry.getKey(), value);
						return resolveValues(Mono.just(source));
					});
				}
				if (entry.getValue()instanceof Document nested) {
					return resolveValues(Mono.just(nested)).map(it -> {
						source.put(entry.getKey(), it);
						return source;
					});
				}
			}
			return Mono.just(source);
		});
	}

	class MongoDocumentProvider {

	}

}
