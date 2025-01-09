/*
 * Copyright 2025. the original author or authors.
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
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.aot.generated;

import static org.assertj.core.api.Assertions.assertThat;
import example.aot.UserRepository;
import org.junit.jupiter.api.Test;
import org.springframework.aot.test.generate.TestGenerationContext;
import org.springframework.core.test.tools.TestCompiler;

/**
 * @author Christoph Strobl
 * @since 2025/01
 */
public class MongoRepositoryContributorTests {

        @Test
        public void testCompile() {

            TestMongoAotRepositoryContext aotContext = new TestMongoAotRepositoryContext(UserRepository.class, null);
            TestGenerationContext generationContext = new TestGenerationContext(UserRepository.class);

            new MongoRepositoryContributor(aotContext).contribute(generationContext);
            generationContext.writeGeneratedContent();

            TestCompiler.forSystem().with(generationContext).compile(compiled -> {
                assertThat(compiled.getAllCompiledClasses()).map(Class::getName).contains("example.aot.UserRepositoryImpl__Aot");
            });
        }

}
