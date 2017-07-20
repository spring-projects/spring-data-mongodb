/*
 * Copyright 2017 the original author or authors.
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
package org.springframework.data.mongodb.core

import com.nhaarman.mockito_kotlin.verify
import example.first.First
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Answers
import org.mockito.Mock
import org.mockito.junit.MockitoJUnitRunner

/**
 * @author Sebastien Deleuze
 * @author Mark Paluch
 */
@RunWith(MockitoJUnitRunner::class)
class ExecutableRemoveOperationExtensionsTests {

    @Mock(answer = Answers.RETURNS_MOCKS)
    lateinit var operation: ExecutableRemoveOperation

    @Test // DATAMONGO-1689
    fun `remove(KClass) extension should call its Java counterpart`() {

        operation.remove(First::class)
        verify(operation).remove(First::class.java)
    }

    @Test // DATAMONGO-1689
    fun `remove() with reified type parameter extension should call its Java counterpart`() {

        operation.remove<First>()
        verify(operation).remove(First::class.java)
    }

}
