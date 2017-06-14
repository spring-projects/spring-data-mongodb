package org.springframework.data.mongodb.core

import example.first.First
import org.junit.Test
import org.junit.runner.RunWith
import org.mockito.Answers
import org.mockito.Mock
import org.mockito.Mockito
import org.mockito.junit.MockitoJUnitRunner

/**
 * @author Sebastien Deleuze
 */
@RunWith(MockitoJUnitRunner::class)
class ExecutableRemoveOperationExtensionsTests {

	@Mock(answer = Answers.RETURNS_MOCKS)
	lateinit var operation: ExecutableRemoveOperation

	@Test // DATAMONGO-1689
	fun `remove(KClass) extension should call its Java counterpart`() {
		operation.remove(First::class)
		Mockito.verify(operation, Mockito.times(1)).remove(First::class.java)
	}

	@Test // DATAMONGO-1689
	fun `remove() with reified type parameter extension should call its Java counterpart`() {
		operation.remove<First>()
		Mockito.verify(operation, Mockito.times(1)).remove(First::class.java)
	}

}
