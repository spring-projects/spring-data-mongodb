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
class ExecutableInsertOperationExtensionsTests {

	@Mock(answer = Answers.RETURNS_MOCKS)
	lateinit var operation: ExecutableInsertOperation

	@Test // DATAMONGO-1689
	fun `insert(KClass) extension should call its Java counterpart`() {
		operation.insert(First::class)
		Mockito.verify(operation, Mockito.times(1)).insert(First::class.java)
	}

	@Test // DATAMONGO-1689
	fun `insert() with reified type parameter extension should call its Java counterpart`() {
		operation.insert<First>()
		Mockito.verify(operation, Mockito.times(1)).insert(First::class.java)
	}

}
