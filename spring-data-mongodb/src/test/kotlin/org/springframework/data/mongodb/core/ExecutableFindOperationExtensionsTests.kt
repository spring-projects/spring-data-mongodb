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
class ExecutableFindOperationExtensionsTests {

	@Mock(answer = Answers.RETURNS_MOCKS)
	lateinit var operation: ExecutableFindOperation

	@Mock(answer = Answers.RETURNS_MOCKS)
	lateinit var operationWithProjection: ExecutableFindOperation.FindOperationWithProjection<First>

	@Test // DATAMONGO-1689
	fun `ExecutableFindOperation#query(KClass) extension should call its Java counterpart`() {
		operation.query(First::class)
		Mockito.verify(operation, Mockito.times(1)).query(First::class.java)
	}

	@Test // DATAMONGO-1689
	fun `ExecutableFindOperation#query() with reified type parameter extension should call its Java counterpart`() {
		operation.query<First>()
		Mockito.verify(operation, Mockito.times(1)).query(First::class.java)
	}

	@Test // DATAMONGO-1689
	fun `ExecutableFindOperation#FindOperationWithProjection#asType(KClass) extension should call its Java counterpart`() {
		operationWithProjection.asType(First::class)
		Mockito.verify(operationWithProjection, Mockito.times(1)).`as`(First::class.java)
	}

	@Test // DATAMONGO-1689
	fun `ExecutableFindOperation#FindOperationWithProjection#asType() with reified type parameter extension should call its Java counterpart`() {
		operationWithProjection.asType()
		Mockito.verify(operationWithProjection, Mockito.times(1)).`as`(First::class.java)
	}

}
