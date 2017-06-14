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
class ExecutableAggregationOperationExtensionsTests {

	@Mock(answer = Answers.RETURNS_MOCKS)
	lateinit var operation: ExecutableAggregationOperation

	@Test // DATAMONGO-1689
	fun `aggregateAndReturn(KClass) extension should call its Java counterpart`() {
		operation.aggregateAndReturn(First::class)
		Mockito.verify(operation, Mockito.times(1)).aggregateAndReturn(First::class.java)
	}

	@Test // DATAMONGO-1689
	fun `aggregateAndReturn() with reified type parameter extension should call its Java counterpart`() {
		operation.aggregateAndReturn<First>()
		Mockito.verify(operation, Mockito.times(1)).aggregateAndReturn(First::class.java)
	}

}
