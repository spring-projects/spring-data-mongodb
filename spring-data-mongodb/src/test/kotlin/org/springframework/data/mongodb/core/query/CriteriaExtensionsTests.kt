package org.springframework.data.mongodb.core.query

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
class CriteriaExtensionsTests {

	@Mock(answer = Answers.RETURNS_MOCKS)
	lateinit var criteria: Criteria

	@Test
	fun `isEqualTo() extension should call its Java counterpart`() {
		val foo = "foo"
		criteria.isEqualTo(foo)
		Mockito.verify(criteria, Mockito.times(1)).`is`(foo)
	}

	@Test
	fun `inValues() extension should call its Java counterpart`() {
		val foo = "foo"
		val bar = "bar"
		criteria.inValues(foo, bar)
		Mockito.verify(criteria, Mockito.times(1)).`in`(foo, bar)
	}

}
