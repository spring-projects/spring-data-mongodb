package org.springframework.data.mongodb.core.query

/**
 * Extension for [Criteria.is] providing an `isEqualTo` alias since `in` is a reserved keyword in Kotlin.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
fun Criteria.isEqualTo(o: Any) : Criteria = `is`(o)

/**
 * Extension for [Criteria.in] providing an `inValues` alias since `in` is a reserved keyword in Kotlin.
 *
 * @author Sebastien Deleuze
 * @since 2.0
 */
fun Criteria.inValues(vararg o: Any) : Criteria = `in`(*o)
