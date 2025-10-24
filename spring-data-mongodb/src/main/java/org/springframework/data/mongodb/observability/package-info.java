/**
 * Infrastructure to provide driver observability using Micrometer.
 * <p>
 * <strong>NOTE:</strong> MongoDB Java Driver 5.7+ comes with observability directly built in which can be configured
 * via {@code MongoClientSettings.Builder#observabilitySettings(ObservabilitySettings)}.
 */
@org.jspecify.annotations.NullMarked
package org.springframework.data.mongodb.observability;
