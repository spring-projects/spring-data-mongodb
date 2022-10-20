package org.springframework.data.mongodb.observability;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import io.micrometer.observation.ObservationRegistry;

/**
 * Class to configure needed beans for MongoDB + Micrometer.
 *
 * @since 3.0
 */
@Configuration
public class MongoMetricsConfiguration {

	@Bean
	public MongoObservationCommandListener mongoObservationCommandListener(ObservationRegistry registry) {
		return new MongoObservationCommandListener(registry);
	}

}
