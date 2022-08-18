/*
 * Copyright 2022 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.aot;

import static org.assertj.core.api.Assertions.*;
import static org.springframework.data.mongodb.aot.RepositoryRegistrationAotContributionAssert.*;

import org.junit.jupiter.api.Test;
import org.springframework.aot.hint.RuntimeHints;
import org.springframework.beans.factory.aot.BeanRegistrationAotContribution;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.support.RegisteredBean;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.annotation.SynthesizedAnnotation;
import org.springframework.data.annotation.LastModifiedDate;
import org.springframework.data.annotation.Transient;
import org.springframework.data.aot.RepositoryRegistrationAotContribution;
import org.springframework.data.mongodb.aot.configs.ImperativeConfig;
import org.springframework.data.mongodb.core.convert.LazyLoadingProxy;
import org.springframework.data.mongodb.core.mapping.Address;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;

/**
 * @author Christoph Strobl
 * @since 2022/04
 */
public class AotMongoRepositoryPostProcessorUnitTests {

	@Test
	void contributesProxiesForDataAnnotations() {

		RepositoryRegistrationAotContribution repositoryBeanContribution = computeConfiguration(ImperativeConfig.class)
				.forRepository(ImperativeConfig.PersonRepository.class);

		assertThatContribution(repositoryBeanContribution) //
				.codeContributionSatisfies(contribution -> {

					contribution.contributesJdkProxy(Transient.class, SynthesizedAnnotation.class);
					contribution.contributesJdkProxy(LastModifiedDate.class, SynthesizedAnnotation.class);
					contribution.contributesJdkProxy(Document.class, SynthesizedAnnotation.class);
					contribution.contributesJdkProxy(DBRef.class, SynthesizedAnnotation.class);
				});
	}

	BeanContributionBuilder computeConfiguration(Class<?> configuration) {

		AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
		ctx.register(configuration);
		ctx.refreshForAotProcessing(new RuntimeHints());

		return it -> {

			String[] repoBeanNames = ctx.getBeanNamesForType(it);
			assertThat(repoBeanNames).describedAs("Unable to find repository %s in configuration %s.", it, configuration)
					.hasSize(1);

			String beanName = repoBeanNames[0];

			AotMongoRepositoryPostProcessor postProcessor = ctx.getBean(AotMongoRepositoryPostProcessor.class);

			postProcessor.setBeanFactory(ctx.getDefaultListableBeanFactory());

			BeanRegistrationAotContribution beanRegistrationAotContribution = postProcessor
					.processAheadOfTime(RegisteredBean.of(ctx.getBeanFactory(), beanName));
			assertThat(beanRegistrationAotContribution).isInstanceOf(RepositoryRegistrationAotContribution.class);
			return (RepositoryRegistrationAotContribution)  beanRegistrationAotContribution;
		};
	}

	interface BeanContributionBuilder {
		RepositoryRegistrationAotContribution forRepository(Class<?> repositoryInterface);
	}
}
