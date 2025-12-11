/*
 * Copyright 2025 the original author or authors.
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
package org.springframework.data.mongodb.repository.aot;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.mockito.Mockito;

import org.springframework.aot.test.generate.TestGenerationContext;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.config.RuntimeBeanReference;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.DefaultBeanNameGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.test.tools.TestCompiler;
import org.springframework.core.type.AnnotationMetadata;
import org.springframework.data.expression.ValueExpressionParser;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.convert.NoOpDbRefResolver;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfigurationSource;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.query.QueryMethodValueEvaluationContextAccessor;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.data.util.Lazy;
import org.springframework.util.ReflectionUtils;

/**
 * Test Configuration Support Class for generated AOT Repository Fragments based on a Repository Interface.
 * <p>
 * This configuration generates the AOT repository, compiles sources and configures a BeanFactory to contain the AOT
 * fragment. Additionally, the fragment is exposed through a {@code repositoryInterface} JDK proxy forwarding method
 * invocations to the backing AOT fragment by default (or when setting {@code fragmentFacade=true}). Note that
 * {@code repositoryInterface} is not a repository proxy.
 *
 * @author Christoph Strobl
 */
public class AotFragmentTestConfigurationSupport implements BeanFactoryPostProcessor {

	private final Class<?> repositoryInterface;
	private final RepositoryConfigurationSource configSource;
	private final boolean registerFragmentFacade;

	public AotFragmentTestConfigurationSupport(Class<?> repositoryInterface) {
		this(repositoryInterface, SampleConfiguration.class, true);
	}

	public AotFragmentTestConfigurationSupport(Class<?> repositoryInterface, Class<?> configClass) {
		this(repositoryInterface, configClass, true);
	}

	public AotFragmentTestConfigurationSupport(Class<?> repositoryInterface, boolean registerFragmentFacade) {
		this(repositoryInterface, SampleConfiguration.class, registerFragmentFacade);
	}

	public AotFragmentTestConfigurationSupport(Class<?> repositoryInterface, Class<?> configClass,
			boolean registerFragmentFacade) {

		this.repositoryInterface = repositoryInterface;
		this.registerFragmentFacade = registerFragmentFacade;
		this.configSource = new AnnotationRepositoryConfigurationSource(AnnotationMetadata.introspect(configClass),
				EnableMongoRepositories.class, new DefaultResourceLoader(), new StandardEnvironment(),
				Mockito.mock(BeanDefinitionRegistry.class), DefaultBeanNameGenerator.INSTANCE);
	}

	@Bean
	MongoCustomConversions customConversions() {
		return MongoCustomConversions.create(adapter -> adapter.useSpringDataJavaTimeCodecs());
	}

	@Bean
	MongoMappingContext mongoMappingContext(MongoCustomConversions conversions) {
		MongoMappingContext context = new MongoMappingContext();
		context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
		return context;
	}

	@Bean
	MongoConverter mongoConverter(MongoMappingContext context, MongoCustomConversions conversions) {
		MappingMongoConverter converter = new MappingMongoConverter(NoOpDbRefResolver.INSTANCE, context);
		converter.setCustomConversions(conversions);
		return converter;
	}

	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

		TestMongoAotRepositoryContext repositoryContext = getRepositoryContext(beanFactory);
		TestGenerationContext generationContext = new TestGenerationContext(repositoryInterface);

		new MongoRepositoryContributor(repositoryContext).contribute(generationContext);
		generationContext.writeGeneratedContent();

		AbstractBeanDefinition aotGeneratedRepository = BeanDefinitionBuilder
				.genericBeanDefinition(
						repositoryInterface.getPackageName() + "." + repositoryInterface.getSimpleName() + "Impl__AotRepository") //
				.addConstructorArgValue(new RuntimeBeanReference(MongoOperations.class)) //
				.addConstructorArgValue(getCreationContext(repositoryContext)).getBeanDefinition();

		TestCompiler.forSystem().withCompilerOptions("-parameters").with(generationContext).compile(compiled -> {
			beanFactory.setBeanClassLoader(compiled.getClassLoader());
			((BeanDefinitionRegistry) beanFactory).registerBeanDefinition("fragment", aotGeneratedRepository);
		});

		if (registerFragmentFacade) {
			BeanDefinition fragmentFacade = BeanDefinitionBuilder.rootBeanDefinition((Class) repositoryInterface, () -> {

				Object fragment = beanFactory.getBean("fragment");
				Object proxy = getFragmentFacadeProxy(fragment);

				return repositoryInterface.cast(proxy);
			}).getBeanDefinition();

			((BeanDefinitionRegistry) beanFactory).registerBeanDefinition("fragmentFacade", fragmentFacade);
		}

		beanFactory.registerSingleton("generationContext", generationContext);
	}

	public TestMongoAotRepositoryContext getRepositoryContext(ConfigurableListableBeanFactory beanFactory) {
		return new TestMongoAotRepositoryContext(beanFactory, repositoryInterface, configSource);
	}

	private Object getFragmentFacadeProxy(Object fragment) {

		return Proxy.newProxyInstance(repositoryInterface.getClassLoader(), new Class<?>[] { repositoryInterface },
				(p, method, args) -> {

					Method target = ReflectionUtils.findMethod(fragment.getClass(), method.getName(), method.getParameterTypes());

					if (target == null) {
						throw new MethodNotImplementedException(
								"Method [%s] is not implemented by [%s]".formatted(method, fragment.getClass()));
					}

					try {
						return target.invoke(fragment, args);
					} catch (ReflectiveOperationException e) {
						ReflectionUtils.handleReflectionException(e);
					}

					return null;
				});
	}

	private RepositoryFactoryBeanSupport.FragmentCreationContext getCreationContext(
			TestMongoAotRepositoryContext repositoryContext) {

		return new RepositoryFactoryBeanSupport.FragmentCreationContext() {

			final Lazy<ProjectionFactory> projectionFactory = Lazy.of(SpelAwareProxyProjectionFactory::new);

			@Override
			public RepositoryMetadata getRepositoryMetadata() {
				return repositoryContext.getRepositoryInformation();
			}

			@Override
			public ValueExpressionDelegate getValueExpressionDelegate() {

				QueryMethodValueEvaluationContextAccessor queryMethodValueEvaluationContextAccessor = new QueryMethodValueEvaluationContextAccessor(
						new StandardEnvironment(), repositoryContext.getBeanFactory());
				return new ValueExpressionDelegate(queryMethodValueEvaluationContextAccessor, ValueExpressionParser.create());
			}

			@Override
			public ProjectionFactory getProjectionFactory() {
				return projectionFactory.get();
			}
		};
	}

	public static class MethodNotImplementedException extends RuntimeException {

		public MethodNotImplementedException(String message) {
			super(message);
		}
	}

	@EnableMongoRepositories(considerNestedRepositories = true, includeFilters = {
			@ComponentScan.Filter(classes = SampleConfiguration.class, type = FilterType.ASSIGNABLE_TYPE) })
	public static class SampleConfiguration {

	}

}
