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

import org.springframework.aot.test.generate.TestGenerationContext;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.BeanFactoryPostProcessor;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.AbstractBeanDefinition;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.core.test.tools.TestCompiler;
import org.springframework.data.projection.ProjectionFactory;
import org.springframework.data.projection.SpelAwareProxyProjectionFactory;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.query.ValueExpressionDelegate;
import org.springframework.util.ReflectionUtils;

/**
 * Test Configuration Support Class for generated AOT Repository Fragments based on a Repository Interface.
 * <p>
 * This configuration generates the AOT repository, compiles sources and configures a BeanFactory to contain the AOT
 * fragment. Additionally, the fragment is exposed through a {@code repositoryInterface} JDK proxy forwarding method
 * invocations to the backing AOT fragment. Note that {@code repositoryInterface} is not a repository proxy.
 *
 * @author Christoph Strobl
 */
public class AotFragmentTestConfigurationSupport implements BeanFactoryPostProcessor {

	private final Class<?> repositoryInterface;
	private final TestMongoAotRepositoryContext repositoryContext;

	public AotFragmentTestConfigurationSupport(Class<?> repositoryInterface) {

		this.repositoryInterface = repositoryInterface;
		this.repositoryContext = new TestMongoAotRepositoryContext(repositoryInterface, null);
	}

	@Override
	public void postProcessBeanFactory(ConfigurableListableBeanFactory beanFactory) throws BeansException {

		TestGenerationContext generationContext = new TestGenerationContext(repositoryInterface);

		new MongoRepositoryContributor(repositoryContext).contribute(generationContext);

		AbstractBeanDefinition aotGeneratedRepository = BeanDefinitionBuilder
				.genericBeanDefinition(
						repositoryInterface.getPackageName() + "." + repositoryInterface.getSimpleName() + "Impl__Aot") //
				.addConstructorArgReference("mongoOperations") //
				.addConstructorArgValue(getCreationContext(repositoryContext)).getBeanDefinition();

		TestCompiler.forSystem().withCompilerOptions("-parameters").with(generationContext).compile(compiled -> {
			beanFactory.setBeanClassLoader(compiled.getClassLoader());
			((BeanDefinitionRegistry) beanFactory).registerBeanDefinition("fragment", aotGeneratedRepository);
		});

		BeanDefinition fragmentFacade = BeanDefinitionBuilder.rootBeanDefinition((Class) repositoryInterface, () -> {

			Object fragment = beanFactory.getBean("fragment");
			Object proxy = getFragmentFacadeProxy(fragment);

			return repositoryInterface.cast(proxy);
		}).getBeanDefinition();

		((BeanDefinitionRegistry) beanFactory).registerBeanDefinition("fragmentFacade", fragmentFacade);

		beanFactory.registerSingleton("generationContext", generationContext);
	}

	private Object getFragmentFacadeProxy(Object fragment) {

		return Proxy.newProxyInstance(repositoryInterface.getClassLoader(), new Class<?>[] { repositoryInterface },
				(p, method, args) -> {

					Method target = ReflectionUtils.findMethod(fragment.getClass(), method.getName(), method.getParameterTypes());

					if (target == null) {
						throw new MethodNotImplementedException("Method [%s] is not implemented by [%s]".formatted(method, target));
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

			@Override
			public RepositoryMetadata getRepositoryMetadata() {
				return repositoryContext.getRepositoryInformation();
			}

			@Override
			public ValueExpressionDelegate getValueExpressionDelegate() {
				return ValueExpressionDelegate.create();
			}

			@Override
			public ProjectionFactory getProjectionFactory() {
				return new SpelAwareProxyProjectionFactory();
			}
		};
	}

	public static class MethodNotImplementedException extends RuntimeException {

		public MethodNotImplementedException(String message) {
			super(message);
		}
	}
}
