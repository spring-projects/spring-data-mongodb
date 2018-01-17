/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.mongodb.core.messaging;

import org.springframework.scheduling.SchedulingAwareRunnable;

/**
 * The actual {@link Task} to run within the {@link MessageListenerContainer}.
 *
 * @author Christoph Strobl
 * @since 2.1
 */
public interface Task extends SchedulingAwareRunnable, Cancelable {

	/**
	 * @return {@literal true} if the task is currently {@link State#RUNNING running}.
	 */
	default boolean isActive() {
		return State.RUNNING.equals(getState());
	}

	/**
	 * Get the current lifecycle phase.
	 *
	 * @return never {@literal null}.
	 */
	State getState();

	/**
	 * The {@link Task.State} defining the lifecycle phase the actual {@link Task}.
	 *
	 * @author Christoph Strobl
	 * @since 2.1
	 */
	enum State {
		CREATED, STARTING, RUNNING, CANCELLED;
	}
}
