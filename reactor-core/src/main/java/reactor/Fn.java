/*
 * Copyright (c) 2011-2013 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package reactor;

import reactor.core.Composable;
import reactor.core.Promise;
import reactor.fn.*;
import reactor.fn.selector.BaseSelector;
import reactor.fn.selector.ClassSelector;
import reactor.fn.selector.RegexSelector;
import reactor.fn.selector.UriTemplateSelector;

import java.util.Arrays;
import java.util.concurrent.Callable;

/**
 * Helper methods to provide syntax sugar for working with functional components in Reactor.
 *
 * @author Jon Brisbin
 * @author Stephane Maldini
 * @author Andy Wilkinson
 */
public abstract class Fn {

	/**
	 * Creates an anonymous {@link Selector}.
	 *
	 * @return A {@link Tuple} containing the {@link Selector} and the object key.
	 * @see {@link reactor.fn.selector.BaseSelector}
	 */
	public static Tuple2<Selector, Object> $() {
		Object obj = new Object();
		return new Tuple2<Selector, Object>($(obj), obj);
	}

	/**
	 * Creates a {@link Selector} based on the given object.
	 *
	 * @param obj Can be anything.
	 * @return The new {@link Selector}.
	 * @see {@link reactor.fn.selector.BaseSelector}
	 */
	public static <T> Selector $(T obj) {
		return new BaseSelector<T>(obj);
	}

	/**
	 * Creates a {@link Selector} based on the given regular expression.
	 *
	 * @param regex The regular expression to compile.
	 * @return The new {@link Selector}.
	 * @see {@link reactor.fn.selector.RegexSelector}
	 */
	public static Selector R(String regex) {
		return new RegexSelector(regex);
	}

	/**
	 * Creates a {@link Selector} based on the given class type and only matches if the other Selector against which this
	 * is compared is assignable according to {@link Class#isAssignableFrom(Class)}.
	 *
	 * @param type The supertype to compare.
	 * @return The new {@link Selector}.
	 * @see {@link reactor.fn.selector.ClassSelector}
	 */
	public static Selector T(Class<?> type) {
		return new ClassSelector(type);
	}

	/**
	 * Creates a {@link Selector} based on a URI template.
	 *
	 * @param uriTemplate The URI template to compile.
	 * @return The new {@link Selector}.
	 * @see {@link reactor.fn.support.UriTemplate}
	 * @see {@link reactor.fn.selector.UriTemplateSelector}
	 */
	public static Selector U(String uriTemplate) {
		return new UriTemplateSelector(uriTemplate);
	}

	/**
	 * Wrap the given object with an {@link Event}.
	 *
	 * @param obj The object to wrap.
	 * @return The new {@link Event}.
	 */
	public static <T> Event<T> event(T obj) {
		return new Event<T>(obj);
	}

	/**
	 * Wrap the given object with an {@link Event} and set the {@link Event#replyTo} property to the given {@code key}.
	 *
	 * @param obj        The object to wrap.
	 * @param replyToKey The key to use as a {@literal replyTo}.
	 * @param <T>        The type of the given object.
	 * @return The new {@link Event}.
	 */
	public static <T> Event<T> event(T obj, Object replyToKey) {
		return new Event<T>(obj).setReplyTo(replyToKey);
	}


	/**
	 * Schedule an arbitrary {@link Consumer} to be executed on the given {@link Observable}, passing the given {@link
	 * Event}.
	 *
	 * @param consumer   The {@link Consumer} to invoke.
	 * @param data       The data to pass to the consumer.
	 * @param observable The {@literal Observable} that will be used to invoke the {@literal Consumer}
	 * @param <T>        The type of the data.
	 */
	public static <T> void schedule(final Consumer<T> consumer, T data, Observable observable) {
		observable.notify(Fn.event(Tuple.of(consumer, data)));
	}


	/**
	 * Return a null event
	 *
	 * @return The new {@link Event}.
	 */
	public static Event<Void> nullEvent() {
		return new Event<Void>(null);
	}


	/**
	 * Wrap the given {@link Callable} and compose a new {@link reactor.fn.Function}.
	 *
	 * @param c The {@link Callable}.
	 * @return An {@link reactor.fn.Consumer} that executes the {@link Callable}.
	 */
	public static <T> Function<? extends Event<T>, T> function(final Callable<T> c) {
		return new Function<Event<T>, T>() {
			@Override
			public T apply(Event<T> o) {
				try {
					return c.call();
				} catch (Exception e) {
					throw new IllegalStateException(e);
				}
			}
		};
	}


	/**
	 * Wrap the given {@link Runnable} and compose a new {@link reactor.fn.Consumer}.
	 *
	 * @param r The {@link Runnable}.
	 * @return An {@link reactor.fn.Consumer} that executes the {@link Runnable}.
	 */
	public static <T> Consumer<T> consumer(final Runnable r) {
		return new Consumer<T>() {
			@Override
			public void accept(T t) {
				r.run();
			}
		};
	}


	/**
	 * Create a delayed {@link Composable} with no initial state, ready to accept values.
	 *
	 * @return A {@link Composable.Builder} to further refine the {@link Composable} and then build it.
	 */
	public static <T> Composable.Builder<T> compose() {
		return new Composable.Builder<T>();
	}

	/**
	 * Create a {@link Composable} from the given value.
	 *
	 * @param value The value to use.
	 * @param <T>   The type of the value.
	 * @return A {@link Composable.Builder} to further refine the {@link Composable} and then build it.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Composable.Builder<T> compose(T value) {
		return new Composable.Builder<T>(Arrays.asList(value));
	}

	/**
	 * Create a {@link Composable} from the given list of values.
	 *
	 * @param values The values to use.
	 * @param <T>    The type of the values.
	 * @return A {@link Composable.Builder} to further refine the {@link Composable} and then build it.
	 */
	public static <T> Composable.Builder<T> compose(Iterable<T> values) {
		return new Composable.Builder<T>(values);
	}

	/**
	 * Create a {@link Composable} from the given {@link Supplier}.
	 *
	 * @param supplier The function to defer.
	 * @param <T>      The type of the values.
	 * @return A {@link Composable.Builder} to further refine the {@link Composable} and then build it.
	 */
	public static <T> Composable.Builder<T> compose(Supplier<T> supplier) {
		return new Composable.Builder<T>(supplier);
	}

	/**
	 * Create a {@literal Promise} based on the given exception.
	 *
	 * @param reason The exception to use as the value.
	 * @param <T>    The type of the intended {@literal Promise} value.
	 * @return The new {@literal Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise.Builder<T> promise(Throwable reason) {
		return (Promise.Builder<T>) new Promise.Builder<Throwable>(reason);
	}

	/**
	 * Create a {@literal Promise} based on the given value.
	 *
	 * @param value The value to use.
	 * @param <T>   The type of the value.
	 * @return The new {@literal Promise}.
	 */
	@SuppressWarnings("unchecked")
	public static <T> Promise.Builder<T> promise(T value) {
		return new Promise.Builder<T>(value);
	}

	/**
	 * Create a {@literal Promise} based on the given supplier.
	 *
	 * @param supplier The value to use.
	 * @param <T>      The type of the function result.
	 * @return The new {@literal Promise}.
	 */
	public static <T> Promise.Builder<T> promise(Supplier<T> supplier) {
		return new Promise.Builder<T>(supplier);
	}

}
