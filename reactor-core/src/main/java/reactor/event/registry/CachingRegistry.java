/*
 * Copyright (c) 2011-2013 GoPivotal, Inc. All Rights Reserved.
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

package reactor.event.registry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.event.lifecycle.Lifecycle;
import reactor.event.selector.Selector;
import reactor.util.ZeroCopyList;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * An optimized selectors registry working with a L1 Cache and ReadWrite reentrant locks.
 *
 * @param <T> the type of Registration held by this registry
 * @author Jon Brisbin
 * @author Andy Wilkinson
 * @author Stephane Maldini
 */
public class CachingRegistry<T> implements Registry<T> {

	private final ReentrantReadWriteLock                       readWriteLock           = new ReentrantReadWriteLock(true);
	private final Lock                                         readLock                = readWriteLock.readLock();
	private final Lock                                         writeLock               = readWriteLock.writeLock();
	private final List<Registration<? extends T>>              registrations           = new ArrayList<Registration<? extends
			T>>();
	private final Map<Object, List<Registration<? extends T>>> registrationCache       = new HashMap<Object,
			List<Registration<? extends T>>>();
	private final Map<Object, List<Registration<? extends T>>> directRegistrationCache = new HashMap<Object,
			List<Registration<? extends T>>>();
	private final Logger                                       log                     = LoggerFactory.getLogger
			(CachingRegistry.class);
	private final boolean cache;

	public CachingRegistry() {
		this(true);
	}

	public CachingRegistry(boolean cache) {
		this.cache = cache;
	}

	private boolean refreshRequired;

	@Override
	public <V extends T> Registration<V> register(Selector sel, V obj) {
		CachableRegistration<V> reg = new CachableRegistration<V>(sel, obj);

		writeLock.lock();
		try {
			Object key = sel.getObject();
			if (Object.class == key.getClass()) {
				List<Registration<? extends T>> registrationList = directRegistrationCache.get(key);
				if (registrationList == null) {
					registrationList = new ArrayList<Registration<? extends T>>();
					directRegistrationCache.put(key, registrationList);
				}
				registrationList.add(reg);
			} else {
				refreshRequired = true;
			}
			registrations.add(reg);
		} finally {
			writeLock.unlock();
		}

		return reg;
	}

	@Override
	public boolean unregister(Object key) {

		assert key != null;

		writeLock.lock();
		try {
			if (registrations.isEmpty()) {
				return false;
			}

			List<Registration<? extends T>> regs = findMatchingRegistrations(key);

			if (regs != null) {
				registrations.removeAll(regs);
				refreshRequired = true;
				return true;
			} else {
				return false;
			}
		} finally {
			writeLock.unlock();
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public List<Registration<? extends T>> select(Object key) {
		List<Registration<? extends T>> matchingRegistrations;

		readLock.lock();

		try {
			if (key != null && key.getClass().equals(Object.class)) {
				matchingRegistrations = directRegistrationCache.get(key);
			} else {
				if (refreshRequired) {
					readLock.unlock();
					writeLock.lock();
					try {
						if (refreshRequired) {
							registrationCache.clear();
							refreshRequired = false;
						}
					} finally {
						readLock.lock();
						writeLock.unlock();
					}
				}

				matchingRegistrations = registrationCache.get(key);

				if (null == matchingRegistrations) {
					readLock.unlock();
					writeLock.lock();
					try {
						matchingRegistrations = registrationCache.get(key);
						if (null == matchingRegistrations) {
							matchingRegistrations = find(key);
						}
					} finally {
						readLock.lock();
						writeLock.unlock();
					}
				}
			}
		} finally {
			readLock.unlock();
		}

		return matchingRegistrations == null ? Collections.EMPTY_LIST :
				new ArrayList<Registration<? extends T>>(matchingRegistrations);
	}

	@Override
	public Iterator<Registration<? extends T>> iterator() {
		try {
			readLock.lock();
			return new ArrayList<Registration<? extends T>>(this.registrations).iterator();
		} finally {
			readLock.unlock();
		}
	}

	private List<Registration<? extends T>> find(Object object) {
		cacheMiss(object);
		try {
			writeLock.lock();

			List<Registration<? extends T>> regs;

			if (registrations.isEmpty()) {
				regs = null;
			} else {
				regs = findMatchingRegistrations(object);
			}

			if (cache && regs != null) {
				registrationCache.put(object, regs);
			}

			return regs;
		} finally {
			writeLock.unlock();
		}
	}

	private List<Registration<? extends T>> findMatchingRegistrations(Object object) {
		List<Registration<? extends T>> regs = new ArrayList<Registration<? extends T>>();
		boolean added = false;
		for (Registration<? extends T> reg : registrations) {
			if (reg.getSelector().matches(object)) {
				regs.add(reg);
				added = true;
			}
		}
		if (!added) {
			if (log.isTraceEnabled()) {
				log.trace("No objects registered for key {}", object);
			}
			return null;
		} else {
			return regs;
		}
	}

	protected void cacheMiss(Object key) {
	}

	@Override
	public void clear() {
		writeLock.lock();
		try {
			registrationCache.clear();
			directRegistrationCache.clear();
			registrations.clear();
		} finally {
			writeLock.unlock();
		}
	}

	private class CachableRegistration<V> implements Registration<V> {
		private final Selector selector;
		private final V        object;
		private volatile boolean cancelAfterUse = false;
		private volatile boolean cancelled      = false;
		private volatile boolean paused         = false;

		private CachableRegistration(Selector selector, V object) {
			this.selector = selector;
			this.object = object;
		}

		@Override
		public Selector getSelector() {
			return selector;
		}

		@Override
		public V getObject() {
			return object;
		}

		@Override
		public Registration<V> cancelAfterUse() {
			cancelAfterUse = !cancelAfterUse;
			return this;
		}

		@Override
		public boolean isCancelAfterUse() {
			return cancelAfterUse;
		}

		@Override
		public Registration<V> cancel() {
			this.cancelled = true;

			writeLock.lock();
			try {
				registrations.remove(CachableRegistration.this);
				if (selector.getObject().getClass().equals(Object.class)) {
					List<Registration<? extends T>> registrationList = directRegistrationCache.get(selector.getObject());
					registrationList.remove(CachableRegistration.this);
					if (registrationList.isEmpty()) {
						directRegistrationCache.remove(selector.getObject());
					}
				} else {
					refreshRequired = true;
				}
				if (Lifecycle.class.isAssignableFrom(object.getClass())) {
					((Lifecycle) object).cancel();
				}
			} finally {
				writeLock.unlock();
			}

			return this;
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		public Registration<V> pause() {
			paused = true;
			if (Lifecycle.class.isAssignableFrom(object.getClass())) {
				((Lifecycle) object).pause();
			}
			return this;
		}

		@Override
		public boolean isPaused() {
			return paused;
		}

		@Override
		public Registration<V> resume() {
			paused = false;
			if (Lifecycle.class.isAssignableFrom(object.getClass())) {
				((Lifecycle) object).resume();
			}
			return this;
		}
	}

}
