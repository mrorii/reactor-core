/*
 * Copyright (c) 2011-2016 Pivotal Software Inc, All Rights Reserved.
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

package reactor.core.publisher;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.Cancellation;
import reactor.core.scheduler.Scheduler;

/**
 * Schedules the emission of the value or completion of the wrapped Mono via
 * the given Scheduler.
 *
 * @param <T> the value type
 */
final class MonoPublishOn<T> extends MonoSource<T, T> {

	final Scheduler scheduler;

	public MonoPublishOn(Publisher<? extends T> source, Scheduler scheduler) {
		super(source);
		this.scheduler = scheduler;
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		source.subscribe(new MonoPublishOnSubscriber<T>(s, scheduler));
	}

	static final class MonoPublishOnSubscriber<T>
			implements Subscriber<T>, Subscription, Runnable {

		final Subscriber<? super T> actual;

		final Scheduler scheduler;

		Subscription s;

		volatile Cancellation future;
		@SuppressWarnings("rawtypes")
		static final AtomicReferenceFieldUpdater<MonoPublishOnSubscriber, Cancellation>
				FUTURE =
				AtomicReferenceFieldUpdater.newUpdater(MonoPublishOnSubscriber.class,
						Cancellation.class,
						"future");

		static final Cancellation CANCELLED = () -> {
		};

		T         value;
		Throwable error;

		public MonoPublishOnSubscriber(Subscriber<? super T> actual,
				Scheduler scheduler) {
			this.actual = actual;
			this.scheduler = scheduler;
		}

		@Override
		public void onSubscribe(Subscription s) {
			if (Operators.validate(this.s, s)) {
				this.s = s;

				actual.onSubscribe(this);
			}
		}

		@Override
		public void onNext(T t) {
			value = t;
			if(schedule() == Scheduler.REJECTED){
				throw Operators.onRejectedExecution(this, null, t);
			}
		}

		@Override
		public void onError(Throwable t) {
			error = t;
			if (schedule() == Scheduler.REJECTED) {
				throw Operators.onRejectedExecution(null, t, null);
			}
		}

		@Override
		public void onComplete() {
			if (value == null) {
				if (schedule() == Scheduler.REJECTED && future != CANCELLED) {
					throw Operators.onRejectedExecution();
				}
			}
		}

		Cancellation schedule() {
			if (future == null) {
				Cancellation c = scheduler.schedule(this);
				if (!FUTURE.compareAndSet(this, null, c)) {
					c.dispose();
				}
				return c;
			}
			return null;
		}

		@Override
		public void request(long n) {
			s.request(n);
		}

		@Override
		public void cancel() {
			Cancellation c = future;
			if (c != CANCELLED) {
				c = FUTURE.getAndSet(this, CANCELLED);
				if (c != null && c != CANCELLED) {
					c.dispose();
				}
			}
			s.cancel();
		}

		@Override
		public void run() {
			if (future == CANCELLED) {
				return;
			}
			T v = value;
			value = null;
			if (v != null) {
				actual.onNext(v);
			}

			if (future == CANCELLED) {
				return;
			}
			Throwable e = error;
			if (e != null) {
				actual.onError(e);
			}
			else {
				actual.onComplete();
			}
		}
	}
}
