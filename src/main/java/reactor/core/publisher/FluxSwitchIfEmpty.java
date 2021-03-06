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

import java.util.Objects;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import reactor.core.Loopback;

/**
 * Switches to another source if the first source turns out to be empty.
 *
 * @param <T> the value type
 * @see <a href="https://github.com/reactor/reactive-streams-commons">Reactive-Streams-Commons</a>
 */
final class FluxSwitchIfEmpty<T> extends FluxSource<T, T> {

	final Publisher<? extends T> other;

	public FluxSwitchIfEmpty(Publisher<? extends T> source,
			Publisher<? extends T> other) {
		super(source);
		this.other = Objects.requireNonNull(other, "other");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		SwitchIfEmptySubscriber<T> parent = new SwitchIfEmptySubscriber<>(s, other);

		s.onSubscribe(parent);

		source.subscribe(parent);
	}

	static final class SwitchIfEmptySubscriber<T>
			extends Operators.MultiSubscriptionSubscriber<T, T> implements Loopback {

		final Publisher<? extends T> other;

		boolean once;

		public SwitchIfEmptySubscriber(Subscriber<? super T> actual,
				Publisher<? extends T> other) {
			super(actual);
			this.other = other;
		}

		@Override
		public void onNext(T t) {
			if (!once) {
				once = true;
			}

			subscriber.onNext(t);
		}

		@Override
		public void onComplete() {
			if (!once) {
				once = true;

				other.subscribe(this);
			}
			else {
				subscriber.onComplete();
			}
		}

		@Override
		public Object connectedInput() {
			return null;
		}

		@Override
		public Object connectedOutput() {
			return other;
		}
	}
}
