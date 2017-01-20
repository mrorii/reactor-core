/*
 * Copyright (c) 2011-2017 Pivotal Software Inc, All Rights Reserved.
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

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import org.junit.After;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.core.Loopback;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Fuseable.ASYNC;

public abstract class AbstractFluxOperatorTest<I, O> {

	public interface Scenario<I, O> {

		static <I, O> Scenario<I, O> from(Function<Flux<I>, Flux<O>> scenario) {
			return from(scenario, Fuseable.NONE);
		}

		static <I, O> Scenario<I, O> from(Function<Flux<I>, Flux<O>> scenario,
				int fusionMode) {
			return from(scenario, fusionMode, null);
		}

		@SuppressWarnings("unchecked")
		static <I, O> Scenario<I, O> from(Function<Flux<I>, Flux<O>> scenario,
				int fusionMode,
				Consumer<StepVerifier.Step<O>> verifier) {
			return from(scenario,
					fusionMode,
					(Flux<I>) Flux.just("dropped", "test2", "test3"),
					verifier);
		}

		static <I, O> Scenario<I, O> from(Function<Flux<I>, Flux<O>> scenario,
				int fusionMode,
				Flux<I> finiteSource,
				Consumer<StepVerifier.Step<O>> verifier) {
			return new SimpleScenario<>(scenario, fusionMode, finiteSource, verifier);
		}

		Function<Flux<I>, Flux<O>> body();

		int fusionMode();

		Consumer<StepVerifier.Step<O>> verifier();

		Flux<I> finiteSource();
	}

	static final class SimpleScenario<I, O> implements Scenario<I, O> {

		final Function<Flux<I>, Flux<O>>     scenario;
		final int                            fusionMode;
		final Consumer<StepVerifier.Step<O>> verifier;
		final Flux<I>                        finiteSource;

		SimpleScenario(Function<Flux<I>, Flux<O>> scenario,
				int fusionMode,
				Flux<I> finiteSource,
				Consumer<StepVerifier.Step<O>> verifier) {
			this.scenario = scenario;
			this.fusionMode = fusionMode;
			this.verifier = verifier;
			this.finiteSource = finiteSource;
		}

		@Override
		public Function<Flux<I>, Flux<O>> body() {
			return scenario;
		}

		@Override
		public int fusionMode() {
			return fusionMode;
		}

		@Override
		public Consumer<StepVerifier.Step<O>> verifier() {
			return verifier;
		}

		@Override
		public Flux<I> finiteSource() {
			return finiteSource;
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public final void assertPrePostState() {
		for (Scenario<I, O> scenario : scenarios_touchAndAssertState()) {
			if (scenario == null) {
				continue;
			}

			Flux<O> f = Flux.<I>from(s -> {
				Trackable t = null;
				if (s instanceof Trackable) {
					t = (Trackable) s;
					assertThat(t.getError()).isNull();
					assertThat(t.isStarted()).isFalse();
					assertThat(t.isTerminated()).isFalse();
				}

				if (s instanceof Receiver) {
					assertThat(((Receiver) s).upstream()).isNull();
				}

				if (s instanceof Loopback) {
					assertThat(((Loopback) s).connectedInput()).isNotNull();
				}

				if (s instanceof Producer) {
					assertThat(((Producer) s).downstream()).isNotNull();
				}

				s.onSubscribe(Operators.emptySubscription());
				s.onSubscribe(Operators.emptySubscription()); //noop path
				if (t != null) {
					assertThat(t.isStarted()).isTrue();
				}
				s.onComplete();
				if (t != null) {
					assertThat(t.isStarted()).isFalse();
					assertThat(t.isTerminated()).isTrue();
				}
			}).as(scenario.body());

			f.subscribe();

			f.filter(d -> true)
			 .subscribe();

			AtomicReference<Trackable> ref = new AtomicReference<>();
			f = scenario.finiteSource()
			            .doOnSubscribe(s -> {
				            Object _s =
						            ((Producer) ((Producer) s).downstream()).downstream();
				            if (_s instanceof Trackable) {
					            Trackable t = (Trackable) _s;
					            ref.set(t);
					            assertThat(t.isStarted()).isFalse();
				            }
			            })
			            .as(scenario.body())
			            .doOnSubscribe(parent -> {
				            if (parent instanceof Trackable) {
					            Trackable t = (Trackable) parent;
					            assertThat(t.getError()).isNull();
					            assertThat(t.isStarted()).isTrue();
					            assertThat(t.isTerminated()).isFalse();
				            }

				            //noop path
				            if (parent instanceof Subscriber) {
					            ((Subscriber<I>) parent).onSubscribe(Operators.emptySubscription());
				            }

				            if (parent instanceof Receiver) {
					            assertThat(((Receiver) parent).upstream()).isNotNull();
				            }

				            if (parent instanceof Loopback) {
					            assertThat(((Loopback) parent).connectedInput()).isNotNull();
				            }

				            if (parent instanceof Producer) {
					            assertThat(((Producer) parent).downstream()).isNotNull();
				            }
			            })
			            .doOnComplete(() -> {
				            if (ref.get() != null) {
					            assertThat(ref.get()
					                          .isStarted()).isFalse();
					            assertThat(ref.get()
					                          .isTerminated()).isTrue();
				            }
			            });

			f.subscribe();

			f.filter(t -> true)
			 .subscribe();

			resetHooks();
		}
	}

	@Test
	public final void errorWithUserProvidedCallback() {
		for (Scenario<I, O> scenario : scenarios_errorInOperatorCallback()) {
			if (scenario == null) {
				continue;
			}

			Consumer<StepVerifier.Step<O>> verifier = scenario.verifier();

			if (verifier == null) {
				verifier = step -> step.verifyErrorMessage("dropped");
			}

			int fusion = scenario.fusionMode();

			verifier.accept(this.operatorErrorVerifier(scenario));

			verifier.accept(this.operatorErrorVerifierFused(scenario));

			if ((fusion & Fuseable.SYNC) != 0) {
				verifier.accept(this.operatorErrorVerifierFusedSync(scenario));
				verifier.accept(this.operatorErrorVerifierFusedConditionalSync(scenario));
			}

			if ((fusion & Fuseable.ASYNC) != 0) {
				verifier.accept(this.operatorErrorVerifierFusedAsync(scenario));
				verifier.accept(this.operatorErrorVerifierFusedConditionalAsync(scenario));
				this.operatorErrorVerifierFusedAsyncState(scenario);
				this.operatorErrorVerifierFusedConditionalAsyncState(scenario);
			}

			verifier.accept(this.operatorErrorVerifierTryNext(scenario));
			verifier.accept(this.operatorErrorVerifierBothConditional(scenario));
			verifier.accept(this.operatorErrorVerifierConditionalTryNext(scenario));
			verifier.accept(this.operatorErrorVerifierFusedTryNext(scenario));
			verifier.accept(this.operatorErrorVerifierFusedBothConditional(scenario));
			verifier.accept(this.operatorErrorVerifierFusedBothConditionalTryNext(scenario));

			resetHooks();
		}
	}

	@Test
	public final void errorWithUpstreamFailure() {
		for (Scenario<I, O> scenario : scenarios_errorFromUpstreamFailure()) {
			if (scenario == null) {
				continue;
			}

			Consumer<StepVerifier.Step<O>> verifier = scenario.verifier();

			if (verifier == null) {
				verifier = step -> step.verifyErrorMessage("dropped");
			}

			verifier.accept(this.operatorErrorSourceVerifier(scenario));
			verifier.accept(this.operatorErrorSourceVerifierTryNext(scenario));
			verifier.accept(this.operatorErrorSourceVerifierFused(scenario));
			verifier.accept(this.operatorErrorSourceVerifierConditional(scenario));
			verifier.accept(this.operatorErrorSourceVerifierConditionalTryNext(scenario));
			verifier.accept(this.operatorErrorSourceVerifierFusedBothConditional(scenario));

			resetHooks();
		}
	}

	@Test
	public final void threeNextAndComplete() {
		for (Scenario<I, O> scenario : scenarios_threeNextAndComplete()) {
			if (scenario == null) {
				continue;
			}

			Consumer<StepVerifier.Step<O>> verifier = scenario.verifier();

			if (verifier == null) {
				continue;
			}

			verifier.accept(this.operatorErrorSourceVerifier(scenario));
			verifier.accept(this.operatorErrorSourceVerifierTryNext(scenario));
			verifier.accept(this.operatorErrorSourceVerifierFused(scenario));
			verifier.accept(this.operatorErrorSourceVerifierConditional(scenario));
			verifier.accept(this.operatorErrorSourceVerifierConditionalTryNext(scenario));
			verifier.accept(this.operatorErrorSourceVerifierFusedBothConditional(scenario));

			resetHooks();
		}
	}


	//errorInOperatorCallbackVerification
	protected List<Scenario<I, O>> scenarios_errorInOperatorCallback() {
		return Collections.emptyList();
	}

	//errorInOperatorCallbackVerification
	protected List<Scenario<I, O>> scenarios_threeNextAndComplete() {
		return Collections.emptyList();
	}

	//assert
	protected List<Scenario<I, O>> scenarios_touchAndAssertState() {
		return scenarios_errorFromUpstreamFailure();
	}

	//errorFromUpstreamFailureVerification
	protected List<Scenario<I, O>> scenarios_errorFromUpstreamFailure() {
		return Collections.emptyList();
	}

	//common source emitting once
	@SuppressWarnings("unchecked")
	protected void testPublisherSource(TestPublisher<I> ts) {
		ts.next(singleItem());
		ts.next(singleItem());
	}

	//common first unused item or dropped
	@SuppressWarnings("unchecked")
	protected I singleItem() {
		return (I) "dropped";
	}

	final StepVerifier.Step<O> operatorErrorVerifier(Scenario<I, O> scenario) {
		Hooks.onErrorDropped(e -> assertThat(e).hasMessage("dropped"));
		return StepVerifier.create(scenario.finiteSource().hide()
		                                                 .as(scenario.body()), 2);
	}

	final StepVerifier.Step<O> operatorErrorVerifierTryNext(Scenario<I, O> scenario) {
		TestPublisher<I> ts = TestPublisher.create();

		return StepVerifier.create(ts.flux()
		                             .as(scenario.body()))
		                   .then(() -> testPublisherSource(ts));
	}

	final StepVerifier.Step<O> operatorErrorSourceVerifierTryNext(Scenario<I, O> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		AtomicBoolean errorDropped = new AtomicBoolean();
		AtomicBoolean nextDropped = new AtomicBoolean();

		Hooks.onErrorDropped(e -> {
			assertThat(e).hasMessage("dropped");
			errorDropped.set(true);
		});
		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo("dropped");
			nextDropped.set(true);
		});
		return StepVerifier.create(ts.flux()
		                             .as(scenario.body()))
		                   .then(() -> {
			                   ts.error(new Exception("dropped"));

			                   //verify drop path
			                   ts.error(new Exception("dropped"));
			                   ts.next(singleItem());
			                   ts.complete();
			                   assertThat(errorDropped.get()).isTrue();
			                   assertThat(nextDropped.get()).isTrue();
		                   });
	}

	final StepVerifier.Step<O> operatorErrorSourceVerifier(Scenario<I, O> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		AtomicBoolean nextDropped = new AtomicBoolean();

		Hooks.onErrorDropped(d -> {
			assertThat(d).hasMessage("dropped");
		});
		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo("dropped");
			nextDropped.set(true);
		});
		return StepVerifier.create(ts.flux()
		                             .hide()
		                             .as(scenario.body()))
		                   .then(() -> {
			                   ts.error(new Exception("dropped"));

			                   //verify drop path
			                   ts.next(singleItem());
			                   assertThat(nextDropped.get()).isTrue();
		                   });
	}

	final StepVerifier.Step<O> operatorErrorVerifierFused(Scenario<I, O> scenario) {
		return StepVerifier.create(scenario.finiteSource()
		                                   .as(scenario.body()));
	}

	final StepVerifier.Step<O> operatorErrorVerifierFusedSync(Scenario<I, O> scenario) {
		return StepVerifier.create(scenario.finiteSource()
		                                   .as(scenario.body()))
		                   .expectFusion(Fuseable.SYNC);
	}

	final StepVerifier.Step<O> operatorErrorVerifierFusedConditionalSync(Scenario<I, O> scenario) {
		return StepVerifier.create(scenario.finiteSource()
		                                   .as(scenario.body())
		                                   .filter(d -> true))
		                   .expectFusion(Fuseable.SYNC);
	}

	final StepVerifier.Step<O> operatorErrorVerifierFusedAsync(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		return StepVerifier.create(up.as(scenario.body()))
		                   .expectFusion(Fuseable.ASYNC)
		                   .then(() -> up.onNext(singleItem()));
	}

	@SuppressWarnings("unchecked")
	final void operatorErrorVerifierFusedAsyncState(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		up.onNext(singleItem());
		up.onNext(singleItem());
		up.onNext(singleItem());
		StepVerifier.create(up.as(scenario.body()))
		            .consumeSubscriptionWith(s -> {
			            if (s instanceof Fuseable.QueueSubscription) {
				            Fuseable.QueueSubscription<O> qs =
						            ((Fuseable.QueueSubscription<O>) s);
				            qs.requestFusion(ASYNC);
				            assertThat(qs.size()).isEqualTo(3);
				            try {
				            	qs.poll();
				            	qs.poll();
				            }
				            catch (Exception e) {
				            }
				            if (qs instanceof Trackable && ((Trackable) qs).getError() != null) {
					            assertThat(((Trackable) qs).getError()).hasMessage(
							            "dropped");
				            }
				            assertThat(qs.size()).isEqualTo(2);
				            qs.clear();
				            assertThat(qs.size()).isEqualTo(0);
			            }
		            })
		            .thenCancel()
		            .verify();
	}

	@SuppressWarnings("unchecked")
	final void operatorErrorVerifierFusedConditionalAsyncState(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		up.onNext(singleItem());
		up.onNext(singleItem());
		up.onNext(singleItem());
		StepVerifier.create(up.as(scenario.body())
		                      .filter(d -> true))
		            .consumeSubscriptionWith(s -> {
			            if (s instanceof Fuseable.QueueSubscription) {
				            Fuseable.QueueSubscription<O> qs =
						            ((Fuseable.QueueSubscription<O>) ((Receiver) s).upstream());
				            qs.requestFusion(ASYNC);
				            assertThat(qs.size()).isEqualTo(3);
				            try {
					            qs.poll();
					            qs.poll();
				            }
				            catch (Exception e) {
				            }
				            if (qs instanceof Trackable && ((Trackable) qs).getError() != null) {
					            assertThat(((Trackable) qs).getError()).hasMessage(
							            "dropped");
				            }
				            assertThat(qs.size()).isEqualTo(2);
				            qs.clear();
				            assertThat(qs.size()).isEqualTo(0);
			            }
		            })
		            .thenCancel()
		            .verify();
	}

	final StepVerifier.Step<O> operatorErrorVerifierFusedConditionalAsync(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		return StepVerifier.create(up.as(scenario.body())
		                             .filter(d -> true))
		                   .expectFusion(Fuseable.ASYNC)
		                   .then(() -> up.onNext(singleItem()));
	}

	final StepVerifier.Step<O> operatorErrorVerifierFusedTryNext(Scenario<I, O> scenario) {
		return StepVerifier.create(scenario.finiteSource()
		                                   .as(scenario.body()), 2);
	}

	@SuppressWarnings("unchecked")
	final StepVerifier.Step<O> operatorErrorSourceVerifierFused(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		AtomicBoolean errorDropped = new AtomicBoolean();
		AtomicBoolean nextDropped = new AtomicBoolean();
		Hooks.onErrorDropped(e -> {
			assertThat(e).hasMessage("dropped");
			errorDropped.set(true);
		});
		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo("dropped");
			nextDropped.set(true);
		});
		return StepVerifier.create(up.as(scenario.body()))
		                   .then(() -> {
			                   up.actual.onError(new Exception("dropped"));

			                   //verify drop path
			                   up.actual.onError(new Exception("dropped"));
			                   assertThat(errorDropped.get()).isTrue();

			                   up.actual.onNext(singleItem());
			                   assertThat(nextDropped.get()).isTrue();
			                   if (up.actual instanceof Fuseable.ConditionalSubscriber) {
				                   nextDropped.set(false);
				                   ((Fuseable.ConditionalSubscriber<I>) up.actual).tryOnNext(
						                   singleItem());
				                   assertThat(nextDropped.get()).isTrue();
			                   }
			                   up.actual.onComplete();
		                   });
	}

	final StepVerifier.Step<O> operatorErrorVerifierConditionalTryNext(Scenario<I, O> scenario) {
		return StepVerifier.create(scenario.finiteSource()
		                                   .hide()
		                                   .as(scenario.body())
		                                   .filter(d -> true), 2);
	}

	final StepVerifier.Step<O> operatorErrorVerifierBothConditional(Scenario<I, O> scenario) {
		TestPublisher<I> ts = TestPublisher.create();

		return StepVerifier.create(ts.flux()
		                             .as(scenario.body())
		                             .filter(d -> true))
		                   .then(() -> testPublisherSource(ts));
	}

	final StepVerifier.Step<O> operatorErrorVerifierFusedBothConditional(Scenario<I, O> scenario) {
		return StepVerifier.create(scenario.finiteSource()
		                                   .as(scenario.body())
		                                   .filter(d -> true));
	}

	final StepVerifier.Step<O> operatorErrorSourceVerifierConditionalTryNext(Scenario<I, O> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		AtomicBoolean errorDropped = new AtomicBoolean();
		AtomicBoolean nextDropped = new AtomicBoolean();

		Hooks.onErrorDropped(e -> {
			assertThat(e).hasMessage("dropped");
			errorDropped.set(true);
		});
		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo("dropped");
			nextDropped.set(true);
		});
		return StepVerifier.create(ts.flux()
		                             .as(scenario.body())
		                             .filter(d -> true))
		                   .then(() -> {
			                   ts.error(new Exception("dropped"));

			                   //verify drop path
			                   ts.error(new Exception("dropped"));
			                   ts.next(singleItem());
			                   ts.complete();
			                   assertThat(errorDropped.get()).isTrue();
			                   assertThat(nextDropped.get()).isTrue();
		                   });
	}

	final StepVerifier.Step<O> operatorErrorSourceVerifierConditional(Scenario<I, O> scenario) {
		TestPublisher<I> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		AtomicBoolean nextDropped = new AtomicBoolean();

		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo("dropped");
			nextDropped.set(true);
		});
		return StepVerifier.create(ts.flux()
		                             .hide()
		                             .as(scenario.body())
		                             .filter(d -> true))
		                   .then(() -> {
			                   ts.error(new Exception("dropped"));

			                   //verify drop path
			                   ts.next(singleItem());
			                   assertThat(nextDropped.get()).isTrue();
		                   });
	}

	final StepVerifier.Step<O> operatorErrorVerifierFusedBothConditionalTryNext(Scenario<I, O> scenario) {
		return StepVerifier.create(scenario.finiteSource()
		                                   .as(scenario.body())
		                                   .filter(d -> true), 2);
	}

	@SuppressWarnings("unchecked")
	final StepVerifier.Step<O> operatorErrorSourceVerifierFusedBothConditional(Scenario<I, O> scenario) {
		UnicastProcessor<I> up = UnicastProcessor.create();
		AtomicBoolean errorDropped = new AtomicBoolean();
		AtomicBoolean nextDropped = new AtomicBoolean();
		Hooks.onErrorDropped(e -> {
			assertThat(e).hasMessage("dropped");
			errorDropped.set(true);
		});
		Hooks.onNextDropped(d -> {
			assertThat(d).isEqualTo("dropped");
			nextDropped.set(true);
		});
		return StepVerifier.create(up.as(scenario.body())
		                             .filter(d -> true))
		                   .then(() -> {
			                   up.actual.onError(new Exception("dropped"));

			                   //verify drop path
			                   up.actual.onError(new Exception("dropped"));
			                   assertThat(errorDropped.get()).isTrue();

			                   up.actual.onNext(singleItem());
			                   assertThat(nextDropped.get()).isTrue();

			                   if (up.actual instanceof Fuseable.ConditionalSubscriber) {
				                   nextDropped.set(false);
				                   ((Fuseable.ConditionalSubscriber<I>) up.actual).tryOnNext(
						                   singleItem());
				                   assertThat(nextDropped.get()).isTrue();
			                   }
			                   up.actual.onComplete();
		                   });
	}

	@After
	public void resetHooks() {
		Hooks.resetOnErrorDropped();
		Hooks.resetOnNextDropped();
		Hooks.resetOnOperator();
		Hooks.resetOnOperatorError();
	}
}
