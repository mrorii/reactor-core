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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.Assert;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import reactor.core.Fuseable;
import reactor.core.Loopback;
import reactor.core.Producer;
import reactor.core.Receiver;
import reactor.core.Trackable;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.test.subscriber.AssertSubscriber;

import static org.assertj.core.api.Assertions.assertThat;
import static reactor.core.Fuseable.ASYNC;
import static reactor.core.Fuseable.SYNC;

public class FluxHandleTest {

	@Test
	public void normal() {
		Set<Integer> expectedValues = new HashSet<>(Arrays.asList(2, 4, 6, 8, 10));

		Flux.range(1, 5)
		    .handle((v, s) -> s.next(v * 2))
		    .subscribeWith(AssertSubscriber.create())
		    .assertContainValues(expectedValues)
		    .assertNoError()
		    .assertComplete();
	}

	@Test
	public void normalHide() {
		Set<Integer> expectedValues = new HashSet<>(Arrays.asList(2, 4, 6, 8, 10));

		Flux.range(1, 5)
		    .hide()
		    .handle((v, s) -> s.next(v * 2))
		    .subscribeWith(AssertSubscriber.create())
		    .assertContainValues(expectedValues)
		    .assertNoError()
		    .assertComplete();
	}

	@Test
	public void filterNullMapResult() {
		Set<Integer> expectedValues = new HashSet<>(Arrays.asList(4, 8));

		Flux.range(1, 5)
		    .handle((v, s) -> {
			    if (v % 2 == 0) {
				    s.next(v * 2);
			    }
		    })
		    .subscribeWith(AssertSubscriber.create())
		    .assertContainValues(expectedValues)
		    .assertNoError()
		    .assertComplete();
	}

	@Test
	public void normalSyncFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		Set<Integer> expectedValues = new HashSet<>(Arrays.asList(2, 4, 6, 8, 10));
		ts.requestedFusionMode(SYNC);

		Flux.range(1, 5).<Integer>handle((v, s) -> s.next(v * 2)).subscribe(ts);

		ts.assertContainValues(expectedValues)
		  .assertNoError()
		  .assertComplete()
		  .assertFuseableSource()
		  .assertFusionMode(SYNC);
	}

	@Test
	public void normalAsyncFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		ts.requestedFusionMode(ASYNC);

		Flux.range(1,
				5).<Integer>handle((v, s) -> s.next(v * 2)).publishOn(Schedulers.single())
		                                                   .subscribe(ts);

		Set<Integer> expectedValues = new HashSet<>(Arrays.asList(2, 4, 6, 8, 10));
		ts.await()
		  .assertContainValues(expectedValues)
		  .assertNoError()
		  .assertComplete()
		  .assertFuseableSource()
		  .assertFusionMode(ASYNC);
	}

	@Test
	public void filterNullMapResultSyncFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		ts.requestedFusionMode(SYNC);

		Flux.range(1, 5).<Integer>handle((v, s) -> {
			if (v % 2 == 0) {
				s.next(v * 2);
			}
		}).subscribe(ts);

		Set<Integer> expectedValues = new HashSet<>(Arrays.asList(4, 8));
		ts.assertContainValues(expectedValues)
		  .assertNoError()
		  .assertComplete()
		  .assertFuseableSource()
		  .assertFusionMode(SYNC);
	}

	@Test
	public void filterNullMapResultAsyncFusion() {
		AssertSubscriber<Integer> ts = AssertSubscriber.create();
		ts.requestedFusionMode(ASYNC);

		Flux.range(1, 5).<Integer>handle((v, s) -> {
			if (v % 2 == 0) {
				s.next(v * 2);
			}
		}).publishOn(Schedulers.single())
		  .subscribe(ts);

		Set<Integer> expectedValues = new HashSet<>(Arrays.asList(4, 8));
		ts.await()
		  .assertContainValues(expectedValues)
		  .assertNoError()
		  .assertComplete()
		  .assertFuseableSource()
		  .assertFusionMode(ASYNC);
	}

	@Test
	public void errorSignal() {

		int data = 1;
		Exception exception = new IllegalStateException();

		final AtomicReference<Throwable> throwableInOnOperatorError =
				new AtomicReference<>();
		final AtomicReference<Object> dataInOnOperatorError = new AtomicReference<>();

		try {
			Hooks.onOperatorError((t, d) -> {
				throwableInOnOperatorError.set(t);
				dataInOnOperatorError.set(d);
				return t;
			});

			AssertSubscriber<Integer> ts = AssertSubscriber.create();

			Flux.just(data).<Integer>handle((v, s) -> s.error(exception)).subscribe(ts);

			ts.await()
			  .assertNoValues()
			  .assertError(IllegalStateException.class)
			  .assertNotComplete();

			Assert.assertSame(throwableInOnOperatorError.get(), exception);
			Assert.assertSame(dataInOnOperatorError.get(), data);
		}
		finally {
			Hooks.resetOnOperatorError();
		}
	}

	@Test
	public void errorPropagated() {

		int data = 1;
		IllegalStateException exception = new IllegalStateException();

		final AtomicReference<Throwable> throwableInOnOperatorError =
				new AtomicReference<>();
		final AtomicReference<Object> dataInOnOperatorError = new AtomicReference<>();

		try {
			Hooks.onOperatorError((t, d) -> {
				throwableInOnOperatorError.set(t);
				dataInOnOperatorError.set(d);
				return t;
			});

			AssertSubscriber<Integer> ts = AssertSubscriber.create();

			Flux.just(data).<Integer>handle((v, s) -> {
				throw exception;
			}).subscribe(ts);

			ts.await()
			  .assertNoValues()
			  .assertError(IllegalStateException.class)
			  .assertNotComplete();

			Assert.assertSame(throwableInOnOperatorError.get(), exception);
			Assert.assertSame(dataInOnOperatorError.get(), data);
		}
		finally {
			Hooks.resetOnOperatorError();
		}
	}

	@Test
	public void handle() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .hide()
		                        .handle((s, d) -> {
			                        if ("test3".equals(s)) {
				                        d.complete();
			                        }
			                        else {
				                        d.next(s);
			                        }
		                        }))
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
	public void handleCancel() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .hide()
		                        .as(this::passThrough), 2)
		            .expectNext("test", "test2")
		            .thenCancel()
		            .verify();
	}

	@Test
	public void handleFusedTryNext() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .handle((s, d) -> {
			                        if ("test3".equals(s)) {
				                        d.complete();
			                        }
			                        else {
				                        d.next(s);
			                        }
		                        }), 3)
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
	public void handleFused() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .handle((s, d) -> {
			                        if ("test3".equals(s)) {
				                        d.complete();
			                        }
			                        else {
				                        d.next(s);
			                        }
		                        }))
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
	public void handleFusedSync() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .handle((s, d) -> {
			                        if ("test3".equals(s)) {
				                        d.complete();
			                        }
			                        else {
				                        d.next(s);
			                        }
		                        }))
		            .expectFusion(Fuseable.SYNC)
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
	public void failHandleFusedSync() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .handle((s, d) -> {
			                        if ("test3".equals(s)) {
				                        d.error(new RuntimeException("test"));
			                        }
			                        else {
				                        d.next(s);
			                        }
		                        }))
		            .expectFusion(Fuseable.SYNC)
		            .expectNext("test", "test2")
		            .verifyErrorMessage("test");
	}

	@Test
	public void handleFusedConditionalTargetSync() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .handle((s, d) -> {
			                        if ("test3".equals(s)) {
				                        d.complete();
			                        }
			                        else {
				                        d.next(s);
			                        }
		                        })
		                        .filter(d -> true))
		            .expectFusion(Fuseable.SYNC)
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
	public void failHandleFusedConditionalTargetSync() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .handle((s, d) -> {
			                        if ("test3".equals(s)) {
				                        d.error(new RuntimeException("test"));
			                        }
			                        else {
				                        d.next(s);
			                        }
		                        })
		                        .filter(d -> true))
		            .expectFusion(Fuseable.SYNC)
		            .expectNext("test", "test2")
		            .verifyErrorMessage("test");
	}

	@Test
	public void handleFusedAsync() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.handle((s, d) -> {
			if ("test3".equals(s)) {
				d.complete();
			}
			else {
				d.next(s);
			}
		}))
		            .expectFusion(Fuseable.ASYNC)
		            .then(() -> {
			            up.onNext("test");
			            up.onNext("test2");
			            up.onNext("test3");
		            })
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
	public void failHandleFusedAsync() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.handle((s, d) -> {
			if ("test3".equals(s)) {
				d.error(new Exception("test"));
			}
			else {
				d.next(s);
			}
		}))
		            .expectFusion(Fuseable.ASYNC)
		            .then(() -> {
			            up.onNext("test");
			            up.onNext("test2");
			            up.onNext("test3");
		            })
		            .expectNext("test", "test2")
		            .verifyErrorMessage("test");
	}

	@Test
	public void failHandleFusedConditionalAsync() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.handle((s, d) -> {
			if ("test3".equals(s)) {
				d.error(new Exception("test"));
			}
			else {
				d.next(s);
			}
		})
		                      .filter(d -> true))
		            .expectFusion(Fuseable.ASYNC)
		            .then(() -> {
			            up.onNext("test");
			            up.onNext("test2");
			            up.onNext("test3");
		            })
		            .expectNext("test", "test2")
		            .verifyErrorMessage("test");
	}

	@Test
	public void handleFusedConditionalTargetAsync() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.handle((s, d) -> {
			if ("test3".equals(s)) {
				d.complete();
			}
			else {
				d.next(s);
			}
		})
		                      .filter(d -> true))
		            .expectFusion(Fuseable.ASYNC)
		            .then(() -> {
			            up.onNext("test");
			            up.onNext("test2");
			            up.onNext("test3");
		            })
		            .expectNext("test", "test2")
		            .verifyComplete();
	}

	@Test
	public void handleFusedConditionalFilteredTargetAsync() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.handle((s, d) -> {
			if ("test3".equals(s)) {
				d.complete();
			}
			else if ("test2".equals(s)) {
				d.next(s);
			}
		})
		                      .filter(d -> true))
		            .expectFusion(Fuseable.ASYNC)
		            .then(() -> {
			            up.onNext("test");
			            up.onNext("test2");
			            up.onNext("test3");
		            })
		            .expectNext("test2")
		            .verifyComplete();
	}

	@Test
	public void handleFusedFilteredTargetAsync() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.handle((s, d) -> {
			if ("test3".equals(s)) {
				d.complete();
			}
			else if ("test2".equals(s)) {
				d.next(s);
			}
		}))
		            .expectFusion(Fuseable.ASYNC)
		            .then(() -> {
			            up.onNext("test");
			            up.onNext("test2");
			            up.onNext("test3");
		            })
		            .expectNext("test2")
		            .verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void handleFusedStateTargetAsync() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		up.onNext("test");
		up.onNext("test2");
		up.onNext("test3");
		StepVerifier.create(up.handle((s, d) -> {
			d.complete();
		}))
		            .consumeSubscriptionWith(s -> {
			            Fuseable.QueueSubscription<String> qs =
					            ((Fuseable.QueueSubscription<String>) s);
			            qs.requestFusion(ASYNC);
			            assertThat(qs.size()).isEqualTo(3);
			            assertThat(qs.poll()).isNull();
			            assertThat(qs.poll()).isNull();
			            assertThat(qs.size()).isEqualTo(2);
			            qs.clear();
			            assertThat(qs.size()).isEqualTo(0);
		            })
		            .thenCancel()
		            .verify();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void handleFusedStateTargetConditionalAsync() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		up.onNext("test");
		up.onNext("test2");
		up.onNext("test3");
		StepVerifier.create(up.handle((s, d) -> {
			d.complete();
		})
		                      .filter(t -> true))
		            .consumeSubscriptionWith(s -> {
			            Fuseable.QueueSubscription<String> qs =
					            ((Fuseable.QueueSubscription<String>) ((Receiver) s).upstream());
			            qs.requestFusion(ASYNC);
			            assertThat(qs.size()).isEqualTo(3);
			            assertThat(qs.poll()).isNull();
			            assertThat(qs.poll()).isNull();
			            assertThat(qs.size()).isEqualTo(2);
			            qs.clear();
			            assertThat(qs.size()).isEqualTo(0);
		            })
		            .thenCancel()
		            .verify();
	}

	@Test
	public void noFusionOnConditionalThreadBarrier() {
		StepVerifier.create(Flux.just("test", "test2")
		                        .as(this::passThrough)
		                        .distinct())
		            .expectFusion(Fuseable.ANY | Fuseable.THREAD_BARRIER, Fuseable.NONE)
		            .thenCancel()
		            .verify();
	}

	@Test
	public void prematureCompleteFusedSync() {
		StepVerifier.create(Flux.just("test")
		                        .as(this::passThrough)
		                        .filter(t -> true))
		            .expectFusion(Fuseable.SYNC)
		            .expectNext("test")
		            .verifyComplete();
	}

	@Test
	public void dropHandleFusedSync() {
		StepVerifier.create(Flux.just("test", "test2")
		                        .handle((data, s) -> {})
		                        .filter(t -> true))
		            .expectFusion(Fuseable.SYNC)
		            .verifyComplete();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void failFusedStateTargetAsync() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		up.onNext("test");
		up.onNext("test2");
		up.onNext("test3");
		StepVerifier.create(up.handle((s, d) -> {
			d.error(new RuntimeException("test"));
		}))
		            .consumeSubscriptionWith(s -> {
			            Fuseable.QueueSubscription<String> qs =
					            ((Fuseable.QueueSubscription<String>) s);
			            qs.requestFusion(ASYNC);
			            assertThat(qs.size()).isEqualTo(3);
			            assertThat(qs.poll()).isNull();
			            try {
				            assertThat(qs.poll()).isNull();
				            Assert.fail();
			            }
			            catch (Exception e) {
				            assertThat(e).hasMessage("test");
			            }
			            assertThat(qs.size()).isEqualTo(2);
			            qs.clear();
			            assertThat(qs.size()).isEqualTo(0);
		            })
		            .thenCancel()
		            .verify();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void failFusedStateConditionalTargetAsync() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		up.onNext("test");
		up.onNext("test2");
		up.onNext("test3");
		StepVerifier.create(up.handle((s, d) -> {
			d.error(new RuntimeException("test"));
		}).filter(d -> true))
		            .consumeSubscriptionWith(s -> {
			            Fuseable.QueueSubscription<String> qs =
					            ((Fuseable.QueueSubscription<String>) ((Receiver) s).upstream());
			            qs.requestFusion(ASYNC);
			            assertThat(qs.size()).isEqualTo(3);
			            assertThat(qs.poll()).isNull();
			            assertThat(((Trackable)qs).getError()).hasMessage("test");
			            try {
				            assertThat(qs.poll()).isNull();
				            Assert.fail();
			            }
			            catch (Exception e) {
				            assertThat(e).hasMessage("test");
			            }
			            assertThat(qs.size()).isEqualTo(2);
			            qs.clear();
			            assertThat(qs.size()).isEqualTo(0);
		            })
		            .thenCancel()
		            .verify();
	}

	@Test
	public void handleFusedCancel() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .as(this::passThrough), 2)
		            .expectNext("test", "test2")
		            .thenCancel()
		            .verify();
	}

	@Test
	public void handleConditionalFused() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .handle((s, d) -> {
			                        if ("test3".equals(s)) {
				                        d.complete();
			                        }
			                        else {
				                        d.next(s);
			                        }
		                        })
		                        .filter("test2"::equals))
		            .expectNext("test2")
		            .verifyComplete();
	}

	@Test
	public void handleConditionalFusedTryNext() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .handle((s, d) -> {
			                        if ("test3".equals(s)) {
				                        d.complete();
			                        }
			                        else {
				                        d.next(s);
			                        }
		                        })
		                        .filter("test2"::equals), 3)
		            .expectNext("test2")
		            .verifyComplete();
	}

	@Test
	public void handleConditionalTargetCancel() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .hide()
		                        .handle((s, d) -> {
			                        if ("test3".equals(s)) {
				                        d.complete();
			                        }
			                        else {
				                        d.next(s);
			                        }
		                        })
		                        .filter("test2"::equals))
		            .thenCancel()
		            .verify();
	}

	@Test
	public void handleConditionalFusedCancelBoth() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .as(this::passThrough), 2)
		            .expectNext("test", "test2")
		            .thenCancel()
		            .verify();
	}

	@Test
	public void handleConditionalTarget() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .hide()
		                        .handle((s, d) -> {
			                        if ("test3".equals(s)) {
				                        d.complete();
			                        }
			                        else {
				                        d.next(s);
			                        }
		                        })
		                        .filter("test2"::equals))
		            .expectNext("test2")
		            .verifyComplete();
	}

	@Test
	public void handleConditionalFusedCancel() {
		StepVerifier.create(Flux.just("test", "test2", "test3")
		                        .as(this::passThrough), 2)
		            .expectNext("test", "test2")
		            .thenCancel()
		            .verify();
	}

	@Test
	public void failNextIfTerminatedHandleBothConditional() {
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo("test"));
		TestPublisher<String> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		StepVerifier.create(ts.flux()
		                      .as(this::passThrough)
		                      .filter("test2"::equals))
		            .then(() -> {
			            ts.complete();
			            ts.next("test");
		            })
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test
	public void ignoreCompleteIfTerminatedHandleFused() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.as(this::passThrough).filter(t -> true))
		            .then(() -> {
			            up.actual.onComplete();
			            up.actual.onComplete();
		            })
		            .verifyComplete();
	}

	@Test
	public void ignoreCompleteIfTerminatedHandleTargetConditional() {
		TestPublisher<String> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		StepVerifier.create(ts.flux()
		                      .hide()
		                      .as(this::passThrough)
		                      .filter("test2"::equals))
		            .then(() -> {
			            ts.complete();
			            ts.complete();
		            })
		            .verifyComplete();
	}

	@Test
	public void failNextIfTerminatedHandleTargetConditional() {
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo("test"));
		TestPublisher<String> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		StepVerifier.create(ts.flux()
		                      .hide()
		                      .as(this::passThrough)
		                      .filter("test2"::equals))
		            .then(() -> {
			            ts.complete();
			            ts.next("test");
		            })
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test
	public void failNextIfTerminatedHandleSourceConditional() {
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo("test"));
		TestPublisher<String> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		StepVerifier.create(ts.flux()
		                      .as(this::passThrough))
		            .then(() -> {
			            ts.complete();
			            ts.next("test");
		            })
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test
	public void failNextIfTerminatedHandle() {
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo("test"));
		TestPublisher<String> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		StepVerifier.create(ts.flux()
		                      .hide()
		                      .as(this::passThrough))
		            .then(() -> {
			            ts.complete();
			            ts.next("test");
		            })
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test
	public void ignoreCompleteIfTerminatedFusedHandle() {
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.as(this::passThrough))
		            .then(() -> {
			            up.actual.onComplete();
			            up.actual.onComplete();
		            })
		            .verifyComplete();
	}

	@Test
	public void failNextIfTerminatedFusedHandle() {
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo("test"));
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.as(this::passThrough))
		            .then(() -> {
			            up.actual.onComplete();
			            up.actual.onNext("test");
		            })
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test
	public void failNextIfTerminatedConditionalBothFusedHandle() {
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo("test"));
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.as(this::passThrough)
		                      .filter(d -> true))
		            .then(() -> {
			            up.actual.onComplete();
			            up.actual.onNext("test");
		            })
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void failTryNextIfTerminatedFusedHandle() {
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo("test"));
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.as(this::passThrough))
		            .then(() -> {
			            up.actual.onComplete();
			            ((Fuseable.ConditionalSubscriber<String>) up.actual).tryOnNext(
					            "test");
		            })
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void failTryNextIfTerminatedConditionalBothFusedHandle() {
		Hooks.onNextDropped(t -> assertThat(t).isEqualTo("test"));
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.as(this::passThrough)
		                      .filter(d -> true))
		            .then(() -> {
			            up.actual.onComplete();
			            ((Fuseable.ConditionalSubscriber<String>) up.actual).tryOnNext(
					            "test");
		            })
		            .verifyComplete();
		Hooks.resetOnNextDropped();
	}

	@Test
	public void ignoreCompleteIfTerminatedHandle() {
		TestPublisher<String> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		StepVerifier.create(ts.flux()
		                      .hide()
		                      .as(this::passThrough))
		            .then(() -> {
			            ts.complete();
			            ts.complete();
		            })
		            .verifyComplete();
	}

	@Test
	public void failErrorIfTerminatedHandle() {
		Hooks.onErrorDropped(t -> assertThat(t).hasMessage("test"));
		TestPublisher<String> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		StepVerifier.create(ts.flux()
		                      .hide()
		                      .as(this::passThrough))
		            .then(() -> {
			            ts.complete();
			            ts.error(new Exception("test"));
		            })
		            .verifyComplete();
		Hooks.resetOnErrorDropped();
	}

	@Test
	public void failErrorIfTerminatedConditionalTargetHandle() {
		Hooks.onErrorDropped(t -> assertThat(t).hasMessage("test"));
		TestPublisher<String> ts =
				TestPublisher.createNoncompliant(TestPublisher.Violation.CLEANUP_ON_TERMINATE);
		StepVerifier.create(ts.flux()
		                      .hide()
		                      .as(this::passThrough)
		                      .filter(d -> true))
		            .then(() -> {
			            ts.complete();
			            ts.error(new Exception("test"));
		            })
		            .verifyComplete();
		Hooks.resetOnErrorDropped();
	}

	@Test
	public void failErrorIfTerminatedFusedHandle() {
		Hooks.onErrorDropped(t -> assertThat(t).hasMessage("test"));
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.as(this::passThrough))
		            .then(() -> {
			            up.actual.onComplete();
			            up.actual.onError(new Exception("test"));
		            })
		            .verifyComplete();
		Hooks.resetOnErrorDropped();
	}

	@Test
	public void failErrorIfTerminatedFusedHandleTargetConditional() {
		Hooks.onErrorDropped(t -> assertThat(t).hasMessage("test"));
		UnicastProcessor<String> up = UnicastProcessor.create();
		StepVerifier.create(up.as(this::passThrough)
		                      .filter(d -> true))
		            .then(() -> {
			            up.actual.onComplete();
			            up.actual.onError(new Exception("test"));
		            })
		            .verifyComplete();
		Hooks.resetOnErrorDropped();
	}

	Flux<String> passThrough(Flux<String> f) {
		return f.handle((a, b) -> b.next(a));
	}

	@Test
	public void handleBackpressuredBothConditional() {
		TestPublisher<String> ts = TestPublisher.create();

		StepVerifier.create(ts.flux()
		                      .as(this::filterTest2), 0)
		            .thenRequest(2)
		            .then(() -> ts.next("test0", "test1"))
		            .expectNext("test0", "test1")
		            .thenRequest(1)
		            .then(() -> ts.next("test2"))
		            .expectNext("test2")
		            .verifyComplete();
	}

	@Test
	public void handleBackpressuredSourceConditional() {
		TestPublisher<String> ts = TestPublisher.create();
		StepVerifier.create(ts.flux()
		                      .as(this::filterTest2)
		                      .filter(d -> true), 0)
		            .thenRequest(2)
		            .then(() -> ts.next("test0", "test1"))
		            .expectNext("test0", "test1")
		            .thenRequest(1)
		            .then(() -> ts.next("test2"))
		            .expectNext("test2")
		            .verifyComplete();
	}

	@Test
	public void handleBackpressuredTargetConditional() {
		TestPublisher<String> ts = TestPublisher.create();
		StepVerifier.create(ts.flux()
		                      .hide()
		                      .as(this::filterTest2)
		                      .filter(d -> true), 0)
		            .thenRequest(2)
		            .then(() -> ts.next("test0", "test1"))
		            .expectNext("test0", "test1")
		            .thenRequest(1)
		            .then(() -> ts.next("test2"))
		            .expectNext("test2")
		            .verifyComplete();
	}

	Flux<String> filterTest2(Flux<String> f) {
		return f.handle((a, b) -> {
			b.next(a);
			if ("test2".equals(a)) {
				b.complete();
			}
		});
	}

	@Test
	public void failHandleError() {
		this.operatorVerifier(this::errorHandle)
		    .verifyErrorMessage("test");

		this.operatorVerifier(this::errorHandle2)
		    .verifyErrorMessage("test");

		this.operatorVerifier(this::errorHandleDoubleNext)
		    .verifyError(IllegalStateException.class);
	}

	@Test
	public void failHandleFusedError() {
		this.operatorVerifierFusedTryNext(this::errorHandle)
		    .verifyErrorMessage("test");

		this.operatorVerifierFused(this::errorHandle)
		    .verifyErrorMessage("test");

		this.operatorVerifierFusedTryNext(this::errorHandle2)
		    .verifyErrorMessage("test");

		this.operatorVerifierFused(this::errorHandle2)
		    .verifyErrorMessage("test");

		this.operatorVerifierFusedTryNext(this::errorHandleDoubleNext)
		    .verifyError(IllegalStateException.class);
	}

	@Test
	public void failHandleFusedBothConditionalError() {
		this.operatorVerifierFusedBothConditionalTryNext(this::errorHandle)
		    .verifyErrorMessage("test");

		this.operatorVerifierFusedBothConditional(this::errorHandle)
		    .verifyErrorMessage("test");

		this.operatorVerifierFusedBothConditionalTryNext(this::errorHandle2)
		    .verifyErrorMessage("test");

		this.operatorVerifierFusedBothConditional(this::errorHandle2)
		    .verifyErrorMessage("test");

		this.operatorVerifierFusedBothConditionalTryNext(this::errorHandleDoubleNext)
		    .verifyError(IllegalStateException.class);
	}

	@Test
	public void failHandleSourceConditionalError() {
		this.operatorVerifierSourceConditional(this::errorHandle)
		    .verifyErrorMessage("test");

		this.operatorVerifierSourceConditional(this::errorHandle2)
		    .verifyErrorMessage("test");

		this.operatorVerifierSourceConditional(this::errorHandleDoubleNext)
		    .verifyError(IllegalStateException.class);
	}

	@Test
	public void failHandleTargetConditionalError() {
		this.operatorVerifierTargetConditional(this::errorHandle)
		    .verifyErrorMessage("test");

		this.operatorVerifierTargetConditional(this::errorHandle2)
		    .verifyErrorMessage("test");

		this.operatorVerifierTargetConditional(this::errorHandleDoubleNext)
		    .verifyError(IllegalStateException.class);
	}

	@Test
	public void failHandleBothConditionalError() {
		this.operatorVerifierBothConditional(this::errorHandle)
		    .verifyErrorMessage("test");

		this.operatorVerifierBothConditional(this::errorHandle2)
		    .verifyErrorMessage("test");

		this.operatorVerifierBothConditional(this::errorHandleDoubleNext)
		    .verifyError(IllegalStateException.class);
	}

	Flux<String> errorHandle(Flux<String> f) {
		return f.handle((s, d) -> {
			throw new RuntimeException(s);
		});
	}

	Flux<String> errorHandle2(Flux<String> f) {
		return f.handle((s, d) -> d.error(new Exception(s)));
	}

	Flux<String> errorHandleDoubleNext(Flux<String> f) {
		return f.handle((s, d) -> {
			d.next("test");
			d.next("test2");
		});
	}

	@Test
	@SuppressWarnings("unchecked")
	public void assertPrePostState() {
		Flux<Integer> f = Flux.<Integer>from(s -> {
			Trackable t = (Trackable) s;
			assertThat(((Receiver) s).upstream()).isNull();
			assertThat(((Producer) s).downstream()).isNotNull();
			assertThat(((Receiver) s).upstream()).isNull();
			assertThat(((Loopback) s).connectedInput()).isNotNull();
			assertThat(t.getError()).isNull();
			assertThat(t.isStarted()).isFalse();
			assertThat(t.isTerminated()).isFalse();

			s.onSubscribe(Operators.emptySubscription());
			s.onSubscribe(Operators.emptySubscription()); //noop path
			assertThat(t.isStarted()).isTrue();
			s.onNext(1); //noop path
			((Fuseable.ConditionalSubscriber<Integer>)s).tryOnNext(1); //noop path
			s.onComplete();
			assertThat(t.isStarted()).isFalse();
			assertThat(t.isTerminated()).isTrue();
		}).handle((d, sink) -> {});

		f.subscribe();

		f.filter(d -> true)
		 .subscribe();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void assertPrePostStateFused() {
		AtomicReference<Trackable> ref = new AtomicReference<>();
		Flux<String> f = Flux.just("test", "test2")
		                     .doOnSubscribe(s -> {
		                     	Trackable t = (Trackable) ((Producer)((Producer)s).downstream()).downstream();
			                     ref.set(t);
			                     assertThat(t.isStarted()).isFalse();
		                     })
		                     .handle((String data, SynchronousSink<String> sink) -> {
		                     })
		                     .doOnSubscribe(parent -> {
			                     Trackable t = (Trackable) parent;
			                     ((Subscriber<String>)t).onSubscribe(Operators.emptySubscription());//noop
			                     // path
			                     assertThat(((Receiver) t).upstream()).isNotNull();
			                     assertThat(((Producer) t).downstream()).isNotNull();
			                     assertThat(((Loopback) t).connectedInput()).isNotNull();
			                     assertThat(t.getError()).isNull();
			                     assertThat(t.isStarted()).isTrue();
			                     assertThat(t.isTerminated()).isFalse();
		                     })
		                     .doOnComplete(() -> {
			                     assertThat(ref.get()
			                                   .isStarted()).isFalse();
			                     assertThat(ref.get()
			                                   .isTerminated()).isTrue();
		                     });

		f.subscribe();

		f.filter(t -> true)
		 .subscribe();
	}

	StepVerifier.Step<String> operatorVerifier(Function<Flux<String>, Flux<String>> scenario) {
		return StepVerifier.create(Flux.just("test", "test2", "test3")
		                               .hide()
		                               .as(scenario), 2);
	}

	StepVerifier.Step<String> operatorVerifierFusedTryNext(Function<Flux<String>, Flux<String>> scenario) {
		return StepVerifier.create(Flux.just("test", "test2", "test3")
		                               .as(scenario), 2);
	}

	StepVerifier.Step<String> operatorVerifierFused(Function<Flux<String>, Flux<String>> scenario) {
		return StepVerifier.create(Flux.just("test", "test2", "test3")
		                               .as(scenario));
	}

	StepVerifier.Step<String> operatorVerifierFusedBothConditionalTryNext(Function<Flux<String>, Flux<String>> scenario) {
		return StepVerifier.create(Flux.just("test", "test2", "test3")
		                               .as(scenario)
		                               .filter(d -> true), 2);
	}

	StepVerifier.Step<String> operatorVerifierFusedBothConditional(Function<Flux<String>, Flux<String>> scenario) {
		return StepVerifier.create(Flux.just("test", "test2", "test3")
		                               .as(scenario)
		                               .filter(d -> true));
	}

	StepVerifier.Step<String> operatorVerifierTargetConditional(Function<Flux<String>, Flux<String>> scenario) {
		return StepVerifier.create(Flux.just("test", "test2", "test3")
		                               .hide()
		                               .as(scenario)
		                               .filter(d -> true), 2);
	}

	StepVerifier.Step<String> operatorVerifierSourceConditional(Function<Flux<String>, Flux<String>> scenario) {
		TestPublisher<String> ts = TestPublisher.create();

		return StepVerifier.create(ts.flux()
		                             .as(scenario))
		                   .then(() -> ts.next("test"));
	}

	StepVerifier.Step<String> operatorVerifierBothConditional(Function<Flux<String>, Flux<String>> scenario) {
		TestPublisher<String> ts = TestPublisher.create();

		return StepVerifier.create(ts.flux()
		                             .as(scenario)
		                             .filter(d -> true))
		                   .then(() -> ts.next("test"));
	}

}
