/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.state;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.core.state.StateFutureImpl.AsyncFrameworkExceptionHandler;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.function.ThrowingRunnable;

import edu.illinois.CTestClass;
import edu.illinois.CTestJUnit5Extension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StateFuture} related implementations. */
@ExtendWith(CTestJUnit5Extension.class)
@CTestClass
class StateFutureTest {
    static AsyncFrameworkExceptionHandler exceptionHandler =
            (message, exception) -> {
                throw new RuntimeException(message, exception);
            };

    @Test
    void basicSyncComplete() {
        StateFutureImpl.CallbackRunner runner = new TestCallbackRunner(null);
        final AtomicInteger counter = new AtomicInteger(0);

        StateFutureImpl<Integer> stateFuture1 = new StateFutureImpl<>(runner, exceptionHandler);
        stateFuture1.thenAccept(counter::addAndGet);
        assertThat(counter).hasValue(0);
        stateFuture1.complete(5);
        assertThat(counter).hasValue(5);

        StateFutureImpl<Integer> stateFuture2 = new StateFutureImpl<>(runner, exceptionHandler);
        StateFuture<String> stateFuture3 =
                stateFuture2.thenApply((v) -> String.valueOf(counter.addAndGet(v)));
        assertThat(counter).hasValue(5);
        stateFuture2.complete(3);
        assertThat(counter).hasValue(8);

        stateFuture3.thenAccept((v) -> counter.addAndGet(-Integer.parseInt(v)));
        assertThat(counter).hasValue(0);

        StateFutureImpl<Integer> stateFuture4 = new StateFutureImpl<>(runner, exceptionHandler);
        StateFutureImpl<Integer> stateFuture5 = new StateFutureImpl<>(runner, exceptionHandler);
        stateFuture4
                .thenCompose(
                        (v) -> {
                            counter.addAndGet(v);
                            return stateFuture5;
                        })
                .thenAccept(counter::addAndGet);
        assertThat(counter).hasValue(0);
        stateFuture4.complete(6);
        assertThat(counter).hasValue(6);
        stateFuture5.complete(3);
        assertThat(counter).hasValue(9);

        StateFutureImpl<Integer> stateFuture6 = new StateFutureImpl<>(runner, exceptionHandler);
        StateFutureImpl<Integer> stateFuture7 = new StateFutureImpl<>(runner, exceptionHandler);
        stateFuture6.thenCombine(
                stateFuture7,
                (v1, v2) -> {
                    counter.addAndGet(v1 - v2);
                    return StateFutureUtils.completedVoidFuture();
                });
        assertThat(counter).hasValue(9);
        stateFuture6.complete(4);
        assertThat(counter).hasValue(9);
        stateFuture7.complete(4 + 9);
        assertThat(counter).hasValue(0);

        StateFutureUtils.completedFuture(3).thenAccept(counter::addAndGet);
        assertThat(counter).hasValue(3);

        counter.set(0);
        ArrayList<StateFutureImpl<Integer>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures.add(new StateFutureImpl<>(runner, exceptionHandler));
        }
        StateFutureUtils.combineAll(futures)
                .thenAccept(
                        (c) -> {
                            int sum = 0;
                            for (Integer v : c) {
                                sum *= 10;
                                sum += v;
                            }
                            counter.addAndGet(sum);
                        });
        assertThat(counter).hasValue(0);
        for (int i = 0; i < 5; i++) {
            futures.get(i).complete(i + 1);
            if (i != 4) {
                assertThat(counter).hasValue(0);
            }
        }
        assertThat(counter).hasValue(12345);
    }

    @Test
    void testRunOnCorrectThread() throws Exception {
        final AtomicInteger threadIdProvider = new AtomicInteger(0);
        final ThreadLocal<Integer> threadId =
                ThreadLocal.withInitial(threadIdProvider::getAndIncrement);
        final AtomicReference<Throwable> exception = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        assertThat(threadId.get()).isZero();

        ExecutorService executor =
                Executors.newSingleThreadExecutor(
                        new ExecutorThreadFactory(this.getClass().getSimpleName()));

        executor.execute(
                () -> {
                    try {
                        assertThat(threadId.get()).isOne();
                    } catch (Throwable e) {
                        exception.set(e);
                    } finally {
                        latch.countDown();
                    }
                });

        latch.await(20, TimeUnit.SECONDS);

        assertThat(latch.getCount()).isZero();
        assertThat(exception).hasValue(null);

        MockValueState valueState = new MockValueState(executor);
        Runnable threadChecker =
                () -> {
                    try {
                        assertThat(threadId.get()).isOne();
                    } catch (Throwable e) {
                        exception.set(e);
                    }
                };

        final CountDownLatch latch2 = new CountDownLatch(2);
        final ArrayList<Integer> list = new ArrayList<>();
        executor.execute(
                () -> {
                    ArrayList<StateFuture<Integer>> futures = new ArrayList<>();
                    for (int i = 0; i < 5; i++) {
                        futures.add(valueState.get());
                    }
                    StateFutureUtils.combineAll(futures)
                            .thenCombine(
                                    valueState.get(),
                                    (c, v) -> {
                                        list.addAll(c);
                                        list.add(v);
                                        threadChecker.run();
                                        return 0;
                                    })
                            .thenCompose(
                                    (v) -> {
                                        threadChecker.run();
                                        return valueState.get();
                                    })
                            .thenApply(
                                    (v) -> {
                                        list.add(v);
                                        threadChecker.run();
                                        return 0;
                                    })
                            .thenAccept(
                                    (v) -> {
                                        threadChecker.run();
                                        latch2.countDown();
                                    });
                    latch2.countDown();
                });

        latch2.await(20, TimeUnit.SECONDS);

        assertThat(latch2.getCount()).isZero();
        assertThat(exception.get()).isNull();
        assertThat(list).hasSize(7);
    }

    @Test
    void testConditionally() {
        StateFutureImpl.CallbackRunner runner = new TestCallbackRunner(null);
        final AtomicInteger counter = new AtomicInteger(0);

        // accept
        StateFutureImpl<Integer> stateFuture1 = new StateFutureImpl<>(runner, exceptionHandler);
        stateFuture1
                .thenConditionallyAccept(e -> e > 0, counter::addAndGet, v -> counter.addAndGet(-v))
                .thenConditionallyAccept(
                        e -> !e, v -> counter.incrementAndGet(), v -> counter.decrementAndGet());
        assertThat(counter).hasValue(0);
        stateFuture1.complete(-5);
        assertThat(counter).hasValue(6);

        // apply
        StateFutureImpl<Integer> stateFuture2 = new StateFutureImpl<>(runner, exceptionHandler);
        stateFuture2
                .thenConditionallyApply(
                        v -> v > 0,
                        v -> String.valueOf(counter.addAndGet(v)),
                        v -> String.valueOf(counter.addAndGet(-v)))
                .thenConditionallyApply(
                        e -> !e.f0,
                        e -> counter.addAndGet(Integer.parseInt((String) e.f1) * 2),
                        e -> counter.addAndGet(Integer.parseInt((String) e.f1) * 3));
        assertThat(counter).hasValue(6);
        stateFuture2.complete(-3);
        assertThat(counter).hasValue(27);

        // compose
        StateFutureImpl<Integer> stateFuture3 = new StateFutureImpl<>(runner, exceptionHandler);
        StateFutureImpl<Integer> stateFuture4 = new StateFutureImpl<>(runner, exceptionHandler);
        StateFutureImpl<Integer> stateFuture5 = new StateFutureImpl<>(runner, exceptionHandler);
        stateFuture3
                .thenConditionallyCompose(
                        v -> v > 0,
                        (v) -> {
                            counter.addAndGet(v);
                            return stateFuture4;
                        },
                        (v) -> {
                            counter.addAndGet(-v);
                            return stateFuture5;
                        })
                .thenConditionallyCompose(
                        t -> !t.f0,
                        (t) -> {
                            counter.addAndGet((Integer) t.f1 * 2);
                            return StateFutureUtils.completedVoidFuture();
                        },
                        (t) -> {
                            counter.addAndGet((Integer) t.f1 * 3);
                            return StateFutureUtils.completedVoidFuture();
                        });

        assertThat(counter).hasValue(27);
        counter.set(0);

        stateFuture3.complete(3);
        assertThat(counter).hasValue(3);

        stateFuture5.complete(5);
        assertThat(counter).hasValue(3);

        stateFuture4.complete(4);
        assertThat(counter).hasValue(15);
    }

    /** Mock for value state. */
    private static class MockValueState {
        AtomicInteger value = new AtomicInteger(0);
        ExecutorService stateExecutor = Executors.newFixedThreadPool(3);
        StateFutureImpl.CallbackRunner runner;

        MockValueState(ExecutorService executor) {
            this.runner = new TestCallbackRunner(executor);
        }

        StateFuture<Integer> get() {
            StateFutureImpl<Integer> ret = new StateFutureImpl<>(runner, exceptionHandler);
            stateExecutor.submit(
                    () -> {
                        int a = ThreadLocalRandom.current().nextInt();
                        if (a > 0) {
                            try {
                                Thread.sleep(a % 1000);
                            } catch (Throwable e) {
                                // ignore
                            }
                        }
                        ret.complete(value.getAndIncrement());
                    });
            return ret;
        }
    }

    private static class TestCallbackRunner implements StateFutureImpl.CallbackRunner {
        private final ExecutorService stateExecutor;

        TestCallbackRunner(ExecutorService stateExecutor) {
            this.stateExecutor = stateExecutor;
        }

        @Override
        public void submit(ThrowingRunnable task) {
            if (stateExecutor == null) {
                ThrowingRunnable.unchecked(task).run();
            } else {
                stateExecutor.submit(() -> ThrowingRunnable.unchecked(task).run());
            }
        }
    }
}
