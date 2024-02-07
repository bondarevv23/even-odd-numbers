package com.github.bondarevv23;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ParityThreads implements AutoCloseable {
    private final int threadsCount;
    private final List<Thread> threads = new ArrayList<>();

    public ParityThreads(int threadsCount) {
        this.threadsCount = threadsCount;
    }

    /**
     * Запускает n потоков с общим счётчиком. Если номер потока и счётчик равны по модулю n, то
     * i-тый поток вызывает parityConsumer, куда первым аргументом передаёт свой номер,
     * вторым -- текущее значение счётчика. После чего инкрементит значение счётчика. Между двумя
     * вызовами parityConsumer проходит timeout милисекунд. Функция использует активное ожидание
     * и не эффективна при большом колличестве потоков / долгом вычислении parityConsumer / большом
     * значении timeout.
     */
    public void startActive(final BiConsumer<Integer, Long> parityConsumer, final long timeout) {
        checkIfNotClosed();
        final Counter counter = new Counter();
        IntStream.range(1, threadsCount + 1).mapToObj(threadNumber -> (Runnable) () -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    if (threadNumber % threadsCount == counter.get() % threadsCount) {
                        parityConsumer.accept(threadNumber, counter.get());
                        MILLISECONDS.sleep(timeout);
                        counter.increment();
                    }
                }
            } catch (InterruptedException consumed) {
                Thread.currentThread().interrupt();
            }
        }).map(Thread::new).forEach(threads::add);
        threads.forEach(Thread::start);
    }

    /**
     * Запускает n потоков с общим счётчиком. Если номер потока и счётчик равны по модулю n, то
     * i-тый поток вызывает parityConsumer, куда первым аргументом передаёт свой номер,
     * вторым -- текущее значение счётчика. После чего инкрементит значение счётчика. Между двумя
     * вызовами parityConsumer проходит timeout милисекунд.
     */
    public void start(final BiConsumer<Integer, Long> parityConsumer, final long timeout) {
        checkIfNotClosed();
        final Counter counter = new Counter();
        final Lock lock = new ReentrantLock();
        final List<Condition> conditions = Stream.generate(() -> null).limit(threadsCount)
                .map(_ -> lock.newCondition())
                .collect(Collectors.toList());
        IntStream.range(1, threadsCount + 1).mapToObj(threadNumber -> (Runnable) () -> {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    lock.lockInterruptibly();
                    try {
                        if (counter.get() % threadsCount != threadNumber % threadsCount) {
                            conditions.get((threadsCount + threadNumber - 2) % threadsCount).await();
                        }
                        parityConsumer.accept(threadNumber, counter.get());
                        MILLISECONDS.sleep(timeout);
                        counter.increment();
                        conditions.get(threadNumber - 1).signal();
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (InterruptedException consumed) {
                Thread.currentThread().interrupt();
            }
        }).map(Thread::new).forEach(threads::add);
        threads.forEach(Thread::start);
    }

    @Override
    public void close() {
        threads.forEach(Thread::interrupt);
        threads.clear();
    }

    private void checkIfNotClosed() {
        if (!threads.isEmpty()) {
            throw new ParityThreadsAreNotClosedException();
        }
    }

    private static class Counter {
        private volatile long i = 1;

        public long get() {
            return i;
        }

        public void increment() {
            i++;
        }
    }

    public static void main(String[] args) throws Exception {
        try (ParityThreads parityThreads = new ParityThreads(3)) {
            parityThreads.startActive(ParityThreads::counterConsumer, 1);
            MILLISECONDS.sleep(30);
        }
        System.out.println();
        try (ParityThreads parityThreads = new ParityThreads(4)) {
            parityThreads.start(ParityThreads::counterConsumer, 1000);
            SECONDS.sleep(12);
        }
    }

    private static void counterConsumer(int threadNum, long counter) {
        System.out.println("Thread " + threadNum + "  output: " + counter);
    }
}
