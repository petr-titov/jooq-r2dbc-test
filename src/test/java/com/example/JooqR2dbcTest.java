package com.example;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import static java.util.function.Predicate.not;

import org.jooq.exception.DataAccessException;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * R2DBC tests using jOOQ
 * <p/>
 * The following tests fail:
 * <li>{@link JooqR2dbcTest#testFlatMapWithError()}
 * <li>{@link JooqR2dbcTest#testMonoZipWithError()}
 */
class JooqR2dbcTest extends AbstractR2dbcTest {

    @Test
    void testSelect() {
        var select = jooq.selectFrom(TABLE);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(select)
                .expectNextCount(ROW_COUNT)
                .verifyComplete();
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

    @Test
    void testSelectWithError() {
        var select = jooq.selectFrom(TABLE_MISSING);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(select)
                .verifyError(DataAccessException.class);
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

    /*
     * Mono#zip()
     */

    @Test
    void testMonoZip() {
        var select = Mono.from(jooq.selectFrom(TABLE).where(F_ID.eq(1)))
            .subscribeOn(SCHEDULER);
        var zip = Mono.zip(select, select, select);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(zip)
                .expectNextCount(1)
                .verifyComplete();
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

    @Test
    void testMonoZipWithError() {
        var select = Mono.from(jooq.selectFrom(TABLE).where(F_ID.eq(1)))
            .subscribeOn(SCHEDULER);
        var selectWithError = Mono.from(jooq.selectFrom(TABLE_MISSING).where(F_ID.eq(1)))
            .subscribeOn(SCHEDULER);
        // 2 errors will be thrown
        var zip = Mono.zip(select, selectWithError, select, selectWithError, select);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(zip)
                .verifyError(DataAccessException.class);
        }

        // !!! connections are licked here !!!
        assertEquals(0, getAcquiredConnectionCount());
    }

    @Test
    void testMonoZipDelayError() {
        var select = Mono.from(jooq.selectFrom(TABLE).where(F_ID.eq(1)))
            .subscribeOn(SCHEDULER);
        var selectWithError = Mono.from(jooq.selectFrom(TABLE_MISSING).where(F_ID.eq(1)))
            .subscribeOn(SCHEDULER);
        // 2 errors will be thrown
        var zip = Mono.zipDelayError(select, selectWithError, select, selectWithError, select);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(zip)
                .verifyErrorMessage(COMPOSITE_EXCEPTION_MESSAGE);
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

    /*
     * Flux#flatMap()
     */

    @Test
    void testFlatMap() {
        var select = Flux.from(jooq.selectFrom(TABLE))
            .map(rec -> rec.get(F_ID))
            .flatMap(id -> Flux.from(jooq.selectFrom(TABLE).where(F_ID.eq(id)))
                .publishOn(SCHEDULER), CONCURRENCY);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(select)
                .expectNextCount(ROW_COUNT)
                .verifyComplete();
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

    @Test
    void testFlatMapWithError() {
        var select = Flux.from(jooq.selectFrom(TABLE))
            .map(rec -> rec.get(F_ID))
            .flatMap(id -> {
                var table = TABLE;
                // 4 errors will be thrown
                if (id == ROW_COUNT / 2 || id == ROW_COUNT / 3 || id == ROW_COUNT / 4 || id == ROW_COUNT / 5) {
                    table = TABLE_MISSING;
                }
                return Flux.from(jooq.selectFrom(table).where(F_ID.eq(id)))
                    .publishOn(SCHEDULER);
            }, CONCURRENCY);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(select)
                .thenConsumeWhile(it -> true)
                .verifyError(DataAccessException.class);
        }

        // !!! connections are licked here !!!
        assertEquals(0, getAcquiredConnectionCount());
    }

    @Test
    void testFlatMapDelayError() {
        var select = Flux.from(jooq.selectFrom(TABLE))
            .map(rec -> rec.get(F_ID))
            .flatMapDelayError(id -> {
                var table = TABLE;
                // 4 errors will be thrown
                if (id == ROW_COUNT / 2 || id == ROW_COUNT / 3 || id == ROW_COUNT / 4 || id == ROW_COUNT / 5) {
                    table = TABLE_MISSING;
                }
                return Flux.from(jooq.selectFrom(table).where(F_ID.eq(id)))
                    .publishOn(SCHEDULER);
            }, CONCURRENCY, CONCURRENCY);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(select)
                .expectNextCount(ROW_COUNT - 4)
                .verifyErrorMessage(COMPOSITE_EXCEPTION_MESSAGE);
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

    /*
     * Connection life time
     */

    @Test
    void testConnectionLifeTime_open() {
        var select = Flux.from(jooq.selectFrom(TABLE))
            .subscribeOn(SCHEDULER);
        StepVerifier.create(select)
            .thenConsumeWhile(it -> getAcquiredConnectionCount() == 1)
            .verifyComplete();
    }

    @Test
    void testConnectionLifeTime_closed() {
        var select = Flux.from(jooq.selectFrom(TABLE))
            .collectList().flatMapIterable(Function.identity())
            .subscribeOn(SCHEDULER);
        StepVerifier.create(select)
            .thenConsumeWhile(it -> getAcquiredConnectionCount() == 0)
            .verifyComplete();
    }

    /*
     * Pagination
     */

    @Test
    void testPagination() {
        var cursor = new AtomicInteger();

        var select = Flux.defer(() ->
                Flux.from(jooq.selectFrom(TABLE)
                    .where(F_ID.gt(cursor.get()))
                    .orderBy(F_ID)
                    .limit(PAGE_SIZE)))
            .doOnRequest(n -> assertEquals(Long.MAX_VALUE, n))
            .map(rec -> rec.get(F_ID))
            .collectList()
            .doOnRequest(n -> assertEquals(1, n))
            .repeat()
            .takeWhile(not(Collection::isEmpty))
            .doOnNext(list -> cursor.set(list.get(list.size() - 1)))
            .subscribeOn(SCHEDULER)

//            // lenient variant - next query is allowed before all items have handled (prefectch >= 0)
//            .concatMapIterable(Function.identity()/* , 1 */)
//            .delayElements(Duration.ofMillis(10))
//            .filter(rec -> getAcquiredConnectionCount() == 0);

            // strict variant - all items must to be handled before next query (prefectch == 0,
            // or prefectch > 0 for less strict variant)
            .concatMap(list -> Flux.fromIterable(list)
                .delayElements(Duration.ofMillis(10))
                .filter(rec -> getAcquiredConnectionCount() == 0)/* , 1 */);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            cursor.set(0);
            StepVerifier.create(select)
                .expectNextCount(ROW_COUNT)
                .verifyComplete();
        }
    }
}
