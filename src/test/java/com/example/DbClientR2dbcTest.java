package com.example;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.function.Function;

import org.springframework.r2dbc.BadSqlGrammarException;
import org.springframework.r2dbc.core.DatabaseClient;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * R2DBC tests using {@link DatabaseClient}
 */
class DbClientR2dbcTest extends AbstractR2dbcTest {

    /*
     * Mono#zip()
     */

    @Test
    void testMonoZip() {
        var select = dbClient.sql("SELECT * FROM test WHERE ID = 1")
            .fetch().one()
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
        var select = dbClient.sql("SELECT * FROM test WHERE ID = 1")
            .fetch().one()
            .subscribeOn(SCHEDULER);
        var selectWithError = dbClient.sql("SELECT * FROM missing WHERE ID = 1")
            .fetch().one()
            .subscribeOn(SCHEDULER);
        // 2 errors will be thrown
        var zip = Mono.zip(select, selectWithError, select, selectWithError, select);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(zip)
                .verifyError(BadSqlGrammarException.class);
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

    @Test
    void testMonoZipDelayError() {
        var select = dbClient.sql("SELECT * FROM test WHERE ID = 1")
            .fetch().one()
            .subscribeOn(SCHEDULER);
        var selectWithError = dbClient.sql("SELECT * FROM missing WHERE ID = 1")
            .fetch().one()
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
        var select = dbClient.sql("SELECT ID FROM test")
            .fetch().all()
            .map(it -> (Integer) it.get("ID"))
            .flatMap(id -> dbClient.sql("SELECT * FROM test WHERE ID = ?")
                .bind(0, id)
                .fetch().one()
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
        var select = dbClient.sql("SELECT ID FROM test")
            .fetch().all()
            .map(it -> (Integer) it.get("ID"))
            .flatMap(id -> {
                var sql = "SELECT * FROM test WHERE ID = ?";
                // 4 errors will be thrown
                if (id == ROW_COUNT / 2 || id == ROW_COUNT / 3 || id == ROW_COUNT / 4 || id == ROW_COUNT / 5) {
                    sql = "SELECT * FROM missing WHERE ID = ?";
                }
                return dbClient.sql(sql)
                    .bind(0, id)
                    .fetch().one()
                    .publishOn(SCHEDULER);
            }, CONCURRENCY);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(select)
                .thenConsumeWhile(it -> true)
                .verifyError(BadSqlGrammarException.class);
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

    @Test
    void testFlatMapDelayError() {
        var select = dbClient.sql("SELECT ID FROM test")
            .fetch().all()
            .map(it -> (Integer) it.get("ID"))
            .flatMapDelayError(id -> {
                var sql = "SELECT * FROM test WHERE ID = ?";
                // 4 errors will be thrown
                if (id == ROW_COUNT / 2 || id == ROW_COUNT / 3 || id == ROW_COUNT / 4 || id == ROW_COUNT / 5) {
                    sql = "SELECT * FROM missing WHERE ID = ?";
                }
                return dbClient.sql(sql)
                    .bind(0, id)
                    .fetch().one()
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
        var select = dbClient.sql("SELECT * FROM test")
            .fetch().all()
            .subscribeOn(SCHEDULER);
        StepVerifier.create(select)
            .thenConsumeWhile(it -> getAcquiredConnectionCount() == 1)
            .verifyComplete();
    }

    @Test
    void testConnectionLifeTime_closed() {
        var select = dbClient.sql("SELECT * FROM test")
            .fetch().all()
            .collectList().flatMapIterable(Function.identity())
            .subscribeOn(SCHEDULER);
        StepVerifier.create(select)
            .thenConsumeWhile(it -> getAcquiredConnectionCount() == 0)
            .verifyComplete();
    }
}
