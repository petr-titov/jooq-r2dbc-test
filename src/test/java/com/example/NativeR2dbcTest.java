package com.example;

import java.util.Map;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

import org.springframework.r2dbc.core.ColumnMapRowMapper;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcBadGrammarException;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

/**
 * Native R2DBC tests
 */
class NativeR2dbcTest extends AbstractR2dbcTest {

    private final ColumnMapRowMapper rowMapper = new ColumnMapRowMapper();

    /*
     * Mono#zip()
     */

    @Test
    void testMonoZip() {
        var select = executeQuery("SELECT * FROM test WHERE ID = 1")
            .next()
            .subscribeOn(SCHEDULER);
        var zip = Mono.zip(select, select, select);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(zip)
                .expectNextCount(1)
                .verifyComplete();
        }

        waitASecond(); // !!!

        assertEquals(0, getAcquiredConnectionCount());
    }

    @Test
    void testMonoZipWithError() {
        var select = executeQuery("SELECT * FROM test WHERE ID = 1")
            .next()
            .subscribeOn(SCHEDULER);
        var selectWithError = executeQuery("SELECT * FROM missing WHERE ID = 1")
            .next()
            .subscribeOn(SCHEDULER);
        // 2 errors will be thrown
        var zip = Mono.zip(select, selectWithError, select, selectWithError, select);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(zip)
                .verifyError(R2dbcBadGrammarException.class);
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

    @Test
    void testMonoZipDelayError() {
        var select = executeQuery("SELECT * FROM test WHERE ID = 1")
            .next()
            .subscribeOn(SCHEDULER);
        var selectWithError = executeQuery("SELECT * FROM missing WHERE ID = 1")
            .next()
            .subscribeOn(SCHEDULER);
        // 2 errors will be thrown
        var zip = Mono.zipDelayError(select, selectWithError, select, selectWithError, select);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(zip)
                .verifyErrorMessage(COMPOSITE_EXCEPTION_MESSAGE);
        }

        waitASecond(); // !!!

        assertEquals(0, getAcquiredConnectionCount());
    }

    /*
     * Flux#flatMap()
     */

    @Test
    void testFlatMap() {
        var select = executeQuery("SELECT * FROM test")
            .map(it -> (Integer) it.get("ID"))
            .flatMap(id -> executeQuery("SELECT * FROM test WHERE ID = ?", id)
                .next()
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
        var select = executeQuery("SELECT * FROM test")
            .map(it -> (Integer) it.get("ID"))
            .flatMap(id -> {
                var sql = "SELECT * FROM test WHERE ID = ?";
                // 4 errors will be thrown
                if (id == ROW_COUNT / 2 || id == ROW_COUNT / 3 || id == ROW_COUNT / 4 || id == ROW_COUNT / 5) {
                    sql = "SELECT * FROM missing WHERE ID = ?";
                }
                return executeQuery(sql, id)
                    .next()
                    .publishOn(SCHEDULER);
            }, CONCURRENCY);

            for (int i = 0; i < ATTEMPT_COUNT; i++) {
                StepVerifier.create(select)
                    .thenConsumeWhile(it -> true)
                    .verifyError(R2dbcBadGrammarException.class);
            }

        assertEquals(0, getAcquiredConnectionCount());
    }

    @Test
    void testFlatMapDelayError() {
        var select = executeQuery("SELECT * FROM test")
            .map(it -> (Integer) it.get("ID"))
            .flatMapDelayError(id -> {
                var sql = "SELECT * FROM test WHERE ID = ?";
                // 4 errors will be thrown
                if (id == ROW_COUNT / 2 || id == ROW_COUNT / 3 || id == ROW_COUNT / 4 || id == ROW_COUNT / 5) {
                    sql = "SELECT * FROM missing WHERE ID = ?";
                }
                return executeQuery(sql, id)
                    .next()
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
     * Utilities
     */

    private Flux<Map<String, Object>> executeQuery(String query) {
        return Flux.usingWhen(connectionFactory.create(),
                connection -> connection.createStatement(query).execute(),
                Connection::close)
            .flatMap(result -> result.map(rowMapper));
    }

    private Flux<Map<String, Object>> executeQuery(String query, int id) {
        return Flux.usingWhen(connectionFactory.create(),
                connection -> connection.createStatement(query).bind(0, id).execute(),
                Connection::close)
                .flatMap(result -> result.map(rowMapper));
    }
}
