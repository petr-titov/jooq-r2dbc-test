package com.example;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.conf.ParamType;
import org.jooq.exception.DataAccessException;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.PoolMetrics;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.R2dbcBadGrammarException;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple3;
import reactor.util.function.Tuples;

/**
 * Tests for https://github.com/jOOQ/jOOQ/issues/15462
 * or https://github.com/r2dbc/r2dbc-pool/issues/198.
 * <p/>
 * The following tests fail:
 * {@link JooqR2dbcTest#testFlatMapWithError()},
 * {@link JooqR2dbcTest#testMonoZipWithError()}.
 */
class JooqR2dbcTest {

    private static final int POOL_SIZE = 10;

    private static final int CONCURRENCY = 10;

    private static final int ROW_COUNT = 100;

    private static final int ATTEMPT_COUNT = 3;

    private static final Scheduler SCHEDULER = Schedulers.boundedElastic();

    private static final Table<org.jooq.Record> TABLE = DSL.table("test");
    private static final Table<org.jooq.Record> TABLE_MISSING = DSL.table("bla_bla_bla");
    private static final Field<Integer> F_ID = DSL.field("ID", SQLDataType.INTEGER.identity(true).notNull());
    private static final Field<String> F_NAME = DSL.field("NAME", SQLDataType.VARCHAR.length(50).notNull());
    private static final Field<String> F_DESC = DSL.field("DESC", SQLDataType.VARCHAR.length(200));

    private ConnectionPool connectionFactory;

    private DSLContext ctx;

    @BeforeAll
    static void setUpBeforeClass() throws Exception {
        System.setProperty("org.jooq.no-logo", "true");
    }

    @AfterAll
    static void tearDownAfterClass() throws Exception {
    }

    @BeforeEach
    void setUp() throws Exception {
        // Pooled connection factory
        var url = "r2dbc:pool:h2:mem:///testdb-%s?maxSize=%s&maxAcquireTime=PT1s"
            .formatted(System.currentTimeMillis(), POOL_SIZE);
        connectionFactory = (ConnectionPool) ConnectionFactories.get(url);

        // jOOQ DSL
        ctx = DSL.using(connectionFactory);

        // DB initialization
        var createTable = ctx.createTable(TABLE)
            .column(F_ID)
            .column(F_NAME)
            .column(F_DESC)
            .primaryKey(F_ID);
        var rows = IntStream.rangeClosed(1, ROW_COUNT)
            .boxed()
            .map(i -> DSL.row("test" + i, "Test " + i))
            .collect(Collectors.toList());
        var insertData = ctx.insertInto(TABLE, F_NAME, F_DESC)
            .valuesOfRows(rows)
            .returningResult(F_ID);
        var initDb = Flux.concat(createTable, insertData);
        StepVerifier.create(initDb)
            .expectNext(0)
            .expectNextCount(ROW_COUNT)
            .verifyComplete();
    }

    @AfterEach
    void tearDown() throws Exception {
        StepVerifier.create(connectionFactory.close())
            .verifyComplete();
        connectionFactory = null;
        ctx = null;
    }

    /*
     * Just one query
     */

    @Test
    void testSelect() {
        var select = ctx.selectFrom(TABLE);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(select)
                .expectNextCount(ROW_COUNT)
                .verifyComplete();
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

    @Test
    void testSelectWithError() {
        var select = ctx.selectFrom(TABLE_MISSING);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(select)
                .verifyError(DataAccessException.class);
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

    /*
     * Flux#flatMap()
     */

    @Test
    void testFlatMap() {
        var select = Flux.from(ctx.selectFrom(TABLE))
            .map(rec -> rec.get(F_ID))
            .flatMap(id -> Flux.from(ctx.selectFrom(TABLE).where(F_ID.eq(id)))
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
        var select = Flux.from(ctx.selectFrom(TABLE))
            .map(rec -> rec.get(F_ID))
            .flatMap(id -> {
                // 4 errors will be thrown
                if (id == ROW_COUNT / 2 || id == ROW_COUNT / 3 || id == ROW_COUNT / 4 || id == ROW_COUNT / 5) {
                    return Flux.from(ctx.selectFrom(TABLE_MISSING).where(F_ID.eq(id)))
                        .publishOn(SCHEDULER);
                }
                return Flux.from(ctx.selectFrom(TABLE).where(F_ID.eq(id)))
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

    // flatMapDelayError() - is workaround for flatMap()
    @Test
    void testFlatMapDelayError() {
        var select = Flux.from(ctx.selectFrom(TABLE))
            .map(rec -> rec.get(F_ID))
            .flatMapDelayError(id -> {
                // 4 errors will be thrown
                if (id == ROW_COUNT / 2 || id == ROW_COUNT / 3 || id == ROW_COUNT / 4 || id == ROW_COUNT / 5) {
                    return Flux.from(ctx.selectFrom(TABLE_MISSING).where(F_ID.eq(id)))
                        .publishOn(SCHEDULER);
                }
                return Flux.from(ctx.selectFrom(TABLE).where(F_ID.eq(id)))
                    .publishOn(SCHEDULER);
            }, CONCURRENCY, CONCURRENCY);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(select)
                .expectNextCount(ROW_COUNT - 4)
                .verifyErrorMessage("Multiple exceptions");
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

    /*
     * Mono#zip()
     */

    @Test
    void testMonoZip() {
        var select = Mono.from(ctx.selectFrom(TABLE).where(F_ID.eq(1)))
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
        var select = Mono.from(ctx.selectFrom(TABLE).where(F_ID.eq(1)))
            .subscribeOn(SCHEDULER);
        var selectWithError = Mono.from(ctx.selectFrom(TABLE_MISSING).where(F_ID.eq(1)))
            .subscribeOn(SCHEDULER);
        // 2 errors will be thrown
        var zip = Mono.zip(select, select, select, selectWithError, selectWithError);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(zip)
                .verifyError(DataAccessException.class);
        }

        // !!! connections are licked here !!!
        assertEquals(0, getAcquiredConnectionCount());
    }

    // zipDelayError() - is workaround for zip()
    @Test
    void testMonoZipDelayError() {
        var select = Mono.from(ctx.selectFrom(TABLE).where(F_ID.eq(1)))
            .subscribeOn(SCHEDULER);
        var selectWithError = Mono.from(ctx.selectFrom(TABLE_MISSING).where(F_ID.eq(1)))
            .subscribeOn(SCHEDULER);
        // 2 errors will be thrown
        var zip = Mono.zipDelayError(select, select, select, selectWithError, selectWithError);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(zip)
                .verifyErrorMessage("Multiple exceptions");
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

    /*
     * Mono#zip() without jOOQ
     */

    @Test
    void testMonoZip_noJooq() {
        var selectSql = DSL.selectFrom(TABLE).where(F_ID.eq(1))
            .getSQL(ParamType.NAMED_OR_INLINED);

        var select = executeQuery(selectSql).next()
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
    void testMonoZipWithError_noJooq() {
        var selectSql = DSL.selectFrom(TABLE).where(F_ID.eq(1))
            .getSQL(ParamType.NAMED_OR_INLINED);
        var selectWithErrorSql = DSL.selectFrom(TABLE_MISSING).where(F_ID.eq(1))
            .getSQL(ParamType.NAMED_OR_INLINED);

        var select = executeQuery(selectSql).next()
            .subscribeOn(SCHEDULER);
        var selectWithError = executeQuery(selectWithErrorSql).next()
            .subscribeOn(SCHEDULER);
        // 2 errors will be thrown
        var zip = Mono.zip(select, select, select, selectWithError, selectWithError);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(zip)
                .verifyError(R2dbcBadGrammarException.class);
        }

        // !!! connections are NOT licked here !!!
        assertEquals(0, getAcquiredConnectionCount());
    }

    @Test
    void testMonoZipDelayError_noJooq() {
        var selectSql = DSL.selectFrom(TABLE).where(F_ID.eq(1))
            .getSQL(ParamType.NAMED_OR_INLINED);
        var selectWithErrorSql = DSL.selectFrom(TABLE_MISSING).where(F_ID.eq(1))
            .getSQL(ParamType.NAMED_OR_INLINED);

        var select = executeQuery(selectSql).next()
            .subscribeOn(SCHEDULER);
        var selectWithError = executeQuery(selectWithErrorSql).next()
            .subscribeOn(SCHEDULER);
        // 2 errors will be thrown
        var zip = Mono.zipDelayError(select, select, select, selectWithError, selectWithError);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(zip)
                .verifyErrorMessage("Multiple exceptions");
        }

        //waitASecond(); // !!!

        assertEquals(0, getAcquiredConnectionCount());
    }

    /*
     * Routines
     */

    private Flux<Tuple3<Integer, String, String>> executeQuery(String query) {
        return Flux.usingWhen(connectionFactory.create(),
                connection -> connection.createStatement(query).execute(),
                Connection::close)
            .flatMap(result -> result.map(this::mapRow));
    }

    private Tuple3<Integer, String, String> mapRow(Row row, RowMetadata meta) {
        return Tuples.of(
            row.get(F_ID.getName(), Integer.class),
            row.get(F_NAME.getName(), String.class),
            row.get(F_DESC.getName(), String.class));
    }

    private int getAcquiredConnectionCount() {
        return connectionFactory.getMetrics()
            .map(PoolMetrics::acquiredSize)
            .orElse(0);
    }

    private void waitASecond() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {}
    }
}
