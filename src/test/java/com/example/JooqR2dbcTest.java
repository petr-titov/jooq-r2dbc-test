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
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.PoolMetrics;
import io.r2dbc.spi.ConnectionFactories;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

/**
 * Test for https://github.com/jOOQ/jOOQ/issues/15462
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
    private static final Field<Integer> F_ID = DSL.field("ID", SQLDataType.INTEGER.identity(true));
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
            .column(F_DESC);
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
                .verifyError();
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

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
                // 1 error will be thrown
                if (id == ROW_COUNT / 10) {
                    return Flux.from(ctx.selectFrom(TABLE_MISSING).where(F_ID.eq(id)))
                        .publishOn(SCHEDULER);
                }
                return Flux.from(ctx.selectFrom(TABLE).where(F_ID.eq(id)))
                    .publishOn(SCHEDULER);
            }, CONCURRENCY);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(select)
                .thenConsumeWhile(it -> true)
                .verifyError();
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
                .verifyError();
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

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
        // 1 error will be thrown
        var selectWithError = Mono.from(ctx.selectFrom(TABLE_MISSING).where(F_ID.eq(1)))
            .subscribeOn(SCHEDULER);
        var zip = Mono.zip(select, select, select, selectWithError);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(zip)
                .verifyError();
        }

        // !!! connections are licked here !!!
        assertEquals(0, getAcquiredConnectionCount());
    }

    // zipDelayError() - is workaround for zip()
    @Test
    void testMonoZipDelayError() {
        var select = Mono.from(ctx.selectFrom(TABLE).where(F_ID.eq(1)))
            .subscribeOn(SCHEDULER);
        // 1 error will be thrown
        var selectWithError = Mono.from(ctx.selectFrom(TABLE_MISSING).where(F_ID.eq(1)))
            .subscribeOn(SCHEDULER);
        var zip = Mono.zipDelayError(select, select, select, selectWithError);

        for (int i = 0; i < ATTEMPT_COUNT; i++) {
            StepVerifier.create(zip)
                .verifyError();
        }

        assertEquals(0, getAcquiredConnectionCount());
    }

    private int getAcquiredConnectionCount() {
        return connectionFactory.getMetrics()
            .map(PoolMetrics::acquiredSize)
            .orElse(0);
    }
}
