package com.example;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jooq.DSLContext;
import org.jooq.Field;
import org.jooq.Table;
import org.jooq.impl.DSL;
import org.jooq.impl.SQLDataType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.r2dbc.core.DatabaseClient;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.PoolMetrics;
import io.r2dbc.spi.ConnectionFactories;

import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

/**
 * Infrastructure for R2DBC tests
 */
abstract class AbstractR2dbcTest {

    protected static final int CONCURRENCY = 10;

    protected static final int ROW_COUNT = 100;

    protected static final int PAGE_SIZE = 10;

    protected static final int ATTEMPT_COUNT = 3;

    protected static final Scheduler SCHEDULER = Schedulers.boundedElastic();

    protected static final Table<org.jooq.Record> TABLE = DSL.table("test");
    protected static final Table<org.jooq.Record> TABLE_MISSING = DSL.table("missing");
    protected static final Field<Integer> F_ID = DSL.field("ID", SQLDataType.INTEGER.identity(true).notNull());
    protected static final Field<String> F_NAME = DSL.field("NAME", SQLDataType.VARCHAR.length(50).notNull());
    protected static final Field<String> F_DESC = DSL.field("DESC", SQLDataType.VARCHAR.length(200));

    protected static final String COMPOSITE_EXCEPTION_MESSAGE = "Multiple exceptions";

    private static final int POOL_SIZE = 10;

    protected ConnectionPool connectionFactory;

    protected DatabaseClient dbClient;

    protected DSLContext jooq;

    protected AbstractR2dbcTest() {}

    @BeforeEach
    void setUp() throws Exception {
        // Pooled connection factory
        var url = "r2dbc:pool:h2:mem:///testdb-%s?maxSize=%s&maxAcquireTime=PT1s"
            .formatted(System.currentTimeMillis(), POOL_SIZE);
        connectionFactory = (ConnectionPool) ConnectionFactories.get(url);

        // Spring DatabaseClient
        dbClient = DatabaseClient.create(connectionFactory);

        // jOOQ DSL
        jooq = DSL.using(connectionFactory);

        // DB initialization
        initDb();
    }

    @AfterEach
    void tearDown() throws Exception {
        StepVerifier.create(connectionFactory.close())
            .verifyComplete();
        connectionFactory = null;
        dbClient = null;
        jooq = null;
    }

    protected int getAcquiredConnectionCount() {
        return connectionFactory.getMetrics()
            .map(PoolMetrics::acquiredSize)
            .orElse(0);
    }

    protected void waitASecond() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {}
    }

    private void initDb() {
        var createTable = jooq.createTable(TABLE)
            .column(F_ID)
            .column(F_NAME)
            .column(F_DESC)
            .primaryKey(F_ID);

        var rows = IntStream.rangeClosed(1, ROW_COUNT)
            .boxed()
            .map(i -> DSL.row("test" + i, "Test " + i))
            .collect(Collectors.toList());

        var insertData = jooq.insertInto(TABLE, F_NAME, F_DESC)
            .valuesOfRows(rows)
            .returningResult(F_ID);

        var initDb = Flux.concat(createTable, insertData);

        StepVerifier.create(initDb)
            .expectNext(0)
            .expectNextCount(ROW_COUNT)
            .verifyComplete();        
    }
}
