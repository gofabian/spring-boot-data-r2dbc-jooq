package gofabian.r2dbc.jooq;

import org.jooq.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.jooq.SQLDialect.H2;
import static org.jooq.SQLDialect.POSTGRES;


/**
 * ReactiveJooq is a reactive wrapper for JOOQ. It is a replacement for JOOQ methods that depend on a blocking JDBC
 * connection. Instead SQL statements are executed via R2DBC.
 * <p>
 * All methods are implemented like that:
 * - Get raw SQL statement and bind values from the JOOQ query/record.
 * - Execute via R2DBC database client.
 * - Convert result to JOOQ record.
 */
public class ReactiveJooq {

    @Support
    public static Mono<Integer> store(UpdatableRecord<?> record) {
        return ReactiveRecordExecutor.from(record).store(record);
    }

    @Support
    public static Mono<Integer> insert(TableRecord<?> record) {
        return ReactiveRecordExecutor.from(record).insert(record);

    }

    @Support
    public static Mono<Integer> update(UpdatableRecord<?> record) {
        return ReactiveRecordExecutor.from(record).update(record);
    }

    @Support
    public static <R extends UpdatableRecord<R>> Mono<Integer> delete(R record) {
        return ReactiveRecordExecutor.from(record).delete(record);
    }

    @Support
    public static <R extends UpdatableRecord<R>> Mono<Void> refresh(R record) {
        return ReactiveRecordExecutor.from(record).refresh(record);
    }

    @Support
    public static Mono<Integer> execute(Query query) {
        return ReactiveQueryExecutor.from(query).execute(query);
    }

    @Support
    public static <R extends Record> Flux<R> executeReturning(InsertResultStep<R> insertQuery) {
        return ReactiveQueryExecutor.from(insertQuery).executeReturning(insertQuery);
    }

    @Support
    public static <R extends Record> Mono<R> executeReturningOne(InsertResultStep<R> insertQuery) {
        return ReactiveQueryExecutor.from(insertQuery).executeReturningOne(insertQuery);
    }

    @Support({H2, POSTGRES})
    public static <R extends Record> Flux<R> executeReturning(UpdateResultStep<R> updateQuery) {
        return ReactiveQueryExecutor.from(updateQuery).executeReturning(updateQuery);
    }

    @Support({H2, POSTGRES})
    public static <R extends Record> Mono<R> executeReturningOne(UpdateResultStep<R> updateQuery) {
        return ReactiveQueryExecutor.from(updateQuery).executeReturningOne(updateQuery);
    }

    @Support
    public static <R extends Record> Flux<R> fetch(Select<R> jooqQuery) {
        return ReactiveQueryExecutor.from(jooqQuery).fetch(jooqQuery);
    }

    @Support
    public static <R extends Record> Mono<R> fetchOne(Select<R> jooqQuery) {
        return ReactiveQueryExecutor.from(jooqQuery).fetchOne(jooqQuery);
    }

    @Support
    public static <R extends Record> Mono<R> fetchAny(Select<R> jooqQuery) {
        return ReactiveQueryExecutor.from(jooqQuery).fetchAny(jooqQuery);
    }

    @Support
    public static Mono<Boolean> fetchExists(Select<?> jooqQuery) {
        return ReactiveQueryExecutor.from(jooqQuery).fetchExists(jooqQuery);
    }

    @Support
    public static Mono<Integer> fetchCount(Select<?> jooqQuery) {
        return ReactiveQueryExecutor.from(jooqQuery).fetchCount(jooqQuery);
    }

}
