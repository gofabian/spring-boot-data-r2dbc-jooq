package gofabian.r2dbc.jooq;

import io.r2dbc.spi.Row;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.springframework.data.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

public class ReactiveJooq {

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static Mono<Integer> executeInsert(TableRecord<?> record) {
        DSLContext dslContext = record.configuration().dsl();
        InsertQuery insert = dslContext.insertQuery(record.getTable());
        insert.setRecord(record);
        return execute(insert);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static Mono<Integer> executeUpdate(UpdatableRecord<?> record) {
        DSLContext dslContext = record.configuration().dsl();
        UpdateQuery update = dslContext.updateQuery(record.getTable());
        Tools.addConditions(update, record, record.getTable().getPrimaryKey().getFieldsArray());
        update.setRecord(record);
        return execute(update);
    }

    @SuppressWarnings("rawtypes")
    public static Mono<Integer> executeDelete(UpdatableRecord<?> record) {
        DSLContext dslContext = record.configuration().dsl();
        DeleteQuery delete = dslContext.deleteQuery(record.getTable());
        Tools.addConditions(delete, record, record.getTable().getPrimaryKey().getFieldsArray());
        return execute(delete);
    }


    public static Mono<Integer> execute(Query jooqQuery) {
        return executeForR2dbcHandle(jooqQuery)
                .fetch()
                .rowsUpdated();
    }

    public static <R extends Record> Flux<R> fetch(Select<R> jooqQuery) {
        return executeForR2dbcHandle(jooqQuery)
                .map(row -> convertRowToRecord(row, jooqQuery))
                .all();
    }

    public static <R extends Record> Mono<R> fetchOne(Select<R> jooqQuery) {
        return executeForR2dbcHandle(jooqQuery)
                .map(row -> convertRowToRecord(row, jooqQuery))
                .one();
    }

    public static <R extends Record> Mono<R> fetchAny(Select<R> jooqQuery) {
        return executeForR2dbcHandle(jooqQuery)
                .map(row -> convertRowToRecord(row, jooqQuery))
                .first();
    }

    public static Mono<Boolean> fetchExists(Select<?> jooqQuery) {
        Select<?> existsQuery = jooqQuery.configuration().dsl()
                .selectOne()
                .whereExists(jooqQuery);
        return fetchOne(existsQuery)
                .map(Objects::nonNull);
    }

    public static Mono<Integer> fetchCount(Select<?> jooqQuery) {
        Select<?> countQuery = jooqQuery.configuration().dsl()
                .selectCount()
                .from(jooqQuery);
        return fetchOne(countQuery)
                .map(record -> record.get(0, Integer.class));
    }

    private static DatabaseClient.GenericExecuteSpec executeForR2dbcHandle(Query jooqQuery) {
        DatabaseClient databaseClient = (DatabaseClient) jooqQuery.configuration().data("databaseClient");
        String sql = jooqQuery.getSQL(ParamType.NAMED);
        DatabaseClient.GenericExecuteSpec executeSpec = databaseClient.execute(sql);
        List<Object> bindValues = jooqQuery.getBindValues();
        for (int i = 0; i < bindValues.size(); i++) {
            Object value = bindValues.get(i);
            executeSpec = executeSpec.bind(i, value);
        }
        return executeSpec;
    }

    private static <R extends Record> R convertRowToRecord(Row row, Select<R> jooqQuery) {
        List<Field<?>> fields = jooqQuery.getSelect();

        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            values[i] = row.get(i);
        }

        DSLContext dslContext = jooqQuery.configuration().dsl();
        Record record = dslContext.newRecord(fields);
        record.fromArray(values);
        record.changed(false);

        return record.into(jooqQuery.getRecordType());
    }

}
