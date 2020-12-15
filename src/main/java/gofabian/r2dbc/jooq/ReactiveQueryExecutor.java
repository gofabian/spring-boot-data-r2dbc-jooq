package gofabian.r2dbc.jooq;

import io.r2dbc.spi.Row;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.jooq.SQLDialect.H2;
import static org.jooq.SQLDialect.POSTGRES;

public class ReactiveQueryExecutor {

    private final DSLContext dslContext;
    private final DatabaseClient databaseClient;

    public ReactiveQueryExecutor(DSLContext dslContext, DatabaseClient databaseClient) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.databaseClient = Objects.requireNonNull(databaseClient);
    }

    public static ReactiveQueryExecutor from(Attachable attachable) {
        return from(attachable.configuration().dsl());
    }

    public static ReactiveQueryExecutor from(DSLContext dslContext) {
        Configuration configuration = dslContext.configuration();
        DatabaseClient databaseClient = (DatabaseClient) configuration.data("databaseClient");
        return new ReactiveQueryExecutor(dslContext, databaseClient);
    }


    @Support
    public Mono<Integer> execute(Query jooqQuery) {
        return createR2dbcExecuteSpec(jooqQuery)
                .fetch()
                .rowsUpdated();
    }

    @Support
    public <R extends Record> Flux<R> executeReturning(InsertResultStep<R> query) {
        StoreQuery<R> storeQuery = JooqInternals.getQueryDelegate(query);
        return executeReturning(storeQuery);
    }

    @Support
    public <R extends Record> Mono<R> executeReturningOne(InsertResultStep<R> query) {
        StoreQuery<R> storeQuery = JooqInternals.getQueryDelegate(query);
        return executeReturningOne(storeQuery);
    }

    @Support({H2, POSTGRES})
    public <R extends Record> Flux<R> executeReturning(UpdateResultStep<R> query) {
        StoreQuery<R> storeQuery = JooqInternals.getQueryDelegate(query);
        return executeReturning(storeQuery);
    }

    @Support({H2, POSTGRES})
    public <R extends Record> Mono<R> executeReturningOne(UpdateResultStep<R> query) {
        StoreQuery<R> storeQuery = JooqInternals.getQueryDelegate(query);
        return executeReturningOne(storeQuery);
    }

    @Support
    public <R extends Record> Flux<R> fetch(Select<R> jooqQuery) {
        return createR2dbcExecuteSpec(jooqQuery)
                .map(row -> convertSelectedRowToRecord(row, jooqQuery))
                .all();
    }

    @Support
    public <R extends Record> Mono<R> fetchOne(Select<R> jooqQuery) {
        return createR2dbcExecuteSpec(jooqQuery)
                .map(row -> convertSelectedRowToRecord(row, jooqQuery))
                .one();
    }

    @Support
    public <R extends Record> Mono<R> fetchAny(Select<R> jooqQuery) {
        return createR2dbcExecuteSpec(jooqQuery)
                .map(row -> convertSelectedRowToRecord(row, jooqQuery))
                .first();
    }

    private <R extends Record> R convertSelectedRowToRecord(Row row, Select<R> jooqQuery) {
        List<Field<?>> allFields = jooqQuery.getSelect();
        Class<? extends R> recordType = jooqQuery.getRecordType();
        return RowConverter.convertRowToRecord(dslContext, row, allFields, recordType);
    }

    @Support
    public Mono<Boolean> fetchExists(Select<?> jooqQuery) {
        Select<?> existsQuery = dslContext
                .selectOne()
                .whereExists(jooqQuery);
        return fetchOne(existsQuery)
                .map(Objects::nonNull);
    }

    @Support
    public Mono<Integer> fetchCount(Select<?> jooqQuery) {
        Select<?> countQuery = dslContext
                .selectCount()
                .from(jooqQuery);
        return fetchOne(countQuery)
                .map(record -> record.get(0, Integer.class));
    }

    private <R extends Record> Mono<R> executeReturningOne(StoreQuery<R> query) {
        return executeReturning(query).collectList().flatMap(list -> {
            if (list.isEmpty()) {
                return Mono.empty();
            } else {
                return Mono.just(list.get(0));
            }
        });
    }

    <R extends Record> Flux<R> executeReturning(StoreQuery<R> query) {
        Table<R> table = JooqInternals.getQueryTable(query);
        List<Field<?>> returningFields = JooqInternals.getQueryReturning(query);
        List<Field<?>> returningResolvedFields = JooqInternals.getQueryReturningResolved(query);

        //noinspection unchecked
        Class<R> recordType = (Class<R>) table.getRecordType();

        // create R2DBC execution spec without "RETURNING" clause
        query.setReturning(Collections.emptyList());
        DatabaseClient.GenericExecuteSpec executeSpec = createR2dbcExecuteSpec(query);
        query.setReturning(returningFields);

        if (returningFields.isEmpty()) {
            return executeSpec.then().flatMapMany(x -> Flux.empty());
        }

        // require generated values in result set
        executeSpec = executeSpec.filter(s -> {
            switch (dslContext.family()) {
                case MYSQL:
                    // MySQL can only return generated id
                    return s.returnGeneratedValues();
                case H2:
                case POSTGRES:
                default:
                    String[] fieldNames = returningResolvedFields.stream().map(Field::getName).toArray(String[]::new);
                    return s.returnGeneratedValues(fieldNames);
            }
        });

        // convert result to records
        switch (dslContext.family()) {
            case MYSQL:
                Identity<R, ?> identity = table.getIdentity();
                // This shouldn't be null, as relevant dialects should
                // return empty generated keys ResultSet
                if (identity == null) {
                    return executeSpec.then().flatMapMany(x -> Flux.empty());
                }
                //noinspection unchecked
                Field<Object> idField = (Field<Object>) identity.getField();

                return executeSpec
                        .map(row -> {
                            Object value = row.get(0, Object.class);
                            return idField.getDataType().convert(value);
                        })
                        .all().collectList()
                        .flatMapMany(ids -> {
                            if (returningResolvedFields.size() == 1 &&
                                    returningResolvedFields.get(0).getName().equals(idField.getName())) {
                                // Only the IDENTITY value was requested. No need for an additional query
                                List<Record> records = ids.stream().map(id -> {
                                    Record record = dslContext.newRecord(returningResolvedFields);
                                    record.set(idField, id);
                                    record.changed(false);
                                    return record;
                                }).collect(Collectors.toList());
                                return Flux.fromIterable(records);
                            } else {
                                // Other values are requested, too. Run another query
                                Select<Record> select = dslContext
                                        .select(returningFields)
                                        .from(table)
                                        .where(idField.in(ids));
                                return fetch(select);
                            }
                        })
                        .map(r -> r.into(recordType));

            case H2:
            case POSTGRES:
            default:
                return executeSpec
                        .map(row -> RowConverter.convertRowToRecord(dslContext, row, returningResolvedFields, recordType))
                        .all();
        }
    }

    /**
     * Execute JOOQ query via R2DBC database client.
     */
    private DatabaseClient.GenericExecuteSpec createR2dbcExecuteSpec(Query jooqQuery) {
        String sql = jooqQuery.getSQL(ParamType.NAMED);
        DatabaseClient.GenericExecuteSpec executeSpec = databaseClient.sql(sql);

        List<Param<?>> parameters = jooqQuery.getParams().values().stream()
                .filter(p -> p.getParamType() != ParamType.INLINED).collect(Collectors.toList());
        for (int i = 0; i < parameters.size(); i++) {
            Param<?> parameter = parameters.get(i);
            Object bindValue = parameter.getValue();
            if (bindValue == null) {
                executeSpec = executeSpec.bindNull(i, parameter.getType());
            } else {
                executeSpec = executeSpec.bind(i, bindValue);
            }
        }
        return executeSpec;
    }


}
