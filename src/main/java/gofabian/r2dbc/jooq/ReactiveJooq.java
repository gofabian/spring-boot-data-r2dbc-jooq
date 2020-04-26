package gofabian.r2dbc.jooq;

import io.r2dbc.spi.Row;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.conf.Settings;
import org.springframework.data.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static org.jooq.SQLDialect.*;
import static org.jooq.conf.SettingsTools.updatablePrimaryKeys;


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

    private static final EnumSet<SQLDialect> REFRESH_GENERATED_KEYS = EnumSet.of(DERBY, H2, MARIADB, MYSQL);


    public static <R extends TableRecord<R>> Mono<Integer> insert(R record) {
        DSLContext dslContext = record.configuration().dsl();
        InsertQuery<R> insert = dslContext.insertQuery(record.getTable());
        addChangedValues(record, insert);

        // Don't store records if no value was set by client code
        if (!insert.isExecutable()) {
//            if (log.isDebugEnabled())
//                log.debug("Query is not executable", insert);

            return Mono.just(0);
        }

        // [#814] Refresh identity and/or main unique key values
        // [#1002] Consider also identity columns of non-updatable records
        // [#1537] Avoid refreshing identity columns on batch inserts
        Collection<Field<?>> key = setReturningIfNeeded(record, insert);
        Mono<Integer> monoResult = execute(insert);

        return monoResult.doOnNext(result -> {
            if (result > 0) {
                record.changed(false);

                // [#1859] If an insert was successful try fetching the generated values.
                getReturningIfNeeded(record, insert, key);
            }
        });
    }

    public static <R extends UpdatableRecord<R>> Mono<Integer> update(R record) {
        DSLContext dslContext = record.configuration().dsl();
        UpdateQuery<R> update = dslContext.updateQuery(record.getTable());
        addChangedValues(record, update);
        Tools.addConditions(update, record, record.getTable().getPrimaryKey().getFieldsArray());

        // Don't store records if no value was set by client code
        if (!update.isExecutable()) {
//            if (log.isDebugEnabled())
//                log.debug("Query is not executable", update);

            return Mono.just(0);
        }

        // [#1596] Check if the record was really changed in the database
        // [#1859] Specify the returning clause if needed
        Collection<Field<?>> key = setReturningIfNeeded(record, update);

        Mono<Integer> monoResult = execute(update);

        return monoResult.doOnNext(result -> {
            if (result > 0) {
                record.changed(false);

                // [#1859] If an update was successful try fetching the generated
                getReturningIfNeeded(record, update, key);
            }
        });
    }

    /**
     * Set all changed values of this record to a store query
     */
    private static <R extends TableRecord<R>> void addChangedValues(R record, StoreQuery<R> query) {
        for (Field<?> field : record.fields()) {
            if (record.changed(field)) {
                addValue(record, field, query);
            }
        }
    }

    /**
     * Extracted method to ensure generic type safety.
     */
    private static <T, R extends TableRecord<R>> void addValue(R record, Field<T> field, StoreQuery<?> store) {
        T value = record.get(field);
        store.addValue(field, Tools.field(value, field));
    }

    private static <R extends TableRecord<R>> Collection<Field<?>> setReturningIfNeeded(R record, StoreQuery<R> query) {
        Collection<Field<?>> key = null;

        Configuration configuration = record.configuration();
        if (record.configuration() != null) {
            Settings settings = configuration.settings();

            // [#7966] Allow users to turning off the returning clause entirely
            if (!FALSE.equals(settings.isReturnIdentityOnUpdatableRecord())
                    && !TRUE.equals(data(configuration, "DATA_OMIT_RETURNING_CLAUSE"))) {

                // [#1859] Return also non-key columns
                if (TRUE.equals(settings.isReturnAllOnUpdatableRecord())) {
                    key = Arrays.asList(record.fields());
                }

                // [#5940] Getting the primary key mostly doesn't make sense on UPDATE statements
                else if (query instanceof InsertQuery || updatablePrimaryKeys(Tools.settings(record))) {
                    key = getReturning(record);
                }
            }
        }

        if (key != null) {
            query.setReturning(key);
        }

        return key;
    }

    @SuppressWarnings("SameParameterValue")
    private static Object data(Configuration configuration, String keyString) {
        for (Object key : configuration.data().keySet()) {
            if (keyString.equals(key.toString())) {
                return configuration.data(key);
            }
        }
        return null;
    }

    private static <R extends TableRecord<R>> Collection<Field<?>> getReturning(R record) {
        Collection<Field<?>> result = new LinkedHashSet<>();

        Identity<R, ?> identity = record.getTable().getIdentity();
        if (identity != null) {
            result.add(identity.getField());
        }

        UniqueKey<?> key = record.getTable().getPrimaryKey();
        if (key != null) {
            result.addAll(key.getFields());
        }

        return result;
    }

    private static <R extends TableRecord<R>> void getReturningIfNeeded(R record, StoreQuery<R> query, Collection<Field<?>> key) {
        if (key != null && !key.isEmpty()) {
            R returnedRecord = query.getReturnedRecord();

            if (returnedRecord != null) {
                for (Field<?> field : key) {
                    setValue(returnedRecord, record, field);
                    record.changed(field, false);
                }
            }

            // [#1859] In some databases, not all fields can be fetched via getGeneratedKeys()
            if (TRUE.equals(record.configuration().settings().isReturnAllOnUpdatableRecord())
                    && REFRESH_GENERATED_KEYS.contains(record.configuration().family())
                    && record instanceof UpdatableRecord) {
                // todo: refresh
//                ((UpdatableRecord<?>) record).refresh(key.toArray(new Field<?>[0]));
            }
        }
    }

    /**
     * Extracted method to ensure generic type safety.
     */
    private static <T, R extends TableRecord<R>> void setValue(R sourceRecord, R targetRecord, Field<T> field) {
        T value = sourceRecord.get(field);
        targetRecord.setValue(field, value);
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

    /**
     * Execute JOOQ query via R2DBC database client.
     */
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

    /**
     * Convert result from R2DBC database client into JOOQ record.
     */
    private static <R extends Record> R convertRowToRecord(Row row, Select<R> jooqQuery) {
        // get selected fields
        List<Field<?>> fields = jooqQuery.getSelect();

        // collect values in fields order
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            values[i] = row.get(i);
        }

        // create intermediate record
        DSLContext dslContext = jooqQuery.configuration().dsl();
        Record record = dslContext.newRecord(fields);
        record.fromArray(values);
        record.changed(false);

        // convert to expected record type
        return record.into(jooqQuery.getRecordType());
    }

}
