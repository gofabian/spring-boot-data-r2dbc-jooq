package gofabian.r2dbc.jooq;

import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.conf.Settings;
import org.jooq.exception.NoDataFoundException;
import org.jooq.tools.JooqLogger;
import org.springframework.data.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;


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

    private static final JooqLogger log = JooqLogger.getLogger(ReactiveJooq.class);

    private static final EnumSet<SQLDialect> REFRESH_GENERATED_KEYS = EnumSet.of(SQLDialect.MYSQL);

    public static <R extends UpdatableRecord<R>> Mono<Integer> store(R record) {
        TableField<R, ?>[] keys = record.getTable().getPrimaryKey().getFieldsArray();
        boolean executeUpdate = false;

        for (TableField<R, ?> field : keys) {

            // If any primary key value is null or changed
            if (record.changed(field) ||

                    // [#3237] or if a NOT NULL primary key value is null, then execute an INSERT
                    (!field.getDataType().nullable() && record.get(field) == null)) {
                executeUpdate = false;
                break;
            }

            // Otherwise, updates are possible
            executeUpdate = true;
        }

        if (executeUpdate) {
            return update(record);
        } else {
            return insert(record);
        }
    }

    public static <R extends TableRecord<R>> Mono<Integer> insert(R record) {
        DSLContext dslContext = record.configuration().dsl();
        InsertQuery<R> insert = dslContext.insertQuery(record.getTable());
        addChangedValues(record, insert);
        return executeStore(record, insert);

    }

    public static <R extends UpdatableRecord<R>> Mono<Integer> update(R record) {
        DSLContext dslContext = record.configuration().dsl();
        UpdateQuery<R> update = dslContext.updateQuery(record.getTable());
        addChangedValues(record, update);
        Tools.addConditions(update, record, record.getTable().getPrimaryKey().getFieldsArray());
        return executeStore(record, update);
    }


    private static <R extends TableRecord<R>> Mono<Integer> executeStore(R record, StoreQuery<R> insert) {
        // Don't store records if no value was set by client code
        if (!insert.isExecutable()) {
            if (log.isDebugEnabled()) {
                log.debug("Query is not executable", insert);
            }

            return Mono.just(0);
        }

        // [#814] Refresh identity and/or main unique key values
        // [#1002] Consider also identity columns of non-updatable records
        // [#1537] Avoid refreshing identity columns on batch inserts
        Collection<Field<?>> key = setReturningIfNeeded(record, insert);

        Mono<Integer> monoResult;

        if (key == null || key.isEmpty()) {
            monoResult = execute(insert);
        } else {
            monoResult = createR2dbcExecuteSpec(insert)
                    .filter(s -> {
                        if (record.configuration().family() == SQLDialect.MYSQL) {
                            return s.returnGeneratedValues();
                        } else {
                            String[] keyNames = key.stream().map(Field::getName).toArray(String[]::new);
                            return s.returnGeneratedValues(keyNames);
                        }
                    })
                    .map((row, metadata) -> convertRowToRecord(row, metadata, record))
                    .one()
                    .flatMap(returnedRecord -> {
                        // [#1859] If an insert was successful try fetching the generated values.
                        Mono<Void> monoRefresh = getReturningIfNeeded(returnedRecord, record, key);
                        return monoRefresh.thenReturn(record);
                    })
                    .hasElement()
                    .map(hasElement -> hasElement ? 1 : 0);
        }

        return monoResult.doOnNext(result -> {
            if (result > 0) {
                record.changed(false);
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
                // todo: for batch queries?
//                    && !TRUE.equals(data(configuration, "DATA_OMIT_RETURNING_CLAUSE"))
            ) {

                // [#1859] Return also non-key columns
                if (TRUE.equals(settings.isReturnAllOnUpdatableRecord())) {
                    key = Arrays.asList(record.fields());
                }

                // [#5940] Getting the primary key mostly doesn't make sense on UPDATE statements
                else if (query instanceof InsertQuery) {
                    key = getReturning(record);
                }
            }
        }

        if (key != null) {
            query.setReturning(key);
        }

        return key;
    }

    // todo: this is not very efficient
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

    private static <R extends TableRecord<R>, T extends UpdatableRecord<T>> Mono<Void> getReturningIfNeeded(Record returnedRecord, R record, Collection<Field<?>> key) {
        if (key != null && !key.isEmpty()) {

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
                //noinspection unchecked
//                return refresh((T) record, key.toArray(new Field<?>[0]));
//                ((UpdatableRecord<?>) record).refresh(key.toArray(new Field<?>[0]));
            }
        }

        return Mono.empty();
    }

    /**
     * Extracted method to ensure generic type safety.
     */
    private static <T, R extends TableRecord<R>> void setValue(Record sourceRecord, R targetRecord, Field<T> field) {
        T value = sourceRecord.get(field);
        targetRecord.setValue(field, value);
    }

    public static <R extends UpdatableRecord<R>> Mono<Integer> delete(R record) {
        DSLContext dslContext = record.configuration().dsl();
        TableField<R, ?>[] keys = record.getTable().getPrimaryKey().getFieldsArray();

        DeleteQuery<R> delete = dslContext.deleteQuery(record.getTable());
        Tools.addConditions(delete, record, keys);

        Mono<Integer> monoResult = execute(delete);

        return monoResult.doFinally(result -> {
            // [#673] [#3363] If store() is called after delete(), a new INSERT should
            // be executed and the record should be recreated
            record.changed(true);
        });
    }


    public static Mono<Integer> execute(Query jooqQuery) {
        return createR2dbcExecuteSpec(jooqQuery)
                .fetch()
                .rowsUpdated();
    }

    public static <R extends Record> Flux<R> fetch(Select<R> jooqQuery) {
        return createR2dbcExecuteSpec(jooqQuery)
                .map(row -> convertRowToGenericRecord(row, jooqQuery))
                .all();
    }

    public static <R extends Record> Mono<R> fetchOne(Select<R> jooqQuery) {
        return createR2dbcExecuteSpec(jooqQuery)
                .map(row -> convertRowToGenericRecord(row, jooqQuery))
                .one();
    }

    public static <R extends Record> Mono<R> fetchAny(Select<R> jooqQuery) {
        return createR2dbcExecuteSpec(jooqQuery)
                .map(row -> convertRowToGenericRecord(row, jooqQuery))
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
    private static DatabaseClient.GenericExecuteSpec createR2dbcExecuteSpec(Query jooqQuery) {
        DatabaseClient databaseClient = (DatabaseClient) jooqQuery.configuration().data("databaseClient");
        String sql = jooqQuery.getSQL(ParamType.NAMED);
        DatabaseClient.GenericExecuteSpec executeSpec = databaseClient.execute(sql);
        List<Object> bindValues = jooqQuery.getBindValues();
        for (int i = 0; i < bindValues.size(); i++) {
            Object value = bindValues.get(i);
            if (value == null) {
                // with the random type (Boolean) we select an R2DBC codec that can encode null
                executeSpec = executeSpec.bindNull(i, Boolean.class);
            } else {
                executeSpec = executeSpec.bind(i, value);
            }
        }
        return executeSpec;
    }

    /**
     * Convert result from R2DBC database client into JOOQ record.
     */
    private static <R extends Record> R convertRowToGenericRecord(Row row, Select<R> jooqQuery) {
        // get selected fields
        List<Field<?>> fields = jooqQuery.getSelect();

        // collect values in fields order
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            values[i] = row.get(i, fields.get(i).getType());
        }

        // create intermediate record
        DSLContext dslContext = jooqQuery.configuration().dsl();
        Record record = dslContext.newRecord(fields);
        record.fromArray(values);
        record.changed(false);

        // convert to expected record type
        return record.into(jooqQuery.getRecordType());
    }

    private static <R extends TableRecord<R>> Record convertRowToRecord(Row row, RowMetadata metadata, R tableRecord) {
        DSLContext dslContext = tableRecord.configuration().dsl();
        Table<R> table = tableRecord.getTable();

        // collect table fields by name
        List<Field<?>> fields = new ArrayList<>();
        metadata.getColumnMetadatas().forEach(column -> {
            for (Field<?> tableField : table.fields()) {
                if (tableField.getName().equals(column.getName())) {
                    fields.add(tableField);
                    return;
                }
            }
            throw new IllegalArgumentException("Table '" + table.getName() + "' does not contain field '"
                    + column.getName() + "'");
        });

        // collect values in fields order
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field<?> field = fields.get(i);
            values[i] = row.get(i, field.getType());
        }

        // create intermediate record
        Record record = dslContext.newRecord(fields);
        record.fromArray(values);
        record.changed(false);
        return record;
    }

}
