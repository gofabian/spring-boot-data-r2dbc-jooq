package gofabian.r2dbc.jooq;

import io.r2dbc.spi.Row;
import org.jooq.*;
import org.jooq.conf.ParamType;
import org.jooq.conf.Settings;
import org.jooq.exception.NoDataFoundException;
import org.jooq.tools.JooqLogger;
import org.springframework.data.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
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

    private static final JooqLogger log = JooqLogger.getLogger(ReactiveJooq.class);

    public static Mono<Integer> store(UpdatableRecord<?> record) {
        TableField<?, ?>[] keys = record.getTable().getPrimaryKey().getFieldsArray();
        boolean executeUpdate = false;

        for (TableField<?, ?> field : keys) {

            // If any primary key value is null or changed
            if (record.changed(field) ||

                    // [JOOQ#3237] or if a NOT NULL primary key value is null, then execute an INSERT
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

    public static Mono<Integer> insert(TableRecord<?> record) {
        DSLContext dslContext = record.configuration().dsl();
        InsertQuery<?> insert = dslContext.insertQuery(record.getTable());
        addChangedValues(record, insert);
        return executeStore(record, insert);

    }

    public static Mono<Integer> update(UpdatableRecord<?> record) {
        DSLContext dslContext = record.configuration().dsl();
        UpdateQuery<?> update = dslContext.updateQuery(record.getTable());
        addChangedValues(record, update);
        Tools.addConditions(update, record, record.getTable().getPrimaryKey().getFieldsArray());
        return executeStore(record, update);
    }


    private static Mono<Integer> executeStore(TableRecord<?> record, StoreQuery<?> insert) {
        // Don't store records if no value was set by client code
        if (!insert.isExecutable()) {
            if (log.isDebugEnabled()) {
                log.debug("Query is not executable", insert);
            }

            return Mono.just(0);
        }

        // [JOOQ#814] Refresh identity and/or main unique key values
        // [JOOQ#1002] Consider also identity columns of non-updatable records
        // [JOOQ#1537] Avoid refreshing identity columns on batch inserts
        Collection<Field<?>> key = setReturningIfNeeded(record, insert);

        Mono<Integer> monoResult;

        if (key == null || key.isEmpty()) {
            monoResult = execute(insert);
        } else {
            monoResult = executeReturning(insert)
                    .collectList()
                    .flatMap(returnedRecords -> {
                        // [JOOQ#1859] If an insert was successful try fetching the generated values.
                        TableRecord<?> r = returnedRecords.isEmpty() ? null : (TableRecord<?>) returnedRecords.get(0);
                        Mono<Void> monoRefresh = getReturningIfNeeded(r, record, key);
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
    private static void addChangedValues(TableRecord<?> record, StoreQuery<?> query) {
        for (Field<?> field : record.fields()) {
            if (record.changed(field)) {
                addValue(record, field, query);
            }
        }
    }

    /**
     * Extracted method to ensure generic type safety.
     */
    private static <T> void addValue(TableRecord<?> record, Field<T> field, StoreQuery<?> store) {
        T value = record.get(field);
        store.addValue(field, Tools.field(value, field));
    }

    private static Collection<Field<?>> setReturningIfNeeded(TableRecord<?> record, StoreQuery<?> query) {
        Collection<Field<?>> key = null;

        Configuration configuration = record.configuration();
        if (record.configuration() != null) {
            Settings settings = configuration.settings();

            // [JOOQ#7966] Allow users to turning off the returning clause entirely
            if (!FALSE.equals(settings.isReturnIdentityOnUpdatableRecord())
                // todo: for batch queries?
//                    && !TRUE.equals(data(configuration, "DATA_OMIT_RETURNING_CLAUSE"))
            ) {

                // [JOOQ#1859] Return also non-key columns
                if (TRUE.equals(settings.isReturnAllOnUpdatableRecord())) {
                    key = Arrays.asList(record.fields());
                }

                // [JOOQ#5940] Getting the primary key mostly doesn't make sense on UPDATE statements
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

    private static Collection<Field<?>> getReturning(TableRecord<?> record) {
        Collection<Field<?>> result = new LinkedHashSet<>();

        Identity<?, ?> identity = record.getTable().getIdentity();
        if (identity != null) {
            result.add(identity.getField());
        }

        UniqueKey<?> key = record.getTable().getPrimaryKey();
        if (key != null) {
            result.addAll(key.getFields());
        }

        return result;
    }

    private static Mono<Void> getReturningIfNeeded(Record returnedRecord, TableRecord<?> record, Collection<Field<?>> key) {
        if (key != null && !key.isEmpty()) {

            if (returnedRecord != null) {
                for (Field<?> field : key) {
                    setValue(returnedRecord, record, field);
                    record.changed(field, false);
                }
            }

            // [JOOQ#1859] In some databases, not all fields can be fetched via getGeneratedKeys()
            if (TRUE.equals(record.configuration().settings().isReturnAllOnUpdatableRecord())
                    && record.configuration().family() == SQLDialect.MYSQL
                    && record instanceof UpdatableRecord) {
                return refresh((UpdatableRecord<?>) record, key.toArray(new Field<?>[0]));
            }
        }

        return Mono.empty();
    }

    /**
     * Extracted method to ensure generic type safety.
     */
    private static <T> void setValue(Record sourceRecord, TableRecord<?> targetRecord, Field<T> field) {
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
            // [JOOQ#673] [JOOQ#3363] If store() is called after delete(), a new INSERT should
            // be executed and the record should be recreated
            record.changed(true);
        });
    }


    public static <R extends UpdatableRecord<R>> Mono<Void> refresh(R record) {
        return refresh(record, record.fields());
    }

    private static Mono<Void> refresh(UpdatableRecord<?> record, Field<?>... refreshFields) {
        DSLContext dslContext = record.configuration().dsl();
        SelectQuery<Record> select = dslContext.selectQuery();
        select.addSelect(refreshFields);
        select.addFrom(record.getTable());
        Tools.addConditions(select, record, record.getTable().getPrimaryKey().getFieldsArray());

        Mono<Record> monoRecord = fetchOne(select);

        monoRecord = monoRecord.doOnNext(returnedRecord -> {
            for (Field<?> field : refreshFields) {
                setValue(returnedRecord, record, field);
                record.changed(field, false);
            }
        });

        return monoRecord.hasElement().flatMap(hasElement -> {
            if (!hasElement) {
                throw new NoDataFoundException("Exactly one row expected for refresh. Record does not exist in database.");
            }
            return Mono.empty();
        });
    }


    public static Mono<Integer> execute(Query jooqQuery) {
        return createR2dbcExecuteSpec(jooqQuery)
                .fetch()
                .rowsUpdated();
    }

    private static final java.lang.reflect.Field delegatingQueryField;
    private static final java.lang.reflect.Field tableField;
    private static final java.lang.reflect.Field returningResolvedListField;
    private static final java.lang.reflect.Field returningListField;

    static {
        try {
            Class<?> delegatingQueryClass = Class.forName("org.jooq.impl.AbstractDelegatingQuery");
            delegatingQueryField = delegatingQueryClass.getDeclaredField("delegate");
            delegatingQueryField.setAccessible(true);
            Class<?> dmlQueryClass = Class.forName("org.jooq.impl.AbstractDMLQuery");
            tableField = dmlQueryClass.getDeclaredField("table");
            tableField.setAccessible(true);
            returningListField = dmlQueryClass.getDeclaredField("returning");
            returningListField.setAccessible(true);
            returningResolvedListField = dmlQueryClass.getDeclaredField("returningResolvedAsterisks");
            returningResolvedListField.setAccessible(true);
        } catch (ClassNotFoundException | NoSuchFieldException e) {
            throw new RuntimeException("Unsupported JOOQ version", e);
        }
    }

    private static <R> R getPrivateField(Object object, java.lang.reflect.Field privateField) {
        try {
            //noinspection unchecked
            return (R) privateField.get(object);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Unsupported JOOQ version", e);
        }
    }

    public static <R extends Record> Flux<R> executeReturning(InsertResultStep<R> query) {
        StoreQuery<R> storeQuery = getPrivateField(query, delegatingQueryField);
        return executeReturning(storeQuery);
    }

    public static <R extends Record> Mono<R> executeReturningOne(InsertResultStep<R> query) {
        StoreQuery<R> storeQuery = getPrivateField(query, delegatingQueryField);
        return executeReturningOne(storeQuery);
    }

    @Support({H2, POSTGRES})
    public static <R extends Record> Flux<R> executeReturning(UpdateResultStep<R> query) {
        StoreQuery<R> storeQuery = getPrivateField(query, delegatingQueryField);
        return executeReturning(storeQuery);
    }

    @Support({H2, POSTGRES})
    public static <R extends Record> Mono<R> executeReturningOne(UpdateResultStep<R> query) {
        StoreQuery<R> storeQuery = getPrivateField(query, delegatingQueryField);
        return executeReturningOne(storeQuery);
    }

    private static <R extends Record> Mono<R> executeReturningOne(StoreQuery<R> query) {
        return executeReturning(query).collectList().flatMap(list -> {
            if (list.isEmpty()) {
                return Mono.empty();
            } else {
                return Mono.just(list.get(0));
            }
        });
    }

    private static <R extends Record> Flux<R> executeReturning(StoreQuery<R> query) {
        DSLContext dslContext = query.configuration().dsl();

        Table<R> table = getPrivateField(query, tableField);
        List<Field<?>> returningFields = new ArrayList<>(getPrivateField(query, returningListField));
        List<Field<?>> returningResolvedFields = new ArrayList<>(getPrivateField(query, returningResolvedListField));

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
                                return ReactiveJooq.fetch(select);
                            }
                        })
                        .map(r -> r.into(recordType));

            case H2:
            case POSTGRES:
            default:
                return executeSpec
                        .map(row -> convertRowToRecord(dslContext, row, returningResolvedFields, recordType))
                        .all();
        }
    }

    public static <R extends Record> Flux<R> fetch(Select<R> jooqQuery) {
        return createR2dbcExecuteSpec(jooqQuery)
                .map(row -> convertRowToRecord(row, jooqQuery))
                .all();
    }

    public static <R extends Record> Mono<R> fetchOne(Select<R> jooqQuery) {
        return createR2dbcExecuteSpec(jooqQuery)
                .map(row -> convertRowToRecord(row, jooqQuery))
                .one();
    }

    public static <R extends Record> Mono<R> fetchAny(Select<R> jooqQuery) {
        return createR2dbcExecuteSpec(jooqQuery)
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
    private static DatabaseClient.GenericExecuteSpec createR2dbcExecuteSpec(Query jooqQuery) {
        DatabaseClient databaseClient = (DatabaseClient) jooqQuery.configuration().data("databaseClient");
        String sql = jooqQuery.getSQL(ParamType.NAMED);
        DatabaseClient.GenericExecuteSpec executeSpec = databaseClient.execute(sql);

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

    private static <R extends Record> R convertRowToRecord(Row row, Select<R> jooqQuery) {
        DSLContext dslContext = jooqQuery.configuration().dsl();
        List<Field<?>> allFields = jooqQuery.getSelect();
        Class<? extends R> recordType = jooqQuery.getRecordType();
        return convertRowToRecord(dslContext, row, allFields, recordType);
    }

    private static <R extends Record> R convertRowToRecord(DSLContext dslContext, Row row, List<Field<?>> fields,
                                                           Class<? extends R> recordType) {
        // collect values in fields order
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            Field<?> field = fields.get(i);
            try {
                values[i] = row.get(i, field.getType());
            } catch (IllegalArgumentException e) {
                // fallback: JOOQ RecordMapper converts the value later
                values[i] = row.get(i, Object.class);
                log.debug("R2DBC cannot convert value to field type: " + field.getType() + ", value=" + values[i]
                        + ", value type=" + (values[i] == null ? null : values[i].getClass()));
            }
        }

        // create intermediate record
        Record record = dslContext.newRecord(fields);
        record.fromArray(values);
        record.changed(false);

        return record.into(recordType);
    }

}
