package gofabian.r2dbc.jooq;

import gofabian.r2dbc.jooq.converter.Converter;
import org.jooq.*;
import org.jooq.conf.Settings;
import org.jooq.exception.NoDataFoundException;
import org.jooq.tools.JooqLogger;
import org.springframework.r2dbc.core.DatabaseClient;
import reactor.core.publisher.Mono;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;

import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;

public class ReactiveRecordExecutor {

    private static final JooqLogger log = JooqLogger.getLogger(RowConverter.class);

    private final DSLContext dslContext;
    private final ReactiveQueryExecutor reactiveQueryExecutor;

    public ReactiveRecordExecutor(DSLContext dslContext, DatabaseClient databaseClient, Converter converter) {
        this.dslContext = Objects.requireNonNull(dslContext);
        this.reactiveQueryExecutor = new ReactiveQueryExecutor(dslContext, databaseClient, converter);
    }

    public static ReactiveRecordExecutor from(Attachable attachable) {
        return from(attachable.configuration().dsl());
    }

    public static ReactiveRecordExecutor from(DSLContext dslContext) {
        Configuration configuration = dslContext.configuration();
        DatabaseClient databaseClient = (DatabaseClient) configuration.data("databaseClient");
        Converter converter = (Converter) configuration.data("converter");
        return new ReactiveRecordExecutor(dslContext, databaseClient, converter);
    }

    @Support
    public Mono<Integer> store(UpdatableRecord<?> record) {
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

    @Support
    public Mono<Integer> insert(TableRecord<?> record) {
        InsertQuery<?> insert = dslContext.insertQuery(record.getTable());
        addChangedValues(record, insert);
        return executeStore(record, insert);

    }

    @Support
    public Mono<Integer> update(UpdatableRecord<?> record) {
        UpdateQuery<?> update = dslContext.updateQuery(record.getTable());
        addChangedValues(record, update);
        Tools.addConditions(update, record, record.getTable().getPrimaryKey().getFieldsArray());
        return executeStore(record, update);
    }


    private Mono<Integer> executeStore(TableRecord<?> record, StoreQuery<?> insert) {
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
            monoResult = reactiveQueryExecutor.execute(insert);
        } else {
            monoResult = reactiveQueryExecutor.executeReturning(insert)
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
    private void addChangedValues(TableRecord<?> record, StoreQuery<?> query) {
        for (Field<?> field : record.fields()) {
            if (record.changed(field)) {
                addValue(record, field, query);
            }
        }
    }

    /**
     * Extracted method to ensure generic type safety.
     */
    private <T> void addValue(TableRecord<?> record, Field<T> field, StoreQuery<?> store) {
        T value = record.get(field);
        store.addValue(field, Tools.field(value, field));
    }

    private Collection<Field<?>> setReturningIfNeeded(TableRecord<?> record, StoreQuery<?> query) {
        Collection<Field<?>> key = null;

        Configuration configuration = dslContext.configuration();
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
    private Object data(Configuration configuration, String keyString) {
        for (Object key : configuration.data().keySet()) {
            if (keyString.equals(key.toString())) {
                return configuration.data(key);
            }
        }
        return null;
    }

    private Collection<Field<?>> getReturning(TableRecord<?> record) {
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

    private Mono<Void> getReturningIfNeeded(Record returnedRecord, TableRecord<?> record, Collection<Field<?>> key) {
        if (key != null && !key.isEmpty()) {

            if (returnedRecord != null) {
                for (Field<?> field : key) {
                    setValue(returnedRecord, record, field);
                    record.changed(field, false);
                }
            }

            // [JOOQ#1859] In some databases, not all fields can be fetched via getGeneratedKeys()
            if (TRUE.equals(dslContext.settings().isReturnAllOnUpdatableRecord())
                    && dslContext.family() == SQLDialect.MYSQL
                    && record instanceof UpdatableRecord) {
                return refresh((UpdatableRecord<?>) record, key.toArray(new Field<?>[0]));
            }
        }

        return Mono.empty();
    }

    /**
     * Extracted method to ensure generic type safety.
     */
    private <T> void setValue(Record sourceRecord, TableRecord<?> targetRecord, Field<T> field) {
        T value = sourceRecord.get(field);
        targetRecord.setValue(field, value);
    }

    @Support
    public <R extends UpdatableRecord<R>> Mono<Integer> delete(R record) {
        TableField<R, ?>[] keys = record.getTable().getPrimaryKey().getFieldsArray();

        DeleteQuery<R> delete = dslContext.deleteQuery(record.getTable());
        Tools.addConditions(delete, record, keys);

        Mono<Integer> monoResult = reactiveQueryExecutor.execute(delete);

        return monoResult.doFinally(result -> {
            // [JOOQ#673] [JOOQ#3363] If store() is called after delete(), a new INSERT should
            // be executed and the record should be recreated
            record.changed(true);
        });
    }

    @Support
    public <R extends UpdatableRecord<R>> Mono<Void> refresh(R record) {
        return refresh(record, record.fields());
    }

    private Mono<Void> refresh(UpdatableRecord<?> record, Field<?>... refreshFields) {
        SelectQuery<Record> select = dslContext.selectQuery();
        select.addSelect(refreshFields);
        select.addFrom(record.getTable());
        Tools.addConditions(select, record, record.getTable().getPrimaryKey().getFieldsArray());

        Mono<Record> monoRecord = reactiveQueryExecutor.fetchOne(select);

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

}
