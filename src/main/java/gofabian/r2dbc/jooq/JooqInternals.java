package gofabian.r2dbc.jooq;

import org.jooq.*;

import java.util.ArrayList;
import java.util.List;

class JooqInternals {

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

    public static <R extends Record> StoreQuery<R> getQueryDelegate(RowCountQuery abstractDelegatingQuery) {
        return getPrivateField(abstractDelegatingQuery, delegatingQueryField);
    }

    public static <R extends Record> Table<R> getQueryTable(StoreQuery<R> abstractDmlQuery) {
        return getPrivateField(abstractDmlQuery, tableField);
    }

    public static <R extends Record> List<Field<?>> getQueryReturning(StoreQuery<R> abstractDmlQuery) {
        return new ArrayList<>(getPrivateField(abstractDmlQuery, returningListField));
    }

    public static <R extends Record> List<Field<?>> getQueryReturningResolved(StoreQuery<R> abstractDmlQuery) {
        return new ArrayList<>(getPrivateField(abstractDmlQuery, returningResolvedListField));
    }

    private static <R> R getPrivateField(Object object, java.lang.reflect.Field privateField) {
        try {
            //noinspection unchecked
            return (R) privateField.get(object);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Unsupported JOOQ version", e);
        }
    }

}
