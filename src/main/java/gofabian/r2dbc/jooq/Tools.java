package gofabian.r2dbc.jooq;

import org.jooq.*;
import org.jooq.conf.Settings;
import org.jooq.impl.DefaultConfiguration;

import static org.jooq.conf.SettingsTools.updatablePrimaryKeys;
import static org.jooq.impl.DSL.val;

/**
 * Subset of internal JOOQ class org.jooq.impl.Tools
 */
class Tools {

    /**
     * Get an attachable's configuration or a new {@link DefaultConfiguration}
     * if <code>null</code>.
     */
    static Configuration configuration(Attachable attachable) {
        return configuration(attachable.configuration());
    }

    /**
     * Get a configuration or a new {@link DefaultConfiguration} if
     * <code>null</code>.
     */
    static Configuration configuration(Configuration configuration) {
        return configuration != null ? configuration : new DefaultConfiguration();
    }

    /**
     * Get a configuration's settings or default settings if the configuration
     * is <code>null</code>.
     */
    static Settings settings(Attachable attachable) {
        return configuration(attachable).settings();
    }

    /**
     * Add primary key conditions to a query
     */
    @SuppressWarnings("deprecation")
    static void addConditions(org.jooq.ConditionProvider query, Record record, Field<?>... keys) {
        for (Field<?> field : keys)
            addCondition(query, record, field);
    }

    /**
     * Add a field condition to a query
     */
    @SuppressWarnings("deprecation")
    static <T> void addCondition(org.jooq.ConditionProvider provider, Record record, Field<T> field) {

        // [#2764] If primary keys are allowed to be changed, the
        if (updatablePrimaryKeys(settings(record)))
            provider.addConditions(condition(field, record.original(field)));
        else
            provider.addConditions(condition(field, record.get(field)));
    }

    /**
     * Create a <code>null</code>-safe condition.
     */
    static <T> Condition condition(Field<T> field, T value) {
        return (value == null) ? field.isNull() : field.eq(value);
    }

    /**
     * Be sure that a given object is a field.
     *
     * @param value The argument object
     * @param field The field to take the bind value type from
     * @return The argument object itself, if it is a {@link Field}, or a bind
     *         value created from the argument object.
     */
    @SuppressWarnings("unchecked")
    static <T> Field<T> field(Object value, Field<T> field) {

        // Fields can be mixed with constant values
        if (value instanceof Field<?>)
            return (Field<T>) value;

            // [#4771] Any other QueryPart type is not supported here
        else if (value instanceof QueryPart)
            throw fieldExpected(value);

        else
            return val(value, field);
    }

    private static IllegalArgumentException fieldExpected(Object value) {
        return new IllegalArgumentException("Cannot interpret argument of type " + value.getClass() + " as a Field: " + value);
    }

    /**
     * A utility method that fails with an exception if
     * {@link Row#indexOf(Field)} doesn't return any index.
     */
    static int indexOrFail(Row row, Field<?> field) {
        int result = row.indexOf(field);

        if (result < 0)
            throw new IllegalArgumentException("Field (" + field + ") is not contained in Row " + row);

        return result;
    }

}
