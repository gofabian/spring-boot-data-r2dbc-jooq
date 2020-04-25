package gofabian.r2dbc.jooq;

import org.jooq.*;
import org.jooq.conf.Settings;
import org.jooq.impl.DefaultConfiguration;

import static org.jooq.conf.SettingsTools.updatablePrimaryKeys;

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

}
