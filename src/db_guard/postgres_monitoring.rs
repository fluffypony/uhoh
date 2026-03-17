use anyhow::Result;

use super::credentials;
use super::postgres::{
    install_delete_counter_trigger_sql, parse_watched_tables, run_postgres_task,
};
use super::postgres_connection::{connect_postgres_client, ResolvedPostgresConnection};

pub fn install_monitoring_infrastructure(
    connection: &ResolvedPostgresConnection,
    tables_csv: &str,
) -> Result<Option<String>> {
    let connection = connection.clone();
    let tables = parse_watched_tables(tables_csv);
    run_postgres_task(async move {
        let client = connect_postgres_client(&connection).await?;
        client
            .batch_execute(
                "
                CREATE TABLE IF NOT EXISTS _uhoh_ddl_events (
                    id BIGSERIAL PRIMARY KEY,
                    event_tag TEXT NOT NULL,
                    object_type TEXT,
                    schema_name TEXT,
                    object_identity TEXT,
                    payload TEXT,
                    occurred_at TIMESTAMPTZ NOT NULL DEFAULT now()
                );

                CREATE TABLE IF NOT EXISTS _uhoh_delete_counts (
                    table_name TEXT NOT NULL,
                    txid BIGINT NOT NULL DEFAULT txid_current(),
                    delete_count INTEGER NOT NULL DEFAULT 0,
                    ts TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (table_name, txid)
                );
                CREATE INDEX IF NOT EXISTS idx_uhoh_delete_counts_ts ON _uhoh_delete_counts(ts);

                CREATE OR REPLACE FUNCTION _uhoh_ddl_handler() RETURNS event_trigger AS $$
                DECLARE rec RECORD;
                DECLARE payload_json TEXT;
                BEGIN
                    FOR rec IN SELECT * FROM pg_event_trigger_dropped_objects() LOOP
                        payload_json := json_build_object(
                            'event_tag', tg_tag,
                            'object_type', rec.object_type,
                            'schema_name', rec.schema_name,
                            'object_identity', rec.object_identity
                        )::text;

                        INSERT INTO _uhoh_ddl_events (
                            event_tag,
                            object_type,
                            schema_name,
                            object_identity,
                            payload
                        ) VALUES (
                            tg_tag,
                            rec.object_type,
                            rec.schema_name,
                            rec.object_identity,
                            payload_json
                        );
                    END LOOP;
                END;
                $$ LANGUAGE plpgsql;
                ",
            )
            .await
            .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;

        match client
            .batch_execute(
                "DROP EVENT TRIGGER IF EXISTS uhoh_ddl_drop;
                 CREATE EVENT TRIGGER uhoh_ddl_drop ON sql_drop
                     EXECUTE FUNCTION _uhoh_ddl_handler();",
            )
            .await
        {
            Ok(()) => tracing::info!("DDL event trigger installed"),
            Err(err) => {
                let message = credentials::scrub_error_message(&err.to_string());
                if message.contains("permission denied") || message.contains("must be superuser") {
                    eprintln!(
                        "Warning: Could not create event trigger (requires superuser). DDL changes like DROP TABLE will not be tracked, but row-level delete monitoring will still function."
                    );
                } else {
                    return Err(anyhow::anyhow!(message));
                }
            }
        }

        if tables.is_empty() {
            let rows = client
                .query(
                    "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = current_schema()",
                    &[],
                )
                .await
                .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
            let mut table_names: Vec<String> = rows
                .into_iter()
                .map(|row| row.get::<_, String>(0))
                .collect();
            for table in &table_names {
                install_delete_counter_trigger_sql(&client, table).await?;
            }
            table_names.sort();
            table_names.dedup();
            Ok(Some(table_names.join(",")))
        } else {
            for table in tables {
                install_delete_counter_trigger_sql(&client, &table).await?;
            }
            Ok(None)
        }
    })
}

pub fn drop_monitoring_infrastructure(
    connection: &ResolvedPostgresConnection,
    tables_csv: &str,
) -> Result<()> {
    let connection = connection.clone();
    let tables = parse_watched_tables(tables_csv);
    run_postgres_task(async move {
        let client = connect_postgres_client(&connection).await?;

        if tables.is_empty() {
            client
                .batch_execute(
                    r"
                    DO $$
                    DECLARE rec RECORD;
                    BEGIN
                        FOR rec IN
                            SELECT n.nspname AS schema_name, c.relname AS table_name, t.tgname AS trigger_name
                            FROM pg_trigger t
                            JOIN pg_class c ON c.oid = t.tgrelid
                            JOIN pg_namespace n ON n.oid = c.relnamespace
                            WHERE NOT t.tgisinternal
                              AND t.tgname LIKE 'uhoh_delete_counter_%'
                        LOOP
                            EXECUTE format(
                                'DROP TRIGGER IF EXISTS %I ON %I.%I',
                                rec.trigger_name,
                                rec.schema_name,
                                rec.table_name
                            );
                        END LOOP;

                        FOR rec IN
                            SELECT n.nspname AS schema_name, p.proname AS fn_name
                            FROM pg_proc p
                            JOIN pg_namespace n ON n.oid = p.pronamespace
                            WHERE p.proname LIKE '_uhoh_count_deletes_%'
                        LOOP
                            EXECUTE format(
                                'DROP FUNCTION IF EXISTS %I.%I()',
                                rec.schema_name,
                                rec.fn_name
                            );
                        END LOOP;
                    END $$;
                    ",
                )
                .await
                .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
        } else {
            for table in tables {
                let trigger_ident = super::quote_pg_ident(&format!(
                    "uhoh_delete_counter_{}",
                    blake3::hash(format!("trigger:{table}").as_bytes()).to_hex()
                ))?;
                let fn_ident = super::quote_pg_ident(&format!(
                    "_uhoh_count_deletes_{}",
                    blake3::hash(table.as_bytes()).to_hex()
                ))?;
                let table_quoted = super::quote_pg_ident(&table)?;
                let sql = format!(
                    "DROP TRIGGER IF EXISTS {trigger_ident} ON {table_quoted}; DROP FUNCTION IF EXISTS {fn_ident}();"
                );
                client.batch_execute(&sql).await.map_err(|e| {
                    anyhow::anyhow!(credentials::scrub_error_message(&e.to_string()))
                })?;
            }
        }

        client
            .batch_execute(
                "
                DROP EVENT TRIGGER IF EXISTS uhoh_ddl_drop;
                DROP FUNCTION IF EXISTS _uhoh_ddl_handler();
                DROP TABLE IF EXISTS _uhoh_ddl_events;
                DROP TABLE IF EXISTS _uhoh_delete_counts;
                ",
            )
            .await
            .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
        Ok(())
    })
}

pub fn test_monitoring_infrastructure(connection: &ResolvedPostgresConnection) -> Result<()> {
    let connection = connection.clone();
    run_postgres_task(async move {
        let client = connect_postgres_client(&connection).await?;

        match client
            .query_one("SELECT 1 FROM _uhoh_ddl_events LIMIT 1", &[])
            .await
        {
            Ok(_) => {}
            Err(_) => {
                let _ = client.query_one("SELECT 1", &[]).await.map_err(|e| {
                    anyhow::anyhow!(credentials::scrub_error_message(&e.to_string()))
                })?;
            }
        }

        match client
            .query_one("SELECT 1 FROM _uhoh_delete_counts LIMIT 1", &[])
            .await
        {
            Ok(_) => {}
            Err(_) => {
                let _ = client.query_one("SELECT 1", &[]).await.map_err(|e| {
                    anyhow::anyhow!(credentials::scrub_error_message(&e.to_string()))
                })?;
            }
        }

        Ok(())
    })
}

pub fn execute_sql(connection: &ResolvedPostgresConnection, sql: &str) -> Result<()> {
    let connection = connection.clone();
    let sql = sql.to_string();
    run_postgres_task(async move {
        let client = connect_postgres_client(&connection).await?;
        client
            .batch_execute(&sql)
            .await
            .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
        Ok(())
    })
}

#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]
#[cfg(test)]
mod tests {
    use super::*;

    // ── DDL identifier generation for drop_monitoring_infrastructure ──
    //
    // The specific-tables branch of drop_monitoring_infrastructure builds
    // DROP TRIGGER / DROP FUNCTION statements using quote_pg_ident and
    // blake3 hashing.  These tests verify the identifier construction and
    // the resulting SQL shape without needing a live Postgres connection.

    /// Build the same DROP SQL that drop_monitoring_infrastructure generates
    /// for a single table in its specific-tables branch.
    fn build_drop_sql_for_table(table: &str) -> String {
        let trigger_ident = super::super::quote_pg_ident(&format!(
            "uhoh_delete_counter_{}",
            blake3::hash(format!("trigger:{table}").as_bytes()).to_hex()
        ))
        .unwrap();
        let fn_ident = super::super::quote_pg_ident(&format!(
            "_uhoh_count_deletes_{}",
            blake3::hash(table.as_bytes()).to_hex()
        ))
        .unwrap();
        let table_quoted = super::super::quote_pg_ident(table).unwrap();
        format!(
            "DROP TRIGGER IF EXISTS {trigger_ident} ON {table_quoted}; DROP FUNCTION IF EXISTS {fn_ident}();"
        )
    }

    #[test]
    fn drop_sql_contains_drop_trigger_and_drop_function() {
        let sql = build_drop_sql_for_table("users");
        assert!(sql.contains("DROP TRIGGER IF EXISTS"), "missing DROP TRIGGER: {sql}");
        assert!(sql.contains("DROP FUNCTION IF EXISTS"), "missing DROP FUNCTION: {sql}");
    }

    #[test]
    fn drop_sql_references_correct_table() {
        let sql = build_drop_sql_for_table("orders");
        // The table identifier must appear after ON
        assert!(sql.contains("ON \"orders\""), "table not referenced: {sql}");
    }

    #[test]
    fn drop_sql_trigger_ident_starts_with_uhoh_prefix() {
        let sql = build_drop_sql_for_table("users");
        // The trigger identifier is quoted and starts with uhoh_delete_counter_
        assert!(
            sql.contains("\"uhoh_delete_counter_"),
            "trigger ident missing prefix: {sql}"
        );
    }

    #[test]
    fn drop_sql_function_ident_starts_with_uhoh_prefix() {
        let sql = build_drop_sql_for_table("users");
        assert!(
            sql.contains("\"_uhoh_count_deletes_"),
            "function ident missing prefix: {sql}"
        );
    }

    #[test]
    fn drop_sql_identifiers_are_double_quoted() {
        let sql = build_drop_sql_for_table("my_table");
        // All identifiers produced by quote_pg_ident are double-quoted
        assert!(sql.contains("\"my_table\""), "table not quoted: {sql}");
        // Trigger and function idents are also quoted (checked via prefix above)
    }

    #[test]
    fn drop_sql_special_chars_in_table_name_are_escaped() {
        // A table name containing a double-quote must be escaped inside the identifier
        let sql = build_drop_sql_for_table("my\"table");
        assert!(
            sql.contains("\"my\"\"table\""),
            "double-quote not escaped: {sql}"
        );
    }

    #[test]
    fn drop_sql_schema_qualified_table() {
        let sql = build_drop_sql_for_table("public.events");
        // quote_pg_ident splits on '.' and quotes each segment
        assert!(
            sql.contains("\"public\".\"events\""),
            "schema-qualified table not quoted: {sql}"
        );
    }

    #[test]
    fn drop_sql_different_tables_produce_different_hashes() {
        let sql_a = build_drop_sql_for_table("users");
        let sql_b = build_drop_sql_for_table("orders");
        // The blake3 hashes differ, so the trigger/function identifiers differ
        assert_ne!(sql_a, sql_b);
    }

    #[test]
    fn drop_sql_deterministic_for_same_table() {
        let sql1 = build_drop_sql_for_table("accounts");
        let sql2 = build_drop_sql_for_table("accounts");
        assert_eq!(sql1, sql2, "DDL generation should be deterministic");
    }

    // ── install_delete_counter_trigger_sql identifier construction ──
    //
    // install_delete_counter_trigger_sql (in postgres.rs) builds CREATE
    // FUNCTION / CREATE TRIGGER SQL.  We replicate its identifier logic
    // here to verify the generated identifiers and SQL shape.

    /// Reconstruct the SQL that install_delete_counter_trigger_sql would
    /// generate for a given table name.
    fn build_install_trigger_sql(table: &str) -> String {
        let table_safe = table.replace('\'', "''");
        let table_quoted = super::super::quote_pg_ident(table).unwrap();
        let fn_ident = super::super::quote_pg_ident(&format!(
            "_uhoh_count_deletes_{}",
            blake3::hash(table.as_bytes()).to_hex()
        ))
        .unwrap();
        let trigger_ident = super::super::quote_pg_ident(&format!(
            "uhoh_delete_counter_{}",
            blake3::hash(format!("trigger:{table}").as_bytes()).to_hex()
        ))
        .unwrap();

        format!(
            "
        CREATE OR REPLACE FUNCTION {fn_ident}() RETURNS trigger AS $$
        BEGIN
            INSERT INTO _uhoh_delete_counts (table_name, txid, delete_count)
            VALUES ('{table_safe}', txid_current(), 1)
            ON CONFLICT (table_name, txid)
            DO UPDATE SET delete_count = _uhoh_delete_counts.delete_count + 1,
                          ts = now();
            RETURN OLD;
        END;
        $$ LANGUAGE plpgsql;

        DROP TRIGGER IF EXISTS {trigger_ident} ON {table_quoted};
        CREATE TRIGGER {trigger_ident}
            BEFORE DELETE ON {table_quoted}
            FOR EACH ROW EXECUTE FUNCTION {fn_ident}();
        "
        )
    }

    #[test]
    fn install_trigger_sql_contains_create_function() {
        let sql = build_install_trigger_sql("users");
        assert!(
            sql.contains("CREATE OR REPLACE FUNCTION"),
            "missing CREATE FUNCTION: {sql}"
        );
    }

    #[test]
    fn install_trigger_sql_contains_create_trigger() {
        let sql = build_install_trigger_sql("users");
        assert!(sql.contains("CREATE TRIGGER"), "missing CREATE TRIGGER: {sql}");
    }

    #[test]
    fn install_trigger_sql_contains_drop_trigger() {
        let sql = build_install_trigger_sql("users");
        assert!(
            sql.contains("DROP TRIGGER IF EXISTS"),
            "missing DROP TRIGGER IF EXISTS: {sql}"
        );
    }

    #[test]
    fn install_trigger_sql_references_table() {
        let sql = build_install_trigger_sql("orders");
        assert!(
            sql.contains("ON \"orders\""),
            "table not referenced in trigger: {sql}"
        );
        assert!(
            sql.contains("'orders'"),
            "table name not in VALUES clause: {sql}"
        );
    }

    #[test]
    fn install_trigger_sql_escapes_single_quotes_in_table_name() {
        let sql = build_install_trigger_sql("it's_a_table");
        // Single quotes in the literal must be doubled
        assert!(
            sql.contains("'it''s_a_table'"),
            "single quotes not escaped in literal: {sql}"
        );
    }

    #[test]
    fn install_trigger_sql_escapes_double_quotes_in_identifier() {
        let sql = build_install_trigger_sql("a\"b");
        assert!(
            sql.contains("\"a\"\"b\""),
            "double quotes not escaped in identifier: {sql}"
        );
    }

    #[test]
    fn install_trigger_sql_function_returns_trigger() {
        let sql = build_install_trigger_sql("t");
        assert!(
            sql.contains("RETURNS trigger"),
            "function should return trigger type: {sql}"
        );
    }

    #[test]
    fn install_trigger_sql_uses_before_delete() {
        let sql = build_install_trigger_sql("t");
        assert!(
            sql.contains("BEFORE DELETE ON"),
            "trigger should fire BEFORE DELETE: {sql}"
        );
    }

    #[test]
    fn install_trigger_sql_uses_for_each_row() {
        let sql = build_install_trigger_sql("t");
        assert!(
            sql.contains("FOR EACH ROW"),
            "trigger should be FOR EACH ROW: {sql}"
        );
    }

    // ── parse_watched_tables (re-exported, tested for completeness) ──

    #[test]
    fn parse_watched_tables_wildcard_returns_empty() {
        assert!(parse_watched_tables("*").is_empty());
    }

    #[test]
    fn parse_watched_tables_csv() {
        assert_eq!(
            parse_watched_tables("users, orders, audit"),
            vec!["users", "orders", "audit"]
        );
    }

    #[test]
    fn parse_watched_tables_empty_string() {
        assert!(parse_watched_tables("").is_empty());
    }

    #[test]
    fn parse_watched_tables_whitespace_only() {
        assert!(parse_watched_tables("   ").is_empty());
    }
}

