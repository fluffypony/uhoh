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
                    r#"
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
                    "#,
                )
                .await
                .map_err(|e| anyhow::anyhow!(credentials::scrub_error_message(&e.to_string())))?;
        } else {
            for table in tables {
                let trigger_ident = crate::db_guard::quote_pg_ident(&format!(
                    "uhoh_delete_counter_{}",
                    blake3::hash(format!("trigger:{table}").as_bytes()).to_hex()
                ))?;
                let fn_ident = crate::db_guard::quote_pg_ident(&format!(
                    "_uhoh_count_deletes_{}",
                    blake3::hash(table.as_bytes()).to_hex()
                ))?;
                let table_quoted = crate::db_guard::quote_pg_ident(&table)?;
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

