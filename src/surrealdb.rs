use color_eyre::eyre::{ContextCompat, Result, eyre};
use itertools::Itertools;
use serde::Deserialize;
use std::collections::HashMap;
use surrealdb::{Connection, Surreal};

use crate::{constants::SCRIPT_MIGRATION_TABLE_NAME, models::ScriptMigration};

pub async fn get_surrealdb_table_exists<C: Connection>(
    client: &Surreal<C>,
    table: &str,
) -> Result<bool> {
    let tables = get_surrealdb_table_definitions(client).await?;
    Ok(tables.contains_key(table))
}

type SurrealdbTableDefinitions = HashMap<String, String>;

pub async fn get_surrealdb_table_definitions<C: Connection>(
    client: &Surreal<C>,
) -> Result<SurrealdbTableDefinitions> {
    let response = client
        .query(surrealdb::sql::statements::InfoStatement::Db(false, None))
        .await?;

    let mut response = response.check()?;

    let result: Option<SurrealdbTableDefinitions> = response.take("tables")?;
    let table_definitions = result.context("Failed to get table definitions")?;

    Ok(table_definitions)
}

#[derive(Debug, Deserialize)]
pub struct SurrealdbTableDefinition {
    pub fields: HashMap<String, String>,
}

pub async fn get_surrealdb_table_definition<C: Connection>(
    client: &Surreal<C>,
    table: &str,
) -> Result<SurrealdbTableDefinition> {
    let response = client
        .query(surrealdb::sql::statements::InfoStatement::Tb(
            table.into(),
            false,
            None,
        ))
        .await?;

    let mut response = response.check()?;

    let result: Option<SurrealdbTableDefinition> = response.take(0)?;
    let table_definition = result.context(format!(
        "Failed to get table definition for table '{}'",
        table
    ))?;

    Ok(table_definition)
}

#[derive(Debug, Deserialize)]
struct SurrealdbInfoForTableResponse {
    events: HashMap<String, String>,
    fields: HashMap<String, String>,
    tables: HashMap<String, String>,
    indexes: HashMap<String, String>,
}

pub async fn get_surrealdb_table_indexes<C: Connection>(
    client: &Surreal<C>,
    table: &str,
) -> Result<std::collections::HashSet<String>> {
    let response = client
        .query(surrealdb::sql::statements::InfoStatement::Tb(
            table.into(),
            false,
            None,
        ))
        .await?;

    let mut response = response.check()?;

    let result: Option<SurrealdbInfoForTableResponse> = response.take(0)?;
    let table_info = result.context(format!(
        "Failed to get table info for table '{}'",
        table
    ))?;

    Ok(std::collections::HashSet::from_iter(table_info.indexes.keys().cloned()))
}

pub async fn list_script_migration_ordered_by_execution_date<C: Connection>(
    client: &Surreal<C>,
) -> Result<Vec<ScriptMigration>> {
    if get_surrealdb_table_exists(client, SCRIPT_MIGRATION_TABLE_NAME).await? {
        let mut result = list_script_migration(client).await?;
        result.sort_by_key(|m| m.executed_at.clone());
        Ok(result)
    } else {
        Ok(vec![])
    }
}

async fn list_script_migration<C: Connection>(client: &Surreal<C>) -> Result<Vec<ScriptMigration>> {
    let result = client.select(SCRIPT_MIGRATION_TABLE_NAME).await?;
    Ok(result)
}

pub fn parse_statements(query_str: &str) -> Result<surrealdb::sql::Query> {
    let query = ::surrealdb::syn::parse_with_capabilities(
        query_str,
        &::surrealdb::dbs::Capabilities::all()
            .with_experimental(::surrealdb::dbs::capabilities::Targets::All),
    )?;

    Ok(query)
}

pub fn is_define_checksum_statement(statement: &surrealdb::sql::Statement) -> bool {
    match statement {
        surrealdb::sql::Statement::Define(surrealdb::sql::statements::DefineStatement::Field(
            define_field_statement,
        )) => {
            define_field_statement.name.to_string() == "checksum"
                && define_field_statement.what.0 == SCRIPT_MIGRATION_TABLE_NAME
        }
        _ => false,
    }
}

/// Filter out DEFINE statements that are already satisfied in the database using INFO metadata.
/// This avoids executing schema operations on large tables when the schema is already in sync.
pub async fn filter_define_statements_needed<C: Connection>(
    client: &Surreal<C>,
    statements: Vec<surrealdb::sql::Statement>,
) -> Result<Vec<surrealdb::sql::Statement>> {
    // Collect referenced table names from the incoming statements
    let mut referenced_tables: std::collections::HashSet<String> = std::collections::HashSet::new();
    for s in statements.iter() {
        match s {
            surrealdb::sql::Statement::Define(surrealdb::sql::statements::DefineStatement::Table(t)) => {
                referenced_tables.insert(t.name.0.clone());
            }
            surrealdb::sql::Statement::Define(surrealdb::sql::statements::DefineStatement::Field(f)) => {
                referenced_tables.insert(f.what.to_string());
            }
            surrealdb::sql::Statement::Define(surrealdb::sql::statements::DefineStatement::Event(e)) => {
                referenced_tables.insert(e.what.to_string());
            }
            surrealdb::sql::Statement::Define(surrealdb::sql::statements::DefineStatement::Index(i)) => {
                referenced_tables.insert(i.what.to_string());
            }
            _ => {}
        }
    }

    // Fetch existing tables map to check existence quickly (INFO FOR DB)
    let existing_tables = get_surrealdb_table_definitions(client).await?;

    // Fetch field and index definitions for referenced, existing tables only (INFO FOR TABLE)
    let mut table_field_defs: std::collections::HashMap<String, SurrealdbTableDefinition> =
        std::collections::HashMap::new();
    let mut table_index_names: std::collections::HashMap<String, std::collections::HashSet<String>> =
        std::collections::HashMap::new();
    for table_name in referenced_tables.into_iter() {
        if existing_tables.contains_key(&table_name) {
            if let Ok(def) = get_surrealdb_table_definition(client, &table_name).await {
                table_field_defs.insert(table_name.clone(), def);
            }
            if let Ok(indexes) = get_surrealdb_table_indexes(client, &table_name).await {
                table_index_names.insert(table_name, indexes);
            }
        }
    }

    // Keep only statements that would change the current schema
    let mut needed: Vec<surrealdb::sql::Statement> = Vec::new();
    for s in statements.into_iter() {
        match s {
            // Create table only if table does not yet exist
            surrealdb::sql::Statement::Define(
                surrealdb::sql::statements::DefineStatement::Table(ref t),
            ) => {
                let table_name = t.name.0.clone();
                if !existing_tables.contains_key(&table_name) {
                    needed.push(s);
                }
            }
            // Create field only if the field does not yet exist in the table
            surrealdb::sql::Statement::Define(
                surrealdb::sql::statements::DefineStatement::Field(ref f),
            ) => {
                let table_name = f.what.to_string();
                let field_name = f.name.to_string();
                match table_field_defs.get(&table_name) {
                    Some(def) => {
                        if !def.fields.contains_key(&field_name) {
                            needed.push(s);
                        }
                    }
                    // If table does not exist in cache (either truly missing or INFO failed),
                    // keep the statement to ensure convergence.
                    None => needed.push(s),
                }
            }
            // Keep events as-is (INFO diffing for events not implemented here)
            surrealdb::sql::Statement::Define(
                surrealdb::sql::statements::DefineStatement::Event(_),
            ) => needed.push(s),
            // Create index only if an index with the same name does not already exist on the table
            surrealdb::sql::Statement::Define(
                surrealdb::sql::statements::DefineStatement::Index(ref i),
            ) => {
                let table_name = i.what.to_string();
                let index_name = i.name.to_string();
                match table_index_names.get(&table_name) {
                    Some(existing) => {
                        if !existing.contains(&index_name) {
                            needed.push(s);
                        }
                    }
                    None => needed.push(s),
                }
            }
            // Pass through all other statements unchanged
            _ => needed.push(s),
        }
    }

    Ok(needed)
}

pub async fn apply_in_transaction<C: Connection>(
    client: &Surreal<C>,
    statements: Vec<surrealdb::sql::Statement>,
    action: TransactionAction,
) -> Result<()> {
    let mut statements = statements.clone();

    statements.insert(
        0,
        surrealdb::sql::Statement::Begin(surrealdb::sql::statements::BeginStatement::default()),
    );

    let end_statement = match action {
        TransactionAction::Commit => {
            surrealdb::sql::Statement::Commit(surrealdb::sql::statements::CommitStatement::default())
        }
        TransactionAction::Rollback => {
            surrealdb::sql::Statement::Cancel(surrealdb::sql::statements::CancelStatement::default())
        }
    };
    statements.push(end_statement);

    let response_result = client.query(statements).await;

    match action {
        TransactionAction::Rollback => {
            let end_result = response_result.and_then(|response| response.check());

            let first_error = end_result.err().context("Error on rollback")?;
            let is_rollback_success = first_error
                .to_string()
                .contains("The query was not executed due to a cancelled transaction");

            if is_rollback_success {
                Ok(())
            } else {
                Err(eyre!(first_error))
            }
        }
        TransactionAction::Commit => {
            let mut response = response_result?;

            let errors = response.take_errors();
            if !errors.is_empty() {
                const FAILED_TRANSACTION_ERROR: &str =
                    "The query was not executed due to a failed transaction";

                let is_failed_transaction = errors
                    .values()
                    .any(|e| e.to_string() == FAILED_TRANSACTION_ERROR);

                let initial_error_messages = match is_failed_transaction {
                    true => {
                        vec![FAILED_TRANSACTION_ERROR.to_string()]
                    }
                    false => vec![],
                };

                let error_messages = errors
                    .values()
                    .map(|e| e.to_string())
                    .filter(|e| e != FAILED_TRANSACTION_ERROR)
                    .collect_vec();
                let error_messages = initial_error_messages
                    .into_iter()
                    .chain(error_messages.into_iter())
                    .collect_vec();

                return Err(eyre!(error_messages.join("\n")));
            }

            Ok(())
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy)]
pub enum TransactionAction {
    Commit,
    Rollback,
}
