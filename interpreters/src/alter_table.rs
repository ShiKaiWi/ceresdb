// Copyright 2023 The CeresDB Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Interpreter for insert statement

use async_trait::async_trait;
use common_types::{
    column_schema::{self, ColumnSchema},
    schema::{self, Schema},
};
use macros::define_result;
use query_frontend::plan::{AlterTableOperation, AlterTablePlan};
use snafu::{ensure, ResultExt, Snafu};
use table_engine::table::AlterSchemaRequest;

use crate::interpreter::{self, AlterTable, Interpreter, InterpreterPtr, Output};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(display("Failed to add column to schema, err:{}", source))]
    AddColumnSchema { source: common_types::schema::Error },

    #[snafu(display("Failed to build schema, err:{}", source))]
    BuildSchema { source: common_types::schema::Error },

    #[snafu(display("Failed to alter table schema, err:{}", source))]
    AlterSchema { source: table_engine::table::Error },

    #[snafu(display("Failed to alter table options, err:{}", source))]
    AlterOptions { source: table_engine::table::Error },

    #[snafu(display("Not allow to add a not null column, name:{}", name))]
    AddNotNull { name: String },
}

define_result!(Error);

pub struct AlterTableInterpreter {
    plan: AlterTablePlan,
}

impl AlterTableInterpreter {
    pub fn create(plan: AlterTablePlan) -> InterpreterPtr {
        Box::new(Self { plan })
    }
}

#[async_trait]
impl Interpreter for AlterTableInterpreter {
    async fn execute(self: Box<Self>) -> interpreter::Result<Output> {
        self.execute_alter().await.context(AlterTable)
    }
}

impl AlterTableInterpreter {
    async fn execute_alter(self: Box<Self>) -> Result<Output> {
        let AlterTablePlan { table, operations } = self.plan;

        match operations {
            AlterTableOperation::AddColumn(columns) => {
                let current_schema = table.schema();
                let new_schema = build_new_schema(&current_schema, columns)?;

                let request = AlterSchemaRequest {
                    schema: new_schema,
                    pre_schema_version: current_schema.version(),
                };

                let num_rows = table.alter_schema(request).await.context(AlterSchema)?;

                Ok(Output::AffectedRows(num_rows))
            }
            AlterTableOperation::ModifySetting(options) => {
                let num_rows = table.alter_options(options).await.context(AlterOptions)?;
                Ok(Output::AffectedRows(num_rows))
            }
        }
    }
}

fn build_new_schema(current_schema: &Schema, column_schemas: Vec<ColumnSchema>) -> Result<Schema> {
    let current_version = current_schema.version();

    let mut builder =
        schema::Builder::with_capacity(current_schema.num_columns() + column_schemas.len())
            .primary_key_indexes(current_schema.primary_key_indexes().to_vec())
            // Increment the schema version.
            .version(current_version + 1);
    for (idx, column) in current_schema.columns().iter().enumerate() {
        if current_schema.is_primary_key_index(&idx) {
            builder = builder
                .add_key_column(column.clone())
                .context(AddColumnSchema)?;
        } else {
            builder = builder
                .add_normal_column(column.clone())
                .context(AddColumnSchema)?;
        }
    }

    builder = builder
        // Enable column id generation of the schema builder.
        .auto_increment_column_id(true);

    // Add new columns
    for mut column_schema in column_schemas {
        // Uninit the id of the column schema.
        column_schema.id = column_schema::COLUMN_ID_UNINIT;

        validate_add_column(&column_schema)?;

        // Only allow to add normal column.
        builder = builder
            .add_normal_column(column_schema)
            .context(AddColumnSchema)?;
    }

    // Build the final schema.
    let new_schema = builder.build().context(BuildSchema)?;

    Ok(new_schema)
}

fn validate_add_column(column_schema: &ColumnSchema) -> Result<()> {
    ensure!(
        column_schema.is_nullable,
        AddNotNull {
            name: &column_schema.name
        }
    );

    Ok(())
}
