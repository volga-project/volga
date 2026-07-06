use std::fs;
use std::path::PathBuf;

use anyhow::Context;
use schemars::schema_for;
use volga::api::KubePipelineSpec;

fn main() -> anyhow::Result<()> {
    let schema = schema_for!(KubePipelineSpec);
    let schema_json = serde_json::to_string_pretty(&schema)
        .context("failed to encode KubePipelineSpec JSON schema")?;

    let output = PathBuf::from("kubevolga/internal/controller/kube_pipeline_spec.schema.json");
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create schema dir {}", parent.display()))?;
    }
    fs::write(&output, schema_json)
        .with_context(|| format!("failed to write schema to {}", output.display()))?;
    println!("wrote {}", output.display());
    Ok(())
}
