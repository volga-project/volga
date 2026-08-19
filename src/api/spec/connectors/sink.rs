use serde::{Deserialize, Serialize};

use crate::runtime::functions::sink::ParquetSinkSpec;
use crate::runtime::operators::sink::sink_operator::SinkConfig;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SinkSpec {
    InMemoryStorageGrpc {
        /// Operator creates `{pipeline}-storage`. Mutually exclusive with `server_addr` on the CR.
        #[serde(default)]
        create: bool,
        #[serde(default)]
        server_addr: Option<String>,
        /// When non-empty, explode rows and upsert into the keyed map by these columns.
        #[serde(default)]
        upsert_key_columns: Vec<String>,
    },
    Request,
    Parquet(ParquetSinkSpec),
    /// Drop-payload sink: count records, do not retain batches.
    Count,
}

impl SinkSpec {
    pub fn in_memory_grpc(server_addr: impl Into<String>) -> Self {
        Self::InMemoryStorageGrpc {
            create: false,
            server_addr: Some(server_addr.into()),
            upsert_key_columns: Vec::new(),
        }
    }

    /// In-memory sink with upsert keys; address is filled in by get_spec / the harness.
    pub fn in_memory_upsert(key_columns: Vec<String>) -> Self {
        Self::InMemoryStorageGrpc {
            create: true,
            server_addr: None,
            upsert_key_columns: key_columns,
        }
    }

    pub fn with_upsert_key_columns(mut self, columns: Vec<String>) -> Self {
        if let Self::InMemoryStorageGrpc {
            upsert_key_columns, ..
        } = &mut self
        {
            *upsert_key_columns = columns;
        }
        self
    }

    pub fn with_server_addr(mut self, server_addr: impl Into<String>) -> Self {
        if let Self::InMemoryStorageGrpc {
            server_addr: addr, ..
        } = &mut self
        {
            *addr = Some(server_addr.into());
        }
        self
    }

    pub fn fill_created_store_addr(&mut self, addr: impl Into<String>) {
        if let Self::InMemoryStorageGrpc {
            create: true,
            server_addr,
            ..
        } = self
        {
            if !has_in_memory_addr(server_addr.as_deref()) {
                *server_addr = Some(addr.into());
            }
        }
    }

    pub fn to_sink_config(&self) -> SinkConfig {
        match self {
            SinkSpec::InMemoryStorageGrpc {
                server_addr,
                upsert_key_columns,
                ..
            } => {
                let addr = server_addr
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .unwrap_or_else(|| {
                        panic!("InMemoryStorageGrpc server_addr must be set before to_sink_config")
                    });
                SinkConfig::in_memory_grpc(addr.to_string())
                    .with_upsert_key_columns(upsert_key_columns.clone())
            }
            SinkSpec::Request => SinkConfig::RequestSinkConfig,
            SinkSpec::Parquet(spec) => SinkConfig::ParquetSinkConfig(spec.to_config()),
            SinkSpec::Count => SinkConfig::CountSinkConfig,
        }
    }

    /// True when this sink needs the in-memory gRPC store process/pod.
    pub fn needs_in_memory_store(&self) -> bool {
        matches!(self, Self::InMemoryStorageGrpc { .. })
    }

    pub fn validate(&self) -> Result<(), String> {
        if let Self::InMemoryStorageGrpc {
            create,
            server_addr,
            ..
        } = self
        {
            let has_addr = has_in_memory_addr(server_addr.as_deref());
            match (*create, has_addr) {
                (true, false) => Ok(()),
                (false, true) => Ok(()),
                (true, true) => Err(
                    "InMemoryStorageGrpc create and server_addr are mutually exclusive".to_string(),
                ),
                (false, false) => Err(
                    "InMemoryStorageGrpc requires create: true or server_addr".to_string(),
                ),
            }
        } else {
            Ok(())
        }
    }
}

fn has_in_memory_addr(server_addr: Option<&str>) -> bool {
    server_addr.map(str::trim).is_some_and(|s| !s.is_empty())
}

pub fn created_in_memory_store_http_addr(namespace: &str, pipeline_name: &str) -> String {
    format!("http://{pipeline_name}-storage.{namespace}.svc.cluster.local:50071")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn in_memory_requires_create_or_addr() {
        let missing = SinkSpec::InMemoryStorageGrpc {
            create: false,
            server_addr: None,
            upsert_key_columns: Vec::new(),
        };
        assert!(missing.validate().is_err());
        assert!(SinkSpec::in_memory_grpc("http://store:50071").validate().is_ok());
        assert!(SinkSpec::in_memory_upsert(vec!["k".into()]).validate().is_ok());

        let both = SinkSpec::InMemoryStorageGrpc {
            create: true,
            server_addr: Some("http://store:50071".into()),
            upsert_key_columns: Vec::new(),
        };
        assert!(both.validate().is_err());
    }

    #[test]
    fn fill_created_store_addr_only_when_create() {
        let mut created = SinkSpec::in_memory_upsert(vec![]);
        created.fill_created_store_addr("http://p-storage.ns.svc.cluster.local:50071");
        match created {
            SinkSpec::InMemoryStorageGrpc {
                create,
                server_addr,
                ..
            } => {
                assert!(create);
                assert_eq!(
                    server_addr.as_deref(),
                    Some("http://p-storage.ns.svc.cluster.local:50071")
                );
            }
            other => panic!("{other:?}"),
        }

        let mut external = SinkSpec::in_memory_grpc("http://external:50071");
        external.fill_created_store_addr("http://ignored");
        match external {
            SinkSpec::InMemoryStorageGrpc { server_addr, .. } => {
                assert_eq!(server_addr.as_deref(), Some("http://external:50071"));
            }
            other => panic!("{other:?}"),
        }
    }
}
