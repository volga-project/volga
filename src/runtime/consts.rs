use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::Duration;

pub const MASTER_DISCOVERY_TIMEOUT: &str = "master.discovery_timeout";
pub const MASTER_REPLACEMENT_TIMEOUT: &str = "master.replacement_timeout";
pub const MASTER_RECOVERY_BUDGET: &str = "master.recovery_budget";
pub const MASTER_RPC_MAX_RETRIES: &str = "master.rpc_max_retries";
pub const MASTER_RPC_RETRY_DELAY: &str = "master.rpc_retry_delay";

#[derive(Clone, Debug)]
pub enum RuntimeValue {
    Duration(Duration),
    U64(u64),
}

#[derive(Clone, Debug)]
pub struct RuntimeConsts {
    values: HashMap<&'static str, RuntimeValue>,
}

impl RuntimeConsts {
    pub fn production() -> Self {
        Self::from([
            (MASTER_DISCOVERY_TIMEOUT, RuntimeValue::Duration(Duration::from_secs(30))),
            (MASTER_REPLACEMENT_TIMEOUT, RuntimeValue::Duration(Duration::from_secs(120))),
            (MASTER_RECOVERY_BUDGET, RuntimeValue::U64(20)),
            (MASTER_RPC_MAX_RETRIES, RuntimeValue::U64(5)),
            (MASTER_RPC_RETRY_DELAY, RuntimeValue::Duration(Duration::from_secs(1))),
        ])
    }

    pub fn test() -> Self {
        Self::from([
            (MASTER_DISCOVERY_TIMEOUT, RuntimeValue::Duration(Duration::from_secs(2))),
            (MASTER_REPLACEMENT_TIMEOUT, RuntimeValue::Duration(Duration::from_secs(2))),
            (MASTER_RECOVERY_BUDGET, RuntimeValue::U64(5)),
            (MASTER_RPC_MAX_RETRIES, RuntimeValue::U64(2)),
            (MASTER_RPC_RETRY_DELAY, RuntimeValue::Duration(Duration::from_millis(50))),
        ])
    }

    pub fn duration(&self, key: &'static str) -> Duration {
        match self.values.get(key) {
            Some(RuntimeValue::Duration(value)) => *value,
            _ => panic!("runtime constant {key} is missing or not a duration"),
        }
    }

    pub fn u64(&self, key: &'static str) -> u64 {
        match self.values.get(key) {
            Some(RuntimeValue::U64(value)) => *value,
            _ => panic!("runtime constant {key} is missing or not an integer"),
        }
    }
}

impl<const N: usize> From<[(&'static str, RuntimeValue); N]> for RuntimeConsts {
    fn from(entries: [(&'static str, RuntimeValue); N]) -> Self {
        Self { values: entries.into_iter().collect() }
    }
}

static RUNTIME_CONSTS: OnceLock<RuntimeConsts> = OnceLock::new();

pub fn runtime_consts() -> &'static RuntimeConsts {
    RUNTIME_CONSTS.get_or_init(|| {
        if cfg!(test) { RuntimeConsts::test() } else { RuntimeConsts::production() }
    })
}
