//! Factory for wiring the data client into a Nautilus node.

use std::{any::Any, cell::RefCell, rc::Rc};

use nautilus_common::{
    cache::CacheView,
    clients::DataClient,
    clock::Clock,
    factories::{ClientConfig, DataClientFactory},
};
use nautilus_model::identifiers::ClientId;

use crate::{common::ASTER, config::AsterDataClientConfig, data::AsterDataClient};

impl ClientConfig for AsterDataClientConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Creates [`AsterDataClient`] instances for a node.
#[derive(Clone, Copy, Debug, Default)]
pub struct AsterDataClientFactory;

impl DataClientFactory for AsterDataClientFactory {
    fn create(
        &self,
        name: &str,
        config: &dyn ClientConfig,
        _cache: CacheView,
        _clock: Rc<RefCell<dyn Clock>>,
    ) -> anyhow::Result<Box<dyn DataClient>> {
        let config = config
            .as_any()
            .downcast_ref::<AsterDataClientConfig>()
            .ok_or_else(|| anyhow::anyhow!("expected AsterDataClientConfig, was {config:?}"))?
            .clone();
        Ok(Box::new(AsterDataClient::new(
            ClientId::from(name),
            config,
        )?))
    }

    fn name(&self) -> &'static str {
        ASTER
    }

    fn config_type(&self) -> &'static str {
        "AsterDataClientConfig"
    }
}
