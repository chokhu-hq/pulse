use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;

use serde::Deserialize;

use crate::config::ConfigError;

const DEFAULT_MAX_MESSAGE_BYTES: NonZeroUsize = match NonZeroUsize::new(8192) {
    Some(n) => n,
    None => NonZeroUsize::MIN,
};
const DEFAULT_MAX_BATCH_EVENTS: NonZeroUsize = match NonZeroUsize::new(100) {
    Some(n) => n,
    None => NonZeroUsize::MIN,
};
const DEFAULT_CLIENT_EVENT_RATE: NonZeroU32 = match NonZeroU32::new(10) {
    Some(n) => n,
    None => NonZeroU32::MIN,
};
const DEFAULT_CLIENT_EVENT_BURST: NonZeroU32 = match NonZeroU32::new(20) {
    Some(n) => n,
    None => NonZeroU32::MIN,
};

pub(crate) struct Secret(String);

impl Secret {
    pub(crate) const fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Secret(..)")
    }
}

#[derive(Debug)]
pub(crate) enum Origins {
    Any,
    Only(Vec<String>),
}

impl Origins {
    fn new(allowed: Vec<String>) -> Self {
        if allowed.is_empty() || allowed.iter().any(|origin| origin == "*") {
            return Self::Any;
        }
        Self::Only(allowed)
    }

    // requests without an origin header come from non-browser clients
    pub(crate) fn allows(&self, origin: Option<&str>) -> bool {
        match (self, origin) {
            (Self::Any, _) | (Self::Only(_), None) => true,
            (Self::Only(allowed), Some(origin)) => allowed.iter().any(|o| o == origin),
        }
    }
}

#[derive(Debug)]
pub(crate) struct App {
    pub(crate) id: String,
    pub(crate) key: String,
    pub(crate) secret: Secret,
    pub(crate) name: String,
    pub(crate) max_connections: Option<NonZeroUsize>,
    pub(crate) max_channels_per_connection: Option<NonZeroUsize>,
    pub(crate) origins: Origins,
    pub(crate) max_message_bytes: NonZeroUsize,
    pub(crate) max_batch_events: NonZeroUsize,
    pub(crate) allows_client_events: bool,
    pub(crate) client_event_rate: NonZeroU32,
    pub(crate) client_event_burst: NonZeroU32,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub(crate) struct AppFile {
    id: String,
    key: String,
    secret: String,
    #[serde(default)]
    name: String,
    #[serde(default)]
    enabled: bool,
    #[serde(default)]
    max_connections: usize,
    #[serde(default)]
    max_channels_per_connection: usize,
    #[serde(default)]
    allowed_origins: Vec<String>,
    #[serde(default)]
    max_message_size: usize,
    #[serde(default)]
    max_batch_events: usize,
    #[serde(default)]
    enable_client_events: bool,
    #[serde(default)]
    max_event_rate: u32,
    #[serde(default)]
    max_event_burst: u32,
}

impl TryFrom<AppFile> for App {
    type Error = ConfigError;

    fn try_from(file: AppFile) -> Result<Self, Self::Error> {
        for (field, value) in [
            ("id", &file.id),
            ("key", &file.key),
            ("secret", &file.secret),
        ] {
            if value.is_empty() {
                return Err(ConfigError::MissingAppField { field });
            }
        }

        Ok(Self {
            id: file.id,
            key: file.key,
            secret: Secret(file.secret),
            name: file.name,
            max_connections: NonZeroUsize::new(file.max_connections),
            max_channels_per_connection: NonZeroUsize::new(file.max_channels_per_connection),
            origins: Origins::new(file.allowed_origins),
            max_message_bytes: NonZeroUsize::new(file.max_message_size)
                .unwrap_or(DEFAULT_MAX_MESSAGE_BYTES),
            max_batch_events: NonZeroUsize::new(file.max_batch_events)
                .unwrap_or(DEFAULT_MAX_BATCH_EVENTS),
            allows_client_events: file.enable_client_events,
            client_event_rate: NonZeroU32::new(file.max_event_rate)
                .unwrap_or(DEFAULT_CLIENT_EVENT_RATE),
            client_event_burst: NonZeroU32::new(file.max_event_burst)
                .unwrap_or(DEFAULT_CLIENT_EVENT_BURST),
        })
    }
}

#[derive(Debug, Default)]
pub(crate) struct Apps {
    by_key: HashMap<String, Arc<App>>,
    by_id: HashMap<String, Arc<App>>,
}

impl Apps {
    pub(crate) fn from_files(files: Vec<AppFile>) -> Result<Self, ConfigError> {
        let mut apps = Self::default();
        for file in files.into_iter().filter(|file| file.enabled) {
            apps.insert(Arc::new(App::try_from(file)?))?;
        }

        Ok(apps)
    }

    fn insert(&mut self, app: Arc<App>) -> Result<(), ConfigError> {
        let Entry::Vacant(by_id) = self.by_id.entry(app.id.clone()) else {
            return Err(ConfigError::DuplicateApp {
                field: "id",
                value: app.id.clone(),
            });
        };
        let Entry::Vacant(by_key) = self.by_key.entry(app.key.clone()) else {
            return Err(ConfigError::DuplicateApp {
                field: "key",
                value: app.key.clone(),
            });
        };

        by_id.insert(Arc::clone(&app));
        by_key.insert(app);
        Ok(())
    }

    pub(crate) fn by_key(&self, key: &str) -> Option<&Arc<App>> {
        self.by_key.get(key)
    }

    pub(crate) fn by_id(&self, id: &str) -> Option<&Arc<App>> {
        self.by_id.get(id)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &Arc<App>> {
        self.by_key.values()
    }

    pub(crate) fn len(&self) -> usize {
        self.by_key.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn app_file(json: &str) -> AppFile {
        serde_json::from_str(json).expect("valid app json")
    }

    #[test]
    fn applies_defaults_for_zero_values() {
        let app = App::try_from(app_file(
            r#"{"id":"a","key":"k","secret":"s","enabled":true}"#,
        ))
        .unwrap();

        assert_eq!(
            app.max_message_bytes.get(),
            8192,
            "default max message size"
        );
        assert_eq!(app.max_batch_events.get(), 100, "default max batch events");
        assert_eq!(app.client_event_rate.get(), 10, "default client event rate");
        assert_eq!(
            app.client_event_burst.get(),
            20,
            "default client event burst"
        );
        assert!(
            app.max_connections.is_none(),
            "zero max connections means unlimited"
        );
        assert!(
            !app.allows_client_events,
            "client events are off unless enabled"
        );
    }

    #[test]
    fn origins() {
        let cases = [
            (vec![], Some("https://a.test"), true),
            (vec!["*"], Some("https://a.test"), true),
            (vec!["https://a.test"], Some("https://a.test"), true),
            (vec!["https://a.test"], Some("https://b.test"), false),
            (vec!["https://a.test"], None, true),
        ];

        for (allowed, origin, expected) in cases {
            let origins = Origins::new(allowed.iter().map(|o| (*o).to_owned()).collect());
            assert_eq!(
                origins.allows(origin),
                expected,
                "allowed {allowed:?} origin {origin:?}"
            );
        }
    }

    #[test]
    fn rejects_duplicate_keys() {
        let files = vec![
            app_file(r#"{"id":"a","key":"k","secret":"s","enabled":true}"#),
            app_file(r#"{"id":"b","key":"k","secret":"s","enabled":true}"#),
        ];

        let err = Apps::from_files(files).unwrap_err();
        assert!(
            matches!(err, ConfigError::DuplicateApp { field: "key", .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn skips_disabled_apps() {
        let files = vec![app_file(
            r#"{"id":"a","key":"k","secret":"s","enabled":false}"#,
        )];

        assert_eq!(
            Apps::from_files(files).unwrap().len(),
            0,
            "disabled apps are not loaded"
        );
    }
}
