use http::{
    uri::InvalidUri,
    uri::{PathAndQuery, Scheme},
    Uri,
};
use serde::{
    de::{Error, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};
use snafu::{ResultExt, Snafu};
use std::fmt;
use std::str::FromStr;

/// Used when Endpoint has no scheme.
static DEFAULT_SCHEME: Scheme = Scheme::HTTPS;

#[derive(Debug, Snafu)]
pub enum EndpointError {
    #[snafu(display("Missing authority in endpoint: {:?}.", uri))]
    MissingAuthority { uri: Uri },
    #[snafu(display("Endpoint {:?} can't have query: {:?}.", uri,uri.query()))]
    HasQuery { uri: Uri },
    #[snafu(display("Uri is not valid: {}", source))]
    Invalid { source: InvalidUri },
}

/// scheme://authority/path  //https://tools.ietf.org/html/rfc3986
///
/// Where only the authority is a must,
/// https is the default scheme,
/// and path is optional.
#[derive(Default, Debug, Clone, Eq, PartialEq)]
pub struct Endpoint(Uri);

impl Endpoint {
    pub fn new(mut uri: Uri) -> Result<Endpoint, EndpointError> {
        if uri.authority().is_none() {
            return Err(EndpointError::MissingAuthority { uri });
        }

        if uri.query().is_some() {
            return Err(EndpointError::HasQuery { uri });
        }

        if uri.path().len() > 0 && uri.path() != "/" {
            debug!(
                message = "detected endpoint with a path.",
                endpoint = %uri,
                path = %uri.path(),
            );
        }

        if uri.scheme().is_none() {
            debug!(
                message = "using default scheme for endpoint.",
                endpoint = %uri,
                sheme = %DEFAULT_SCHEME
            );
            let mut parts = uri.into_parts();
            parts.scheme = Some(DEFAULT_SCHEME.clone());
            parts
                .path_and_query
                .get_or_insert_with(|| PathAndQuery::from_static(""));
            uri = Uri::from_parts(parts).expect("Unable to construct URI");
        }

        Ok(Endpoint(uri))
    }

    pub fn from_str(uri: &str) -> Result<Self, EndpointError> {
        Self::new(Uri::from_str(uri).context(Invalid)?)
    }

    /// Panics if it's not a valid endpoint.
    pub fn from_static(endpoint: &'static str) -> Self {
        Endpoint::new(Uri::from_static(endpoint)).expect("Static endpoint is not valid")
    }

    pub fn host(&self) -> &str {
        self.0
            .authority()
            .expect("Endpoint without an authority")
            .host()
    }

    pub fn build_uri_static(&self, path_and_query: &'static str) -> Uri {
        self.build(path_and_query)
            .expect("Static path and query are not valid")
    }

    /// Ignores empty queries
    pub fn build_uri(&self, path: &str, query: &str) -> Result<Uri, InvalidUri> {
        if query.is_empty() {
            self.build(path)
        } else {
            self.build(&format!("{}?{}", path, query))
        }
    }

    fn build(&self, path_and_query: &str) -> Result<Uri, InvalidUri> {
        let mut parts = self.0.clone().into_parts();

        // Construct
        let mut s = "/".to_owned();
        if let Some(pq) = parts.path_and_query.as_ref() {
            s.push_str(pq.path().trim_start_matches('/').trim_end_matches('/'));
            s.push_str("/");
        }
        s.push_str(path_and_query.trim_start_matches('/'));

        parts.path_and_query = Some(PathAndQuery::from_maybe_shared(s)?);
        Ok(Uri::from_parts(parts).expect("Unable to construct URI"))
    }
}

impl Serialize for Endpoint {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let uri = format!("{}", self.0);
        serializer.serialize_str(&uri)
    }
}

impl<'a> Deserialize<'a> for Endpoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'a>,
    {
        deserializer.deserialize_str(UriVisitor)
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<Endpoint> for Uri {
    fn from(t: Endpoint) -> Self {
        t.0
    }
}

impl std::ops::Deref for Endpoint {
    type Target = Uri;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

struct UriVisitor;

impl<'a> Visitor<'a> for UriVisitor {
    type Value = Endpoint;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "a string containing a valid scheme://authority Uri")
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        Endpoint::new(s.parse().map_err(Error::custom)?).map_err(Error::custom)
    }
}
