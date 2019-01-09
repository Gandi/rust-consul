//! Future-aware client for consul
//!
//! This library is an client for consul that gives you stream of changes
//! done in consul

#![deny(missing_docs, missing_debug_implementations, warnings)]

extern crate hyper;
extern crate hyper_tls;
#[macro_use] extern crate log;
extern crate futures;
extern crate native_tls;
extern crate serde;
#[macro_use] extern crate serde_derive;
extern crate serde_json;
extern crate tokio_core;
extern crate url;

use std::error::{Error as StdError};
use std::fmt::{self, Write};
use std::io;
use std::mem;
use std::net::IpAddr;
use std::num::ParseIntError;
use std::str::FromStr;
use std::time::{Duration, Instant};
use std::marker::PhantomData;

use serde_json::{from_slice, Error as JsonError, Value as JsonValue};
use futures::{Stream, Future, Poll, Async};
use hyper::{Chunk, Body, StatusCode, Uri};
use hyper::client::{Client as HttpClient, FutureResponse, HttpConnector};
use hyper::header::{Header, ContentType, Headers, Raw, Formatter as HyperFormatter};
use hyper::header::parsing::from_one_raw_str;
use hyper::error::{Error as HyperError, Result as HyperResult};
use hyper_tls::HttpsConnector;
use native_tls::{Error as TlsError};
use tokio_core::reactor::{Handle, Timeout};
use url::{Url, ParseError as UrlParseError};


/// General errors that breaks the stream
#[derive(Debug)]
pub enum Error {
    /// Error given internaly by hyper
    Http(HyperError),
    /// You have polled the watcher from two different threads
    InvalidState,
    /// You have given us an invalid url
    InvalidUrl(UrlParseError),
    /// Error while initializing tls
    Tls(TlsError),
    /// uncatched io error
    Io(io::Error),
    /// consul response failed to parse
    BodyParse(ParseError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            Error::Http(ref he) => write!(f, "http error: {}", he),
            Error::InvalidState => write!(f, "invalid state reached"),
            Error::InvalidUrl(ref pe) => write!(f, "invalid url: {}", pe),
            Error::Tls(ref te) => write!(f, "{}", te),
            Error::Io(ref ie) => write!(f, "{}", ie),
            Error::BodyParse(ref be) => write!(f, "{}", be),
        }
    }
}

impl StdError for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Http(_) => "http error",
            Error::InvalidState => "invalid state reached",
            Error::InvalidUrl(_) => "invalid url",
            Error::Tls(_) => "Tls initialization problem",
            Error::Io(_) => "io problem",
            Error::BodyParse(_) => "body parse problem",
        }
    }
}

impl From<UrlParseError> for Error {
    fn from(e: UrlParseError) -> Error {
        Error::InvalidUrl(e)
    }
}

impl From<TlsError> for Error {
    fn from(e: TlsError) -> Error {
        Error::Tls(e)
    }
}

impl From<HyperError> for Error {
    fn from(e: HyperError) -> Error {
        Error::Http(e)
    }
}

impl From<io::Error> for Error {
    fn from(e: io::Error) -> Error {
        Error::Io(e)
    }
}

impl From<ParseError> for Error {
    fn from(e: ParseError) -> Error {
        Error::BodyParse(e)
    }
}

/// Errors related to blocking protocol as defined by consul
#[derive(Debug, Copy, Clone)]
pub enum ProtocolError {
    /// Consul did not reply with X-Consul-Index header
    BlockingMissing,
    /// Consul did not reply with Content-Type: application/json
    ContentTypeNotJson,
    /// Consul did not reply with 200 Ok status
    NonOkResult(StatusCode),
    /// connection refused to consul
    ConnectionRefused,
    /// we had an error, and consumer resetted the stream
    StreamRestarted,
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            ProtocolError::BlockingMissing => write!(f, "{}", self.description()),
            ProtocolError::ContentTypeNotJson => write!(f, "{}", self.description()),
            ProtocolError::NonOkResult(ref status) => write!(f, "Non ok result from consul: {}", status),
            ProtocolError::ConnectionRefused => write!(f, "connection refused to consul"),
            ProtocolError::StreamRestarted => write!(f, "consumer restarted the stream"),
        }
    }
}

impl StdError for ProtocolError {
    fn description(&self) -> &str {
        match *self {
            ProtocolError::BlockingMissing => "X-Consul-Index missing from response",
            ProtocolError::ContentTypeNotJson => "Consul replied with a non-json content",
            ProtocolError::NonOkResult(_) => "Non ok result from consul",
            ProtocolError::ConnectionRefused => "connection refused to consul",
            ProtocolError::StreamRestarted => "consumer restarted the stream",
        }
    }
}

/// Error that Watch may yield *in the stream*
#[derive(Debug)]
pub enum ParseError {
    /// Consul protocol error (missing header, unknown return format)
    Protocol(ProtocolError),
    /// Json result does not fit expected format
    UnexpectedJsonFormat,
    /// The data is not in json format
    BodyParsing(JsonError),
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        match *self {
            ParseError::Protocol(ref pe) => write!(f, "Protocol error: {}", pe),
            ParseError::UnexpectedJsonFormat => write!(f, "{}", self.description()),
            ParseError::BodyParsing(ref je) => write!(f, "Data not in json format: {}", je),
        }
    }
}

impl StdError for ParseError {
    fn description(&self) -> &str {
        match *self {
            ParseError::Protocol(_) => "Protocol error",
            ParseError::UnexpectedJsonFormat => "Unexpected json format",
            ParseError::BodyParsing(_) => "Data not in json format",
        }
    }
}

impl From<ProtocolError> for ParseError {
    fn from(e: ProtocolError) -> ParseError {
        ParseError::Protocol(e)
    }
}

#[derive(Clone, Copy, Debug)]
struct Blocking {
    index: u64,
}

impl Blocking {
    fn from<'a>(headers: &'a Headers) -> Result<Self, ()> {
        headers.get::<Self>().ok_or(()).map(|res| res.clone())
    }

    fn to_string(&self) -> String {
        let mut out = String::new();
        let _ = write!(out, "{}", self.index);
        out
    }

    fn add_to_uri(&self, uri: &Url) -> Url {
        let mut uri = uri.clone();
        uri.query_pairs_mut()
            .append_pair("index", self.to_string().as_str())

            .finish();
        uri
    }
}

impl Default for Blocking {
    fn default() -> Blocking {
        Blocking {
            index: 0,
        }
    }
}

impl Header for Blocking {
    fn header_name() -> &'static str {
        static NAME: &'static str = "X-Consul-Index";
        NAME
    }

    fn parse_header(raw: &Raw) -> HyperResult<Self> {
        from_one_raw_str(raw)
    }

    fn fmt_header(&self, f: &mut HyperFormatter) -> fmt::Result {
        f.fmt_line(self)
    }
}

impl FromStr for Blocking {
    type Err = ParseIntError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let index = s.parse::<u64>()?;
        Ok(Blocking {
            index
        })
    }
}

impl fmt::Display for Blocking {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.index)
    }
}

#[derive(Debug)]
struct BodyBuffer {
    inner: Body,
    buffer: Chunk,
}

impl BodyBuffer {
    fn new(inner: Body) -> BodyBuffer {
        BodyBuffer {
            inner,
            buffer: Chunk::default(),
        }
    }
}

impl Future for BodyBuffer {
    type Item = Chunk;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        trace!("polling BodyBuffer");
        loop {
            match self.inner.poll() {
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Ok(Async::Ready(None)) => {
                    let buffer = mem::replace(&mut self.buffer, Chunk::default());

                    return Ok(Async::Ready(buffer));
                },
                Ok(Async::Ready(Some(data))) => {
                    self.buffer.extend(data);
                    // loop, see if there is any more data here
                },
                Err(e) => return Err(Error::Http(e)),
            }
        }
    }
}

/// Consul client
#[derive(Debug, Clone)]
pub struct Client {
    http_client: HttpClient<HttpsConnector<HttpConnector>>,
    base_uri: Url,
    handle: Handle,
}

impl Client {
    /// Allocate a new consul client
    pub fn new(base_uri: &str, handle: &Handle) -> Result<Client, Error> {
        let base_uri = Url::parse(base_uri)?;

        let connector = HttpsConnector::new(4, handle)?;
        let http_client = HttpClient::configure()
            .keep_alive(true)
            .connector(connector)
            .build(handle);

        Ok(Client{
            http_client,
            base_uri,
            handle: handle.clone(),
        })
    }

    /// List services in the kernel and watch them
    pub fn services(&self) -> Watcher<Services> {
        let mut base_uri = self.base_uri.clone();
        base_uri.set_path("/v1/catalog/services");

        Watcher{
            state: WatcherState::Init{
                base_uri,
                client: self.clone(),
                error_strategy: ErrorStrategy::default(),
            },
            phantom: PhantomData::<Services>
        }
    }

    /// Watch changes of nodes on a service
    pub fn watch_service(&self, name: &str, passing: bool) -> Watcher<ServiceNodes> {
        let mut base_uri = self.base_uri.clone();
        base_uri.set_path("/v1/health/service/");
        let mut base_uri = base_uri.join(name).unwrap();
        if passing {
            base_uri.query_pairs_mut()
                .append_pair("passing", "true")
                .finish();
        }

        Watcher{
            state: WatcherState::Init{
                base_uri,
                client: self.clone(),
                error_strategy: ErrorStrategy::default(),
            },
            phantom: PhantomData::<ServiceNodes>
        }
    }

    /// Get agent informations
    pub fn agent(&self) -> FutureConsul<Agent> {
        let mut base_uri = self.base_uri.clone();
        base_uri.set_path("/v1/agent/self");

        FutureConsul{
            state: FutureState::Init{
                base_uri,
                client: self.clone(),
            },
            phantom: PhantomData::<Agent>
        }
    }
}

#[derive(Debug)]
enum ErrorHandling {
    RetryBackoff,
}

#[derive(Debug)]
struct ErrorStrategy {
    request_timeout: Duration,
    on_error: ErrorHandling,
}

impl Default for ErrorStrategy {
    fn default() -> ErrorStrategy {
        ErrorStrategy {
            request_timeout: Duration::new(5, 0),
            on_error: ErrorHandling::RetryBackoff,
        }
    }
}

#[derive(Debug)]
struct ErrorState{
    strategy: ErrorStrategy,
    current_retries: u64,
    last_try: Option<Instant>,
    last_contact: Option<Instant>,
    last_ok: Option<Instant>,
    last_error: Option<ProtocolError>,
}

impl ErrorState {
    pub fn next_timeout(&self, handle: &Handle) -> DebugTimeout {
        let retries = if self.current_retries > 10 {
            10
        } else {
            self.current_retries
        };

        debug!("Will sleep for {} seconds and retry", retries);
        let duration = Duration::new(retries, 0);

        DebugTimeout(
            // TODO: meh unwrap()
            Timeout::new(duration, handle).unwrap()
        )
    }
}

impl From<ErrorStrategy> for ErrorState {
    fn from(strategy: ErrorStrategy) -> ErrorState {
        ErrorState {
            strategy,
            current_retries: 0,
            last_try: None,
            last_contact: None,
            last_ok: None,
            last_error: None,
        }
    }
}

struct DebugTimeout(Timeout);

impl fmt::Debug for DebugTimeout {
    fn fmt(&self, f: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        write!(f, "Timeout")
    }
}

impl Future for DebugTimeout {
    type Item = ();
    type Error = io::Error;

    #[inline]
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        trace!("Timeout::poll() called");
        let res = self.0.poll();

        trace!("res {:?}", res);
        res
    }
}

fn url_to_uri(uri: &Url) -> Uri {
    let out = Uri::from_str(uri.as_str());
    if out.is_err() {
        error!("url malformed: {:?}", uri);
    }

    // TODO: meh unwrap()
    out.unwrap()
}

#[derive(Debug)]
enum WatcherState{
    Init{
        base_uri: Url,
        client: Client,
        error_strategy: ErrorStrategy,
    },
    Completed {
        base_uri: Url,
        client: Client,
        error_state: ErrorState,
        blocking: Blocking,
    },
    Error {
        base_uri: Url,
        client: Client,
        blocking: Blocking,
        error_state: ErrorState,
        retry: Option<DebugTimeout>,
    },
    PendingHeaders {
        base_uri: Url,
        client: Client,
        error_state: ErrorState,
        request: FutureResponse,
        blocking: Blocking,
    },
    PendingBody {
        base_uri: Url,
        client: Client,
        error_state: ErrorState,
        blocking: Blocking,
        headers: Headers,
        body: BodyBuffer,
    },
    Working,
}

impl Stream for WatcherState {
    type Item = Result<Chunk, ProtocolError>;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        trace!("polling WatcherState");
        loop {
            match mem::replace(self, WatcherState::Working) {
                WatcherState::Init{base_uri, client, error_strategy} => {
                    trace!("querying uri: {}", base_uri);

                    let request = client.http_client.get(url_to_uri(&base_uri));
                    trace!("{}: no response for now => PendingHeader", base_uri);
                    *self = WatcherState::PendingHeaders {
                        base_uri,
                        client,
                        request,
                        error_state: error_strategy.into(),
                        blocking: Blocking::default(),
                    };
                },
                WatcherState::Completed{base_uri, client, blocking, mut error_state} => {
                    let uri = blocking.add_to_uri(&base_uri);
                    trace!("querying uri: {}", uri);

                    error_state.last_try = Some(Instant::now());

                    let request = client.http_client.get(url_to_uri(&uri));
                    trace!("{}: no response for now => PendingHeader", base_uri);
                    *self = WatcherState::PendingHeaders {
                        base_uri,
                        client,
                        request,
                        blocking,
                        error_state,
                    };
                },
                WatcherState::PendingHeaders{base_uri, client, blocking, mut request, mut error_state} => {
                    trace!("{}: polling headers", base_uri);

                    match request.poll() {
                        Err(HyperError::Io(e)) => {
                            warn!("{}: got io error: {}", base_uri, e);
                            match e.kind() {
                                io::ErrorKind::ConnectionRefused => {
                                    let err = ProtocolError::ConnectionRefused;
                                    error_state.last_error = Some(err);
                                    error_state.current_retries += 1;
                                    *self = WatcherState::Error{
                                         base_uri,
                                         client,
                                         blocking,
                                         error_state,
                                         retry: None,
                                    };
                                    return Ok(Async::Ready(Some(Err(err))));
                                },
                                _ => {
                                    return Err(e.into());
                                }
                            }
                        },
                        Err(e) => {
                            error!("{}: got error, stopping: {}", base_uri, e);
                            return Err(e.into());
                        },
                        Ok(Async::Ready(response_headers)) => {
                            let status = response_headers.status();
                            let headers = response_headers.headers().clone();
                            let response_has_json_content_type = headers.get::<ContentType>().map(|h| h.eq(&ContentType::json())).unwrap_or(false);
                            error_state.last_contact = Some(Instant::now());

                            if status != StatusCode::Ok {
                                warn!("{}: got non-200 status: {}", base_uri, status);
                                let err = ProtocolError::NonOkResult(status);
                                error_state.last_error = Some(err);
                                error_state.current_retries += 1;
                                *self = WatcherState::Error{
                                     base_uri,
                                     client,
                                     blocking,
                                     error_state,
                                     retry: None,
                                };
                                return Ok(Async::Ready(Some(Err(err))))
                            }


                            if !response_has_json_content_type {
                                warn!("{}: got non-json content: {:?}", base_uri, headers);
                                error_state.last_error = Some(ProtocolError::ContentTypeNotJson);
                                *self = WatcherState::Error{
                                     base_uri,
                                     client,
                                     blocking,
                                     error_state,
                                     retry: None,
                                };
                            } else {

                                trace!("{}: got headers {} {:?} => PendingBody", base_uri, status, headers);
                                let body = BodyBuffer::new(response_headers.body());

                                *self = WatcherState::PendingBody {
                                    base_uri,
                                    client,
                                    blocking,
                                    headers,
                                    body,
                                    error_state,
                                };
                            };
                        },
                        Ok(Async::NotReady) => {
                            trace!("{}: still no headers => PendingHeaders", base_uri);
                            *self = WatcherState::PendingHeaders {
                                base_uri,
                                client,
                                blocking,
                                request,
                                error_state,
                            };
                            return Ok(Async::NotReady);
                        }
                    }
                },
                WatcherState::PendingBody{base_uri, client, blocking, headers, mut body, mut error_state} => {
                    trace!("{}: polling body", base_uri);

                    if let Async::Ready(body) = body.poll()? {
                        debug!("{}: got content: {:?}", base_uri, body);
                        let new_blocking = Blocking::from(&headers).map_err(|_| ProtocolError::BlockingMissing);
                        match new_blocking {
                            Err(err) => {
                                error!("{}: got error while parsing blocking headers: {:?}, {:?}", base_uri, headers, err);
                                error_state.last_error = Some(err);
                                error_state.current_retries += 1;
                                *self = WatcherState::Error{
                                     base_uri,
                                     client,
                                     error_state,
                                     blocking,

                                     // The next call to poll() will start the
                                     // timer (don't generate a timer ifclient
                                     // does not need)
                                     retry: None,
                                };
                                return Ok(Async::Ready(Some(Err(err))));
                            },
                            Ok(blocking) => {
                                info!("{}: got blocking headers: {}", base_uri, blocking);
                                error_state.last_ok = Some(Instant::now());
                                error_state.last_error = None;
                                error_state.current_retries = 0;

                                *self = WatcherState::Completed{
                                    base_uri,
                                    client,
                                    blocking,
                                    error_state
                                };

                                return Ok(Async::Ready(Some(Ok(body))));
                            }
                        }
                    } else {
                        trace!("{}: still no body => PendingBody", base_uri);

                        *self = WatcherState::PendingBody {
                            base_uri,
                            client,
                            headers,
                            blocking,
                            body,
                            error_state,
                        };
                        return Ok(Async::NotReady);
                    }
                },

                WatcherState::Error{base_uri, client, blocking, error_state, retry} => {
                    trace!("{}: still no body => PendingBody", base_uri);
                    if let Some(mut retry) = retry {
                        // We have a timeout loaded, see if it resolved
                        if let Async::Ready(_) = retry.poll()? {
                            trace!("{}: timeout completed", base_uri);
                            *self = WatcherState::Completed{
                                base_uri,
                                client,
                                blocking,
                                error_state
                            };
                        } else {
                            trace!("{}: timeout not completed", base_uri);
                            *self = WatcherState::Error{
                                base_uri,
                                client,
                                blocking,
                                error_state,
                                retry: Some(retry)
                            };
                            return Ok(Async::NotReady);
                        }
                    } else {
                        let next_timeout = error_state.next_timeout(&client.handle);
                        trace!("{}: setting timeout", base_uri);
                        *self = WatcherState::Error{
                            base_uri,
                            client,
                            blocking,
                            error_state,
                            retry: Some(next_timeout),
                        };
                        // loop will consume the poll
                    }
                }

                // Dead end
                WatcherState::Working => {
                    error!("watcher in working state, weird");
                    return Err(Error::InvalidState);
                },
            }
        }
    }
}

/// Watch changes made in consul and parse those changes
#[derive(Debug)]
pub struct Watcher<T>{
    state: WatcherState,
    phantom: PhantomData<T>,
}

impl<T> Watcher<T> {
    /// Whenever the stream yield an error. The stream closes and
    /// can't be consumed anymore. In such cases, you are required to reset
    /// the stream. It will then, sleep (according to the error strategy)
    /// and reconnect to consul.
    pub fn reset(&mut self) {
        let (base_uri, client, blocking, mut error_state) = match mem::replace(&mut self.state, WatcherState::Working) {
            WatcherState::Init{base_uri, client, error_strategy, ..} =>
                (base_uri, client, Blocking::default(), ErrorState::from(error_strategy)),
            WatcherState::Completed{base_uri, client, blocking, error_state, ..} |
            WatcherState::Error{base_uri, client, blocking, error_state, ..} |
            WatcherState::PendingHeaders{base_uri, client, blocking, error_state, ..} |
            WatcherState::PendingBody{base_uri, client, blocking, error_state, ..} =>
                (base_uri, client, blocking, error_state),
            WatcherState::Working => panic!("stream resetted while polled. State is invalid"),
        };
        error_state.last_error = Some(ProtocolError::StreamRestarted);
        self.state = WatcherState::Error{
             base_uri,
             client,
             blocking,
             error_state,
             retry: None,
        };
    }
}

impl<T> Stream for Watcher<T>
    where T: ConsulType {
    type Item = Result<T::Reply, ParseError>;
    type Error = Error;

    #[inline]
    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // poll()? pattern will bubble up the error
        match self.state.poll()? {
            Async::NotReady => Ok(Async::NotReady),
            Async::Ready(None) => Ok(Async::Ready(None)),
            Async::Ready(Some(Err(e))) => Ok(Async::Ready(Some(Err(e.into())))),
            Async::Ready(Some(Ok(body))) => {
                Ok(Async::Ready(Some(T::parse(&body))))
            }
        }
    }
}


/// Trait for parsing types out of consul
pub trait ConsulType {
    /// The kind of replies this parser yields
    type Reply;

    /// Parse an http body and give back a result
    fn parse(buf: &Chunk) -> Result<Self::Reply, ParseError>;
}

fn read_map_service_name(value: &JsonValue) -> Result<Vec<ServiceName>, ParseError> {
    if let &JsonValue::Object(ref map) = value {
        let mut out = Vec::with_capacity(map.len());
        for (k, v) in map.iter() {
            if let &JsonValue::Array(ref _values) = v {
                if k != "consul" {
                    out.push(k.clone());
                }
            } else {
                return Err(ParseError::UnexpectedJsonFormat)
            }
        }
        Ok(out)
    } else {
        Err(ParseError::UnexpectedJsonFormat)
    }
}

/// Services name used in consul
pub type ServiceName = String;

/// Parse services list in consul
#[derive(Debug)]
pub struct Services {}
impl ConsulType for Services {
    type Reply = Vec<ServiceName>;

    fn parse(buf: &Chunk) -> Result<Self::Reply, ParseError> {
         let v: JsonValue = from_slice(&buf).map_err(ParseError::BodyParsing)?;
         let res = read_map_service_name(&v)?;

         Ok(res)
    }
}

/// Parse node list from services in consul
#[derive(Debug)]
pub struct ServiceNodes {}
impl ConsulType for ServiceNodes {
    type Reply = Vec<Node>;

    fn parse(buf: &Chunk) -> Result<Self::Reply, ParseError> {
         let v: Vec<TempNode> = from_slice(&buf).map_err(ParseError::BodyParsing)?;

         Ok(v.into_iter().map(|x| x.node).collect())
    }
}

#[derive(Deserialize)]
struct TempNode {
    #[serde(rename = "Node")]
    node: Node,
}

/// Node hosting services
#[derive(Debug, Deserialize, PartialEq, Clone)]
pub struct Node {
    /// Node name
    #[serde(rename = "Node")]
    pub name: String,

    /// Node address
    #[serde(rename = "Address")]
    pub address: IpAddr,
}

/// A future response from consul
#[derive(Debug)]
pub struct FutureConsul<T> {
    state: FutureState,
    phantom: PhantomData<T>,
}

impl<T> Future for FutureConsul<T>
    where T: ConsulType {
    type Item = T::Reply;
    type Error = Error;

    #[inline]
    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        // poll()? pattern will bubble up the error
        match self.state.poll()? {
            Async::NotReady => Ok(Async::NotReady),
            Async::Ready(body) => {
                T::parse(&body).map(|res| {
                   Async::Ready(res)
                }).map_err(|e| Error::BodyParse(e))
            },
        }
    }
}

#[derive(Debug)]
enum FutureState {
    Init {
        base_uri: Url,
        client: Client,
    },
    PendingHeaders {
        base_uri: Url,
        client: Client,
        request: FutureResponse,
    },
    PendingBody {
        base_uri: Url,
        client: Client,
        headers: Headers,
        body: BodyBuffer,
    },
    Done,
    Working,
}

impl Future for FutureState {
    type Item = Chunk;
    type Error = Error;

    fn poll(&mut self) -> Poll<Self::Item, Self::Error> {
        trace!("polling FutureState");
        loop {
            match mem::replace(self, FutureState::Working) {
                FutureState::Init{base_uri, client} => {
                    trace!("querying uri: {}", base_uri);

                    let request = client.http_client.get(url_to_uri(&base_uri));
                    trace!("no response for now => PendingHeader");
                    *self = FutureState::PendingHeaders {
                        base_uri,
                        client,
                        request,
                    };
                },
                FutureState::PendingHeaders{base_uri, client, mut request} => {
                    trace!("polling headers");

                    match request.poll()? {
                        Async::Ready(response_headers) => {
                            let status = response_headers.status();
                            let headers = response_headers.headers().clone();
                            let response_has_json_content_type = headers.get::<ContentType>().map(|h| h.eq(&ContentType::json())).unwrap_or(false);
                            if status != StatusCode::Ok {
                                let err = ProtocolError::NonOkResult(status);
                                return Err(Error::BodyParse(ParseError::Protocol(err)))
                            } else if !response_has_json_content_type {
                                let err = ProtocolError::ContentTypeNotJson;
                                return Err(Error::BodyParse(ParseError::Protocol(err)))
                            } else {
                                trace!("got headers {} {:?} => PendingBody", status, headers);
                                let body = BodyBuffer::new(response_headers.body());
                                *self = FutureState::PendingBody {
                                    base_uri,
                                    client,
                                    headers,
                                    body,
                                };
                            }
                        },
                        Async::NotReady => {
                            trace!("still no headers => PendingHeaders");
                            *self = FutureState::PendingHeaders {
                                base_uri,
                                client,
                                request,
                            };
                            return Ok(Async::NotReady);
                        },
                    }
                },
                FutureState::PendingBody{base_uri, client, headers, mut body} => {
                    trace!("polling body");

                    if let Async::Ready(body) = body.poll()? {
                        *self = FutureState::Done;
                        return Ok(Async::Ready(body));
                    } else {
                        *self = FutureState::PendingBody{
                            base_uri,
                            client,
                            headers,
                            body
                        };
                        return Ok(Async::NotReady);
                    }
                }

                // Dead end
                FutureState::Working | FutureState::Done => {
                    return Err(Error::InvalidState);
                },
            }
        }
    }
}

#[derive(Deserialize)]
struct InnerAgent {
    #[serde(rename = "Member")]
    member: InnerMember,
}

#[derive(Deserialize)]
struct InnerMember {
    #[serde(rename = "Addr")]
    addr: IpAddr,
}

/// Parse node list from services in consul
#[derive(Debug)]
pub struct Agent {
    /// public ip address used by this address
    pub member_address: IpAddr,
}

impl ConsulType for Agent {
    type Reply = Agent;

    fn parse(buf: &Chunk) -> Result<Self::Reply, ParseError> {
        let agent: InnerAgent = serde_json::from_slice(&buf).map_err(ParseError::BodyParsing)?;
        Ok(Agent {
            member_address: agent.member.addr,
        })
    }
}

