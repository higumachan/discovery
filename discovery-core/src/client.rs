use graphql_client::{GraphQLQuery, QueryBody, Response};
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Client;
use reqwest::Response as ReqwestResponse;
use serde::Serialize;
use serde_json::Value;
use sha1::Digest;
use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::rc::Rc;
use thiserror::Error;

use crate::cache::{Cache, Data, DataValidationError};

pub struct CacheWrap<C>(Rc<RefCell<C>>);

impl<C> CacheWrap<C> {
    fn inner(&self) -> Rc<RefCell<C>> {
        self.0.clone()
    }
}

pub struct DiscoveryClientBuilder<C> {
    uri: Option<String>,
    authorization: Option<String>,
    cache: Option<CacheWrap<C>>,
}

#[derive(Error, Debug)]
pub enum BuilderError {
    #[error("uri not found")]
    URINotFound,
    #[error("invalid header")]
    InvalidHeader(#[from] reqwest::header::InvalidHeaderValue),
    #[error("reqwest error")]
    ReqwestError(#[from] reqwest::Error),
}

impl<C: Cache> DiscoveryClientBuilder<C> {
    pub fn new() -> Self {
        Self {
            cache: None,
            uri: None,
            authorization: None,
        }
    }

    pub fn uri(mut self, uri: String) -> Self {
        self.uri = Some(uri);
        self
    }

    pub fn authorization(mut self, authorization: String) -> Self {
        self.authorization = Some(authorization);
        self
    }

    pub fn cache(mut self, cache: CacheWrap<C>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub fn build(self) -> std::result::Result<DiscoveryClient<C>, BuilderError> {
        let mut headers = HeaderMap::new();

        if let Some(auth) = self.authorization {
            headers.insert("Authorization", HeaderValue::from_str(&auth)?);
        }

        let reqwest_client = reqwest::Client::builder()
            .default_headers(headers)
            .build()?;

        Ok(DiscoveryClient {
            uri: self.uri.ok_or(BuilderError::URINotFound)?,
            reqwest_client,
            cache: self.cache,
        })
    }
}

pub struct DiscoveryClient<C> {
    uri: String,
    cache: Option<CacheWrap<C>>,
    reqwest_client: Client,
}

#[derive(Error, Debug)]
enum ClientError {
    #[error("reqwest error")]
    ReqwestError(#[from] reqwest::Error),
    #[error("deserialize error")]
    DeserializeError(#[from] serde_json::Error),
    #[error("data validation error")]
    DataValidationError(#[from] DataValidationError),
}

fn request_body_hash<Q: GraphQLQuery>(qb: &QueryBody<<Q as GraphQLQuery>::Variables>) -> String {
    let b = bincode::serialize(qb).expect("can not serialize");
    let d = sha1::Sha1::digest(b);
    base64::encode(d)
}

type ClientResult<T> = std::result::Result<T, ClientError>;

impl<C: Cache> DiscoveryClient<C> {
    pub async fn query<Q: GraphQLQuery>(
        &self,
        variable: <Q as GraphQLQuery>::Variables,
    ) -> ClientResult<Response<<Q as GraphQLQuery>::ResponseData>> {
        let request_body = Q::build_query(variable);

        let body_hash = request_body_hash::<Q>(&request_body);

        let cached = self
            .cache
            .as_ref()
            .and_then(|c| c.inner().borrow_mut().get_result_data(&body_hash).ok());

        Ok(if let Some(data) = cached {
            let response = serde_json::from_value(data.value().clone())?;
            response
        } else {
            let data = Data::new(self.send::<Q>(request_body).await?)?;
            self.cache.as_ref().and_then(|c| {
                c.inner()
                    .borrow_mut()
                    .store_result_data(&body_hash, data.clone())
                    .ok()
            });
            let response = serde_json::from_value(data.value().clone())?;
            response
        })
    }

    async fn send<Q: GraphQLQuery>(
        &self,
        query_body: QueryBody<<Q as GraphQLQuery>::Variables>,
    ) -> ClientResult<Value> {
        let res = self
            .reqwest_client
            .post(self.uri.as_str())
            .json(&query_body)
            .send()
            .await?;

        let response_body: Value = res.json().await?;

        Ok(response_body)
    }
}
