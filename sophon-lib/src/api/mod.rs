use std::borrow::Cow;

use bytes::Bytes;
use prost::Message;
use reqwest::blocking::Client;
use serde::de::DeserializeOwned;

use crate::{GameEdition, SophonError, api::schemas::ApiResponse};

pub mod schemas;

mod api_wrapper_macros;
pub(crate) use api_wrapper_macros::*;

/// Convenience struct for keeping the common arguments
#[derive(Debug, Clone)]
pub struct ApiContext {
    pub client: Client,
    pub edition: GameEdition,
}

/// A wrapper for chaining api calls that require a previous api call result
#[derive(Debug, Clone)]
pub struct ApiContextWrapper<'ctx, T> {
    pub ctx: &'ctx ApiContext,
    pub value: T,
}

pub type ApiResult<'ctx, T> = Result<ApiContextWrapper<'ctx, T>, SophonError>;

impl<T: Copy> Copy for ApiContextWrapper<'_, T> {}

impl ApiContext {
    pub fn wrap<T>(&self, value: T) -> ApiContextWrapper<'_, T> {
        ApiContextWrapper { ctx: self, value }
    }

    #[allow(unused)]
    pub(crate) fn api_get_request<T: DeserializeOwned>(
        &self,
        url: impl AsRef<str>,
    ) -> Result<T, SophonError> {
        let response = self.client.get(url.as_ref()).send()?.error_for_status()?;

        Ok(response.json::<ApiResponse<T>>()?.data)
    }

    #[allow(unused)]
    pub(crate) fn api_post_request<T: DeserializeOwned>(
        &self,
        url: impl AsRef<str>,
    ) -> Result<T, SophonError> {
        let response = self.client.post(url.as_ref()).send()?.error_for_status()?;

        Ok(response.json::<ApiResponse<T>>()?.data)
    }

    #[allow(unused)]
    pub(crate) fn api_get_request_raw(&self, url: impl AsRef<str>) -> Result<String, SophonError> {
        let response = self.client.get(url.as_ref()).send()?.error_for_status()?;

        Ok(response.text()?)
    }

    #[allow(unused)]
    pub(crate) fn api_post_request_raw(&self, url: impl AsRef<str>) -> Result<String, SophonError> {
        let response = self.client.post(url.as_ref()).send()?.error_for_status()?;

        Ok(response.text()?)
    }
}

impl<'ctx, T> ApiContextWrapper<'ctx, T> {
    pub fn map<'a, T2: 'a, F: FnOnce(&'a T) -> T2>(&'a self, f: F) -> ApiContextWrapper<'ctx, T2> {
        ApiContextWrapper {
            ctx: self.ctx,
            value: f(&self.value),
        }
    }

    pub fn map_iter<'a, I: Iterator + 'a, F: FnOnce(&'a T) -> I>(
        &'a self,
        f: F,
    ) -> ApiContextWrapperIter<'ctx, I> {
        ApiContextWrapperIter { iter: self.map(f) }
    }

    pub fn as_ref(&self) -> ApiContextWrapper<'ctx, &T> {
        ApiContextWrapper {
            ctx: self.ctx,
            value: &self.value,
        }
    }
}

impl<'ctx, T: Clone> ApiContextWrapper<'ctx, &T> {
    pub fn cloned(&self) -> ApiContextWrapper<'ctx, T> {
        ApiContextWrapper {
            ctx: self.ctx,
            value: self.value.clone(),
        }
    }
}

impl<'ctx, T> From<ApiContextWrapper<'ctx, Option<T>>> for Option<ApiContextWrapper<'ctx, T>> {
    fn from(value: ApiContextWrapper<'ctx, Option<T>>) -> Self {
        value.value.map(|v| ApiContextWrapper {
            ctx: value.ctx,
            value: v,
        })
    }
}

impl<'ctx, T> From<ApiContextWrapper<'ctx, Result<T, SophonError>>> for ApiResult<'ctx, T> {
    fn from(value: ApiContextWrapper<'ctx, Result<T, SophonError>>) -> Self {
        value.value.map(|v| ApiContextWrapper {
            ctx: value.ctx,
            value: v,
        })
    }
}

#[derive(Debug, Clone)]
pub struct ApiContextWrapperIter<'ctx, I> {
    iter: ApiContextWrapper<'ctx, I>,
}

impl<'ctx, I: Iterator> Iterator for ApiContextWrapperIter<'ctx, I> {
    type Item = ApiContextWrapper<'ctx, <I as Iterator>::Item>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.ctx.wrap(self.iter.value.next()).into()
    }
}

// Protobuf helpers

pub fn get_protobuf_from_url<T>(
    client: &Client,
    url: impl AsRef<str>,
    compression: bool,
) -> Result<T, SophonError>
where
    T: Message,
    T: Default,
{
    let response = client.get(url.as_ref()).send()?.error_for_status()?;

    let compressed_manifest = response.bytes()?;

    let protobuf_bytes = if compression {
        zstd::decode_all(&*compressed_manifest).unwrap()
    } else {
        compressed_manifest.into()
    };

    let parsed_manifest = T::decode(protobuf_bytes.as_slice()).unwrap();

    Ok(parsed_manifest)
}

pub fn get_protobuf_from_url_raw(
    client: &Client,
    url: impl AsRef<str>,
    compression: bool,
) -> Result<Bytes, SophonError> {
    let response = client.get(url.as_ref()).send()?.error_for_status()?;

    let compressed_manifest = response.bytes()?;

    let protobuf_bytes = if compression {
        zstd::decode_all(&*compressed_manifest).unwrap().into()
    } else {
        compressed_manifest
    };

    Ok(protobuf_bytes)
}

/// Try to decode protobuf from a binary blob. Tries to decompress the data using zstd, and if
/// that fails, assumes the data is uncompressed
pub fn decode_protobuf<T>(data: &[u8]) -> Result<T, SophonError>
where
    T: Message,
    T: Default,
{
    let mut data_ref = data;
    let decomperssed_data = zstd::decode_all(&mut data_ref)
        .inspect_err(|err| {
            tracing::warn!(
                ?err,
                "Failed to decompress provided data stream, assuming the data is uncompressed"
            )
        })
        .map(Cow::Owned)
        .unwrap_or(Cow::Borrowed(data));

    T::decode(decomperssed_data.as_ref())
        .map_err(|err| SophonError::IoError(std::io::Error::other(err)))
}
