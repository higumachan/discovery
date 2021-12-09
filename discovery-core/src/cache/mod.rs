use rstest::rstest;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value as JsonValue, Value};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::rc::Rc;
use thiserror::Error;

const TYPENAME: &'static str = "__typename";
const REF: &'static str = "__ref";

pub type ResultKey = String;

#[derive(Debug)]
pub struct InMemoryCache {
    result_cache: HashMap<ResultKey, NormalizedData>,
    identity_cache: HashMap<Key, NormalizedData>,
}

impl InMemoryCache {
    pub fn new() -> Self {
        InMemoryCache {
            result_cache: HashMap::new(),
            identity_cache: HashMap::new(),
        }
    }
}

pub trait Cache {
    fn identify(&self, data: &Data) -> Key;
    fn store_result_data(
        &mut self,
        key: &ResultKey,
        data: Data,
    ) -> Result<NormalizedData, CacheError>;
    fn get_result_data(&self, key: &ResultKey) -> Result<Data, CacheError>;
    fn store_identity_data(&mut self, key: &Key, data: NormalizedData) -> Result<(), CacheError>;
    fn get_identity_data(&self, key: &Key) -> Result<Data, CacheError>;
}

#[derive(Debug, Error)]
pub enum CacheError {
    #[error("result key not found")]
    ResultKeyNotFound(ResultKey),
    #[error("key not found")]
    KeyNotFound(Key),
    #[error("expect has \"{}\"", REF)]
    ExpectHasReference(JsonValue),
}

impl Cache for InMemoryCache {
    fn identify(&self, data: &Data) -> Key {
        todo!()
    }
    fn store_result_data(
        &mut self,
        key: &ResultKey,
        data: Data,
    ) -> Result<NormalizedData, CacheError> {
        let mut normalized_data_list = vec![];
        let normalized = match &data.0 {
            JsonValue::Object(obj) => NormalizedData::Object(
                obj.iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            normalize_data::<Self>(v, &mut normalized_data_list),
                        )
                    })
                    .collect(),
            ),
            JsonValue::Array(arr) => NormalizedData::Array(
                arr.iter()
                    .map(|v| normalize_data::<Self>(v, &mut normalized_data_list))
                    .collect(),
            ),
            _ => unreachable!(),
        };

        for (key, value) in normalized_data_list {
            self.store_identity_data(&key, NormalizedData::try_from(value).unwrap());
        }
        let _prev = self.result_cache.insert(key.clone(), normalized.clone());
        Ok(normalized)
    }
    fn get_result_data(&self, key: &ResultKey) -> Result<Data, CacheError> {
        let normalized_data = self
            .result_cache
            .get(key)
            .ok_or_else(|| CacheError::ResultKeyNotFound(key.clone()))?;

        let data = match normalized_data {
            NormalizedData::Object(obj) => Data(JsonValue::Object(
                obj.iter()
                    .map(|(k, v)| Ok((k.clone(), denormalize_data::<Self>(v, self)?)))
                    .collect::<Result<_, CacheError>>()?,
            )),
            NormalizedData::Array(arr) => Data(JsonValue::Array(
                arr.iter()
                    .map(|v| denormalize_data::<Self>(v, &self))
                    .collect::<Result<_, CacheError>>()?,
            )),
            _ => unreachable!(),
        };
        Ok(data)
    }
    fn store_identity_data(&mut self, key: &Key, data: NormalizedData) -> Result<(), CacheError> {
        let _prev = self.identity_cache.insert(key.clone(), data);
        Ok(())
    }
    fn get_identity_data(&self, key: &Key) -> Result<Data, CacheError> {
        let normalized_data = self
            .identity_cache
            .get(key)
            .ok_or_else(|| CacheError::KeyNotFound(key.clone()))?;

        let data = match normalized_data {
            NormalizedData::Object(obj) => Data(JsonValue::Object(
                obj.iter()
                    .map(|(k, v)| {
                        (
                            k.clone(),
                            denormalize_data::<Self>(v, self).unwrap().clone(),
                        )
                    })
                    .chain([(
                        TYPENAME.to_string(),
                        JsonValue::String(key.typename().to_string()),
                    )])
                    .collect(),
            )),
            NormalizedData::Array(arr) => Data(JsonValue::Array(
                arr.iter()
                    .map(|v| denormalize_data::<Self>(v, &self).unwrap())
                    .collect(),
            )),
            _ => unreachable!(),
        };
        Ok(data)
    }
}

type GraphQLType = String;
type Id = String;

#[derive(Debug, Clone, Eq, PartialEq, Hash, Serialize, Deserialize)]
#[serde(into = "String", try_from = "String")]
pub struct Key(GraphQLType, Id);

impl TryFrom<String> for Key {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let sp: Vec<_> = value.split(":").collect();
        if sp.len() != 2 {
            return Err("error".to_string());
        }
        Ok(Key(sp[0].to_string(), sp[1].to_string()))
    }
}

impl Into<String> for Key {
    fn into(self) -> String {
        format!("{}:{}", self.0, self.1)
    }
}

impl Key {
    pub fn field_name() -> &'static str {
        "id"
    }

    pub fn typename(&self) -> &str {
        self.0.as_str()
    }
}

trait HasKeyData {
    fn key(&self) -> Key;
}

#[derive(Error, Debug)]
pub enum DataValidationError {
    #[error("not has __typename")]
    NotHasTypenameWhenHasId(String, JsonValue),
    #[error("typename is string")]
    TypeNameIsNotString(String, JsonValue),
    #[error("InvalidJsonType")]
    InvalidJsonType(String, JsonValue),
}

#[derive(Debug, Clone, PartialEq)]
pub struct Data(JsonValue);

fn validate_data(path: &str, value: &JsonValue) -> Result<(), DataValidationError> {
    match value {
        JsonValue::Object(obj) => {
            if let Some(_) = obj.get(Key::field_name()) {
                let _ = obj
                    .get(TYPENAME)
                    .ok_or_else(|| {
                        DataValidationError::NotHasTypenameWhenHasId(
                            path.to_string(),
                            value.clone(),
                        )
                    })?
                    .as_str()
                    .ok_or_else(|| {
                        DataValidationError::TypeNameIsNotString(path.to_string(), value.clone())
                    })?
                    .to_string();
            }

            for (k, v) in obj {
                if !k.starts_with("__") {
                    validate_data(format!("{} > {}", path, k).as_str(), v)?;
                }
            }
        }
        JsonValue::Array(arr) => {
            for (i, v) in arr.iter().enumerate() {
                validate_data(format!("{} > {}", path, i).as_str(), v)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn normalize_data<C: Cache>(
    value: &JsonValue,
    normalized_data_list: &mut Vec<(Key, JsonValue)>,
) -> JsonValue {
    match value {
        JsonValue::Object(obj) => {
            let normalized_obj = obj
                .iter()
                .filter_map(|(k, v)| {
                    (!k.starts_with("__"))
                        .then(|| (k.to_string(), normalize_data::<C>(v, normalized_data_list)))
                })
                .collect::<JsonValue>();
            if let Some(id) = normalized_obj
                .get(Key::field_name())
                .map(|x| x.as_str().unwrap())
            {
                let typename = obj.get(TYPENAME).unwrap().as_str().unwrap();
                let key = Key(typename.to_string(), id.to_string());
                normalized_data_list.push((key.clone(), normalized_obj));
                json!({ REF: key })
            } else {
                value.clone()
            }
        }
        JsonValue::Array(arr) => arr
            .iter()
            .map(|v| normalize_data::<C>(v, normalized_data_list))
            .collect::<JsonValue>(),
        _ => value.clone(),
    }
}

fn denormalize_data<C: Cache>(value: &JsonValue, cache: &C) -> Result<JsonValue, CacheError> {
    match value {
        JsonValue::Object(obj) => {
            let key: Key = serde_json::from_value(
                obj.get(REF)
                    .ok_or_else(|| CacheError::ExpectHasReference(value.clone()))?
                    .clone(),
            )
            .map_err(|_| CacheError::ExpectHasReference(value.clone()))?;
            let data = cache.get_identity_data(&key)?;
            Ok(data.0)
        }
        JsonValue::Array(arr) => {
            let ar = arr
                .iter()
                .map(|x| denormalize_data(x, cache))
                .collect::<Result<Vec<JsonValue>, CacheError>>()?;
            Ok(JsonValue::Array(ar))
        }
        _ => Ok(value.clone()),
    }
}

impl Data {
    pub fn new(value: JsonValue) -> Result<Self, DataValidationError> {
        let path = "root";
        match &value {
            JsonValue::Object(obj) => {
                for (k, v) in obj {
                    if !k.starts_with("__") {
                        validate_data(format!("{} > {}", path, k).as_str(), v)?;
                    }
                }
            }
            JsonValue::Array(arr) => {
                for (i, v) in arr.iter().enumerate() {
                    validate_data(format!("{} > {}", path, i).as_str(), v)?;
                }
            }
            _ => {
                return Err(DataValidationError::InvalidJsonType(
                    path.to_string(),
                    value.clone(),
                ))
            }
        }
        Ok(Self(value))
    }

    pub fn value(&self) -> &JsonValue {
        &self.0
    }
}

#[derive(Debug, Clone)]
pub enum NormalizedData {
    Object(Map<String, JsonValue>),
    Array(Vec<JsonValue>),
}

impl TryFrom<JsonValue> for NormalizedData {
    type Error = ();

    fn try_from(value: JsonValue) -> Result<Self, Self::Error> {
        match value {
            JsonValue::Object(obj) => Ok(Self::Object(obj)),
            JsonValue::Array(arr) => Ok(Self::Array(arr)),
            _ => Err(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_data1() -> Data {
        Data::new(json!({
          "person": {
            "__typename": "Person",
            "id": "cGVvcGxlOjE=",
            "name": "Luke Skywalker",
            "homeworld": {
              "__typename": "Planet",
              "id": "cGxhbmV0czox",
              "name": "Tatooine"
            }
          }
        }))
        .unwrap()
    }

    fn test_data2() -> Data {
        Data::new(json!([
            {
            "__typename": "Person",
            "id": "cGVvcGxlOjE=",
            "name": "Luke Skywalker",
            "homeworld": {
              "__typename": "Planet",
              "id": "cGxhbmV0czox",
              "name": "Tatooine"
          }
        },
            {
            "__typename": "Person",
            "id": "aaabbb",
            "name": "Jedi",
            "homeworld": {
              "__typename": "Planet",
              "id": "cGxhbmV0czox",
              "name": "Tatooine"
          }
        },
        ]))
        .unwrap()
    }

    fn test_data3() -> Data {
        Data::new(json!({
          "person": {
            "__typename": "Person",
            "id": "cGVvcGxlOjE=",
            "name": "Luke Skywalker",
            "homeworlds": [{
              "__typename": "Planet",
              "id": "cGxhbmV0czox",
              "name": "Tatooine"
            },
                    {
              "__typename": "Planet",
              "id": "AAA",
              "name": "Tatooine2"
            }
            ]
          }
        }))
        .unwrap()
    }

    fn test_data4() -> Data {
        Data::new(json!({
                "allPeople": {
          "edges": [
            {
              "node": {
                "id": "cGVvcGxlOjE=",
                "__typename": "Person",
                "name": "Luke Skywalker"
              }
            },
            {
              "node": {
                "id": "cGVvcGxlOjI=",
                "__typename": "Person",
                "name": "C-3PO"
              }
            },
            {
              "node": {
                "id": "cGVvcGxlOjM=",
                "__typename": "Person",
                "name": "R2-D2"
              }
            },
            {
              "node": {
                "id": "cGVvcGxlOjQ=",
                "__typename": "Person",
                "name": "Darth Vader"
              }
            },
            {
              "node": {
                "id": "cGVvcGxlOjU=",
                "__typename": "Person",
                "name": "Leia Organa"
              }
            },
            {
              "node": {
                "id": "cGVvcGxlOjY=",
                "__typename": "Person",
                "name": "Owen Lars"
              }
            },
            {
              "node": {
                "id": "cGVvcGxlOjc=",
                "__typename": "Person",
                "name": "Beru Whitesun lars"
              }
            },
          ]
        }
            }))
        .unwrap()
    }

    #[rstest]
    #[case(test_data1(), 2)]
    #[case(test_data2(), 3)]
    #[case(test_data3(), 3)]
    #[case(test_data4(), 7)]
    fn normalize(#[case] data: Data, #[case] num_identity_entry: usize) {
        let mut cache = InMemoryCache::new();

        let normalized = cache.store_result_data(&"test".to_string(), data).unwrap();
        assert_eq!(cache.identity_cache.len(), num_identity_entry);
    }

    #[rstest]
    #[case(test_data1())]
    #[case(test_data2())]
    #[case(test_data3())]
    fn normalize_and_denormalize(#[case] data: Data) {
        let mut cache = InMemoryCache::new();

        let normalized = cache
            .store_result_data(&"test".to_string(), data.clone())
            .unwrap();
        let denormaliaed = cache.get_result_data(&"test".to_string()).unwrap();

        assert_eq!(data, denormaliaed);
    }

    #[rstest]
    #[case(test_data1())]
    #[case(test_data2())]
    #[case(test_data3())]
    fn normalize_and_denormalize_identity_cache_miss(#[case] data: Data) {
        let mut cache = InMemoryCache::new();

        let normalized = cache
            .store_result_data(&"test".to_string(), data.clone())
            .unwrap();

        cache.identity_cache.clear();

        let result = cache.get_result_data(&"test".to_string());

        assert!(matches!(result, Err(CacheError::KeyNotFound(_))));
    }

    #[rstest]
    #[case(test_data1())]
    #[case(test_data2())]
    #[case(test_data3())]
    fn normalize_and_denormalize_result_cache_miss(#[case] data: Data) {
        let mut cache = InMemoryCache::new();

        let normalized = cache
            .store_result_data(&"test".to_string(), data.clone())
            .unwrap();

        cache.result_cache.clear();

        let result = cache.get_result_data(&"test".to_string());

        dbg!(&result);
        assert!(matches!(result, Err(CacheError::ResultKeyNotFound(_))));
    }
}
