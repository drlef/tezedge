// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;

use storage::skip_list::{DatabaseBackedSkipList, TypedSkipList};
use storage::tests_common::TmpStorage;

use crate::common::{OrderedValue, Value};

mod common;

#[test]
fn list_new() {
    let tmp_storage = TmpStorage::create("__skip_list:list_new").expect("Storage error");
    let list: Box<dyn TypedSkipList<_, _, Value>> = Box::new(DatabaseBackedSkipList::new(1, tmp_storage.storage().kv()).expect("failed to create skip list"));
    assert_eq!(list.len(), 0);
}

#[test]
fn list_push() {
    let tmp_storage = TmpStorage::create("__skip_list:list_push").expect("Storage error");
    let mut list: Box<dyn TypedSkipList<_, _, Value>> = Box::new(DatabaseBackedSkipList::new(2, tmp_storage.storage().kv()).expect("failed to create skip list"));
    list.push(Value::new(vec![1])).expect("failed to push value to skip list");
    assert!(list.contains(0));
}

#[test]
fn list_index_level() {
    type List = DatabaseBackedSkipList;
    for index in 0..7 {
        assert_eq!(List::index_level(index), 0);
    }
    assert_eq!(List::index_level(7), 1);
    assert_eq!(List::index_level(16), 1);
    assert_eq!(List::index_level(552), 3);
}

#[test]
fn list_check_first() {
    let tmp_storage = TmpStorage::create("__skip_list:list_check_first").expect("Storage error");
    let mut list: Box<dyn TypedSkipList<_, _, Value>> = Box::new(DatabaseBackedSkipList::new(3, tmp_storage.storage().kv()).expect("failed to create skip list"));
    list.push(Value::new(vec![1])).expect("failed to push value to skip list");
    let val = list.get(0).expect("failed to get value from skip list");
    assert_eq!(val.is_some(), list.contains(0), "List `get` and `contains` return inconsistent answers");
    assert!(val.is_some());
    assert_eq!(val.unwrap(), Value::new(vec![1]));
}

#[test]
fn list_check_second() {
    let tmp_storage = TmpStorage::create("__skip_list:list_check_second").expect("Storage error");
    let mut list: Box<dyn TypedSkipList<_, _, Value>> = Box::new(DatabaseBackedSkipList::new(4, tmp_storage.storage().kv()).expect("failed to create skip list"));
    list.push(Value::new(vec![1])).expect("failed to push value to skip list");
    list.push(Value::new(vec![2])).expect("failed to push value to skip list");
    let val = list.get(1).expect("failed to get value from skip list");
    assert_eq!(val.is_some(), list.contains(1), "List `get` and `contains` return inconsistent answers");
    assert!(val.is_some());
    assert_eq!(val.unwrap(), Value::new(vec![1, 2]));
}

#[test]
fn list_check_bottom_lane() {
    let tmp_storage = TmpStorage::create("__skip_list:list_check_bottom_lane").expect("Storage error");
    let mut list: Box<dyn TypedSkipList<_, _, Value>> = Box::new(DatabaseBackedSkipList::new(5, tmp_storage.storage().kv()).expect("failed to create skip list"));
    for index in 0..=6 {
        list.push(Value::new(vec![index])).expect("failed to push value to skip list");
    }
    assert_eq!(list.levels(), 1);
    let val = list.get(6).expect("failed to get value from skip list");
    assert_eq!(val.is_some(), list.contains(6), "List `get` and `contains` return inconsistent answers");
    assert!(val.is_some());
    assert_eq!(val.unwrap(), Value::new((0..=6).collect()));
}

#[test]
pub fn list_check_faster_lane() {
    let tmp_storage = TmpStorage::create("__skip_list:list_check_faster_lane").expect("Storage error");
    let mut list: Box<dyn TypedSkipList<_, _, Value>> = Box::new(DatabaseBackedSkipList::new(6, tmp_storage.storage().kv()).expect("failed to create skip list"));
    for index in 0..=7 {
        list.push(Value::new(vec![index])).expect("failed to push value to skip list");
    }
    assert_eq!(list.levels(), 2);
    let val = list.get(7).expect("failed to get value from skip list");
    assert_eq!(val.is_some(), list.contains(7), "List `get` and `contains` return inconsistent answers");
    assert!(val.is_some());
    assert_eq!(val.unwrap(), Value::new((0..=7).collect()));
}

#[test]
pub fn list_check_lane_traversal() {
    let tmp_storage = TmpStorage::create("__skip_list:list_check_lane_traversal").expect("Storage error");
    let mut list: Box<dyn TypedSkipList<_, _, Value>> = Box::new(DatabaseBackedSkipList::new(7, tmp_storage.storage().kv()).expect("failed to create skip list"));
    for index in 0..=63 {
        list.push(Value::new(vec![index])).expect("failed to push value to skip list");
    }
    assert_eq!(list.levels(), 3);
    let val = list.get(63).expect("failed to get value from skip list");
    assert_eq!(val.is_some(), list.contains(63), "List `get` and `contains` return inconsistent answers");
    assert!(val.is_some());
    assert_eq!(val.unwrap(), Value::new((0..=63).collect()));
}

#[test]
pub fn list_check_lane_order_traversal() {
    let tmp_storage = TmpStorage::create("__skip_list:list_check_lane_order_traversal").expect("Storage error");
    let mut list: Box<dyn TypedSkipList<_, _, OrderedValue>> = Box::new(DatabaseBackedSkipList::new(8, tmp_storage.storage().kv()).expect("failed to create skip list"));
    for (value, key) in (0..=63).zip((0..=7).cycle()) {
        let mut map = HashMap::new();
        map.insert(key, value);
        list.push(OrderedValue::new(map)).expect("failed to store value into skip list");
    }
    assert_eq!(list.levels(), 3);
    let val = list.get(63).expect("failed to get value from skip list");
    assert_eq!(val.is_some(), list.contains(63), "List `get` and `contains` return inconsistent answers");
    assert!(val.is_some());
    let mut expected = HashMap::new();
    for (value, key) in (56..=63).zip(0..=7) {
        expected.insert(key, value);
    }
    assert_eq!(val.unwrap(), OrderedValue::new(expected));
}

#[test]
pub fn list_check_get_key() {
    let tmp_storage = TmpStorage::create("__skip_list:list_check_get_key").expect("Storage error");
    let mut list: Box<dyn TypedSkipList<_, _, OrderedValue>> = Box::new(DatabaseBackedSkipList::new(8, tmp_storage.storage().kv()).expect("failed to create skip list"));
    for x in 0..=7 {
        let mut map = HashMap::new();
        map.insert(x, x);
        list.push(OrderedValue::new(map)).expect("failed to store value into skip list");
    }
    assert_eq!(list.levels(), 2);
    let val = list.get_key(7, &7);
    assert_eq!(val.unwrap(), Some(7));
    let val = list.get_key(6, &7);
    assert_eq!(val.unwrap(), None);
}