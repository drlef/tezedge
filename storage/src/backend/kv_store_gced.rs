use std::collections::HashSet;

use std::mem;
use std::ops::{Deref, DerefMut};
use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread;
use std::time::Duration;

use crate::merkle_storage::{ContextValue, Entry, EntryHash};
use crate::storage_backend::{
    StorageBackend as KVStore, StorageBackendError as KVStoreError,
    StorageBackendStats as KVStoreStats,
};

// TODO: add assertions for EntryHash to make sure it is stack allocated.

/// Finds the value with hash `key` in one of the cycle stores (trying from newest to oldest)
fn stores_get<T, S>(stores: &S, key: &EntryHash) -> Option<ContextValue>
where
    T: KVStore,
    S: Deref<Target = Vec<T>>,
{
    stores
        .iter()
        .rev()
        .find_map(|store| store.get(key).unwrap_or(None))
}

/// Returns store index containing the entry with hash `key`  (trying from newest to oldest)
fn stores_containing<T, S>(stores: &S, key: &EntryHash) -> Option<usize>
where
    T: KVStore,
    S: Deref<Target = Vec<T>>,
{
    stores
        .iter()
        .enumerate()
        .find(|(_, store)| store.contains(key).unwrap_or(false))
        .map(|(index, _)| index)
    // .map(|(rev_offset, _)| stores.len() - rev_offset - 1)
}

/// Returns `true` if any of the stores contains an entry with hash `key`, and `false` otherwise
fn stores_contains<T, S>(stores: &S, key: &EntryHash) -> bool
where
    T: KVStore,
    S: Deref<Target = Vec<T>>,
{
    stores
        .iter()
        .rev()
        .find(|store| store.contains(key).unwrap_or(false))
        .is_some()
}

/// Searches the stores for an entry with hash `key`, and if found, the value is deleted.
///
/// The return value is `None` if the value was not found, or Some((store_idx, value)) otherwise.
fn stores_delete<T, S>(stores: &mut S, key: &EntryHash) -> Option<(usize, ContextValue)>
where
    T: KVStore,
    S: DerefMut<Target = Vec<T>>,
{
    stores
        .iter_mut()
        .enumerate()
        .rev()
        .find_map(|(index, store)| {
            store
                .delete(key)
                .unwrap_or(None)
                .map(|value| (index, value))
        })
}

/// Commands used by KVStoreGCed to interact with GC thread.
pub enum CmdMsg {
    StartNewCycle,
    Exit,
    MarkReused(EntryHash),
}

/// Garbage Collected Key Value Store
pub struct KVStoreGCed<T: KVStore> {
    /// Amount of cycles we keep
    cycle_count: usize,
    /// Stores for each cycle, older to newer
    stores: Arc<RwLock<Vec<T>>>,
    /// Stats for each store in archived stores
    stores_stats: Arc<Mutex<Vec<KVStoreStats>>>,
    /// Current in-process cycle store
    current: T,
    /// Current in-process cycle store stats
    current_stats: KVStoreStats,
    /// GC thread
    thread: thread::JoinHandle<()>,
    // TODO: Mutex because it's required to be Sync. Better way?
    /// Channel to communicate with GC thread from main thread
    msg: Mutex<mpsc::Sender<CmdMsg>>,
}

impl<T: 'static + KVStore + Default> KVStoreGCed<T> {
    pub fn new(cycle_count: usize) -> Self {
        // size < 2 wouldn't make sense because there would be nowhere to move things
        assert!(
            cycle_count > 1,
            "cycle_count less than 2 for KVStoreGCed not supported"
        );

        let (tx, rx) = mpsc::channel();
        let stores = Arc::new(RwLock::new(
            (0..(cycle_count - 1)).map(|_| Default::default()).collect(),
        ));
        let stores_stats = Arc::new(Mutex::new(vec![Default::default(); cycle_count - 1]));

        Self {
            cycle_count,
            stores: stores.clone(),
            stores_stats: stores_stats.clone(),
            thread: thread::spawn(move || kvstore_gc_thread_fn(stores, stores_stats, rx)),
            msg: Mutex::new(tx),
            current: Default::default(),
            current_stats: Default::default(),
        }
    }

    /// Finds an entry with hash `key` in one of the archived cycle stores (trying from newest to oldest)
    fn stores_get(&self, key: &EntryHash) -> Option<ContextValue> {
        stores_get(&self.stores.read().unwrap(), key)
    }

    /// Returns `true` if any of the archived cycle stores contains a value with hash `key`, and `false` otherwise
    fn stores_contains(&self, key: &EntryHash) -> bool {
        stores_contains(&self.stores.read().unwrap(), key)
    }
}

impl<T: 'static + KVStore + Default> KVStore for KVStoreGCed<T> {
    fn is_persisted(&self) -> bool {
        self.current.is_persisted()
    }

    /// Get an entry with hash `key` from the current in-progress cycle store,
    /// otherwise find and get from archived cycle stores
    fn get(&self, key: &EntryHash) -> Result<Option<ContextValue>, KVStoreError> {
        Ok(self.current.get(key)?.or_else(|| self.stores_get(key)))
    }

    /// Checks if an entry with hash `key` exists in any of the cycle stores
    fn contains(&self, key: &EntryHash) -> Result<bool, KVStoreError> {
        Ok(self.current.contains(key)? || self.stores_contains(key))
    }

    fn put(&mut self, key: EntryHash, value: ContextValue) -> Result<bool, KVStoreError> {
        let measurement = KVStoreStats::from((&key, &value));
        let was_added = self.current.put(key, value)?;

        if was_added {
            self.current_stats += measurement;
        }

        Ok(was_added)
    }

    fn merge(&mut self, key: EntryHash, value: ContextValue) -> Result<(), KVStoreError> {
        self.current.merge(key, value)
    }

    fn delete(&mut self, key: &EntryHash) -> Result<Option<ContextValue>, KVStoreError> {
        self.current.delete(key)
    }

    /// Marks an entry as "reused" in the current cycle.
    ///
    /// Entries that are reused/referenced in current cycle
    /// will be preserved after garbage collection.
    fn mark_reused(&mut self, key: EntryHash) {
        let _ = self.msg.lock().unwrap().send(CmdMsg::MarkReused(key));
    }

    /// Not needed/implemented.
    // TODO: Maybe this method should go into separate trait?
    fn retain(&mut self, _pred: HashSet<EntryHash>) -> Result<(), KVStoreError> {
        unimplemented!()
    }

    /// Starts a new cycle.
    ///
    /// Garbage collector will start collecting the oldest cycle.
    fn start_new_cycle(&mut self, _last_commit_hash: Option<EntryHash>) {
        self.stores_stats
            .lock()
            .unwrap()
            .push(mem::take(&mut self.current_stats));
        self.stores
            .write()
            .unwrap()
            .push(mem::take(&mut self.current));
        let _ = self.msg.lock().unwrap().send(CmdMsg::StartNewCycle);
    }

    /// Waits for garbage collector to finish collecting the oldest cycle.
    fn wait_for_gc_finish(&self) {
        //TODO: give some time GC thread to startup - in the future we
        //should get rid of that one and use condition variable
        //but his method is only used for testing anyway
        thread::sleep(Duration::from_millis(300));
        while self.stores.read().unwrap().len() >= self.cycle_count {
            thread::sleep(Duration::from_millis(2));
        }
    }

    fn get_stats(&self) -> Vec<KVStoreStats> {
        self.stores_stats
            .lock()
            .unwrap()
            .iter()
            .chain(vec![&self.current_stats])
            .cloned()
            .collect()
    }
}

/// Garbage collector main function
fn kvstore_gc_thread_fn<T: KVStore>(
    stores: Arc<RwLock<Vec<T>>>,
    stores_stats: Arc<Mutex<Vec<KVStoreStats>>>,
    rx: mpsc::Receiver<CmdMsg>,
) {
    // number of preserved archived cycles
    let len = stores.read().unwrap().len();
    // maintains incoming references (hashes) for each archived cycle store.
    let mut reused_keys = vec![HashSet::new(); len];
    // this is filled when garbage collection starts and it contains keys
    // that are reused from the oldest store and needs to be moved to newer
    // stores so that after destroying oldest store they are preserved.
    let mut todo_keys: Vec<EntryHash> = vec![];
    let mut received_exit_msg = false;

    loop {
        // wait (block) for main thread events if there are no items to garbage collect
        let wait_for_events = reused_keys.len() == len && !received_exit_msg;

        let msg = if wait_for_events {
            match rx.recv() {
                Ok(value) => Some(value),
                Err(_) => {
                    // println!("MerkleStorage GC thread shut down! reason: mpsc::Sender dropped.");
                    return;
                }
            }
        } else {
            match rx.try_recv() {
                Ok(value) => Some(value),
                Err(_) => None,
            }
        };

        // process messages received from the main thread
        match msg {
            Some(CmdMsg::StartNewCycle) => {
                // new cycle started, we add a new reused/referenced keys HashSet for it
                reused_keys.push(Default::default());
            }
            Some(CmdMsg::Exit) => {
                received_exit_msg = true;
            }
            Some(CmdMsg::MarkReused(key)) => {
                if let Some(index) = stores_containing(&stores.read().unwrap(), &key) {
                    // only way index can be greater than reused_keys.len() is if GC thread
                    // lags behind (gc has pending 1-2 cycles to collect). When we still haven't
                    // received event from main thread that new cycle has started, yet it has.
                    // So we might receive `key` that was only in `current` store (when this event
                    // was sent by main thread). So if gc had't lagged behind, we wouldn't have found
                    // entry with that `key`. So this entry shouldn't be marked as reused.
                    if index < reused_keys.len() {
                        let keys = &mut reused_keys[index];
                        if keys.insert(key) {
                            stores_stats.lock().unwrap()[index].update_reused_keys(keys);
                        }
                    }
                }
            }
            None => {
                // we exit only if there are no remaining keys to be processed
                if received_exit_msg && todo_keys.len() == 0 && reused_keys.len() == len {
                    // println!("MerkleStorage GC thread shut down! reason: received exit message.");
                    return;
                }
            }
        }

        // if reused_keys.len() > len  that means that we need to start garbage collecting oldest cycle,
        // reused_keys[0].len() > 0  If no keys are reused we can just drop the cycle.
        if reused_keys.len() > len && reused_keys[0].len() > 0 {
            todo_keys.extend(mem::take(&mut reused_keys[0]).into_iter());
        }

        if todo_keys.len() > 0 {
            let mut stats = stores_stats.lock().unwrap();
            let mut stores = stores.write().unwrap();

            // it will block if new cycle begins and we still haven't garbage collected prev cycle.
            // Higher the max_iter (2048) will be, longer gc will block the main thread, but GC will
            // finish faster and will lag behind less.
            // Smaller the max_iter (2048) will be, longer it will take for gc to finish and it will
            // lag behind more (which will increase memory usage), but it will slow down main thread less
            // as the lock will be released on more frequent intervals.
            // TODO: it would be more optimized if this number can change dynamically based on
            // how much gc lags behind and such different parameters.
            let max_iter = if stores.len() - len <= 1 {
                2048
            } else {
                usize::MAX
            };

            for _ in 0..max_iter {
                let key = match todo_keys.pop() {
                    Some(key) => key,
                    None => break,
                };

                let (store_index, entry_bytes) = match stores_delete(&mut stores, &key) {
                    Some(result) => result,
                    // it's possible entry was deleted already when iterating on some other root during gc.
                    // So it's perfectly normal if referenced entry isn't found.
                    None => continue,
                };

                let stat: KVStoreStats = (&key, &entry_bytes).into();
                stats[store_index] -= &stat;

                let entry: Entry = match bincode::deserialize(&entry_bytes) {
                    Ok(value) => value,
                    Err(err) => {
                        eprintln!(
                            "MerkleStorage GC: error while decerializing entry: {:?}",
                            err
                        );
                        continue;
                    }
                };

                // move the entry to the latest store

                // TODO: it would be better if we would move entries to the newest store
                // they were referenced in. This way entries won't live longer then they
                // have to (unlike how its now). This can be achieved if we keep cycle
                // information with `reused_keys`. So if it is Map instead of Set and
                // and we store maximum cycle in which it was referenced in as a value.
                // Then we can move each entry to a given store based on that value.
                if let Err(err) = stores.last_mut().unwrap().put(key.clone(), entry_bytes) {
                    eprintln!(
                        "MerkleStorage GC: error while adding entry to store: {:?}",
                        err
                    );
                } else {
                    // and update the stats for that store
                    *stats.last_mut().unwrap() += &stat;
                }

                match entry {
                    Entry::Blob(_) => {}
                    Entry::Tree(tree) => {
                        let children = tree.into_iter().map(|(_, node)| node.entry_hash);
                        todo_keys.extend(children);
                    }
                    Entry::Commit(commit) => {
                        todo_keys.push(commit.root_hash);
                    }
                }
            }
        }

        if reused_keys.len() > len && reused_keys[0].len() == 0 && todo_keys.len() == 0 {
            drop(reused_keys.drain(..1));
            drop(stores_stats.lock().unwrap().drain(..1));
            drop(stores.write().unwrap().drain(..1));
        }
    }

    // println!("MerkleStorage GC thread shut down!");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::BTreeMapBackend;
    use crate::storage_backend::size_of_vec;
    use std::convert::TryFrom;

    fn empty_kvstore_gced(cycle_count: usize) -> KVStoreGCed<BTreeMapBackend> {
        KVStoreGCed::new(cycle_count)
    }

    fn entry_hash(key: &[u8]) -> EntryHash {
        assert!(key.len() < 32);
        let bytes: Vec<u8> = key
            .iter()
            .chain(std::iter::repeat(&0u8))
            .take(32)
            .map(|elem| *elem)
            .collect();

        EntryHash::try_from(bytes).unwrap()
    }

    fn blob(value: Vec<u8>) -> Entry {
        Entry::Blob(value)
    }

    fn blob_serialized(value: Vec<u8>) -> Vec<u8> {
        bincode::serialize(&blob(value)).unwrap()
    }

    fn get<T: 'static + KVStore + Default>(store: &KVStoreGCed<T>, key: &[u8]) -> Option<Entry> {
        store
            .get(&entry_hash(key))
            .unwrap()
            .map(|x| bincode::deserialize(&x[..]).unwrap())
    }

    fn put<T: 'static + KVStore + Default>(store: &mut KVStoreGCed<T>, key: &[u8], value: Entry) {
        store
            .put(entry_hash(key), bincode::serialize(&value).unwrap())
            .unwrap();
    }

    fn mark_reused<T: 'static + KVStore + Default>(store: &mut KVStoreGCed<T>, key: &[u8]) {
        store.mark_reused(entry_hash(key));
    }

    #[test]
    // TODO: fix data race in GC thread
    #[ignore]
    fn test_key_reused_exists() {
        let store = &mut empty_kvstore_gced(3);

        put(store, &[1], blob(vec![1]));
        put(store, &[2], blob(vec![2]));
        store.start_new_cycle(None);
        put(store, &[3], blob(vec![3]));
        store.start_new_cycle(None);
        put(store, &[4], blob(vec![4]));
        mark_reused(store, &[1]);
        store.start_new_cycle(None);

        store.wait_for_gc_finish();

        assert_eq!(get(store, &[1]), Some(blob(vec![1])));
        assert_eq!(get(store, &[2]), None);
        assert_eq!(get(store, &[3]), Some(blob(vec![3])));
        assert_eq!(get(store, &[4]), Some(blob(vec![4])));
    }

    #[test]
    // TODO: fix data race in GC thread
    #[ignore]
    fn test_stats() {
        let store = &mut empty_kvstore_gced(3);

        let kv1 = (entry_hash(&[1]), blob_serialized(vec![1]));
        let kv2 = (entry_hash(&[2]), blob_serialized(vec![1, 2]));
        let kv3 = (entry_hash(&[3]), blob_serialized(vec![1, 2, 3]));
        let kv4 = (entry_hash(&[4]), blob_serialized(vec![1, 2, 3, 4]));

        store.put(kv1.0.clone(), kv1.1.clone()).unwrap();
        store.put(kv2.0.clone(), kv2.1.clone()).unwrap();
        store.start_new_cycle(None);
        store.put(kv3.0.clone(), kv3.1.clone()).unwrap();
        store.start_new_cycle(None);
        store.put(kv4.0.clone(), kv4.1.clone()).unwrap();
        store.mark_reused(kv1.0.clone());

        store.wait_for_gc_finish();

        let stats: Vec<_> = store.get_stats().into_iter().rev().take(3).rev().collect();
        assert_eq!(stats[0].key_bytes, 64);
        assert_eq!(
            stats[0].value_bytes,
            size_of_vec(&kv1.1) + size_of_vec(&kv2.1)
        );
        assert_eq!(stats[0].reused_keys_bytes, 96);

        assert_eq!(stats[1].key_bytes, 32);
        assert_eq!(stats[1].value_bytes, size_of_vec(&kv3.1));
        assert_eq!(stats[1].reused_keys_bytes, 0);

        assert_eq!(stats[2].key_bytes, 32);
        assert_eq!(stats[2].value_bytes, size_of_vec(&kv4.1));
        assert_eq!(stats[2].reused_keys_bytes, 0);

        assert_eq!(
            store.total_mem_usage_as_bytes(),
            vec![
                // TODO: bring back when MerklHash aka EntryHash will be allocated
                // on stack
                // self.reused_keys_bytes = list.capacity() * mem::size_of::<EntryHash>();
                // 4 * mem::size_of::<EntryHash>(),
                4 * 32,
                96, // reused keys
                size_of_vec(&kv1.1),
                size_of_vec(&kv2.1),
                size_of_vec(&kv3.1),
                size_of_vec(&kv4.1),
            ]
            .iter()
            .sum::<usize>()
        );

        store.start_new_cycle(None);
        store.wait_for_gc_finish();

        let stats = store.get_stats();
        assert_eq!(stats[0].key_bytes, 32);
        assert_eq!(stats[0].value_bytes, size_of_vec(&kv3.1));
        assert_eq!(stats[0].reused_keys_bytes, 0);

        assert_eq!(stats[1].key_bytes, 64);
        assert_eq!(
            stats[1].value_bytes,
            size_of_vec(&kv1.1) + size_of_vec(&kv4.1)
        );
        assert_eq!(stats[1].reused_keys_bytes, 0);

        assert_eq!(stats[2].key_bytes, 0);
        assert_eq!(stats[2].value_bytes, 0);
        assert_eq!(stats[2].reused_keys_bytes, 0);

        assert_eq!(
            store.total_mem_usage_as_bytes(),
            vec![
                // TODO: bring back when MerklHash aka EntryHash will be allocated
                // on stack
                // self.reused_keys_bytes = list.capacity() * mem::size_of::<EntryHash>();
                // 3 * mem::size_of::<EntryHash>(),
                3 * 32,
                size_of_vec(&kv1.1),
                size_of_vec(&kv3.1),
                size_of_vec(&kv4.1),
            ]
            .iter()
            .sum::<usize>()
        );
    }
}
