// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! We need to fetch different data from p2p or send data to protocol validation
//! Main purpose of this module is to synchronize this request/responses per peers and handle queues management for peer
//!
//! We dont handle unique requests accross different peers, but if we want to, we just need to add here some synchronization.
//! Now we just handle unique requests per peer.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use riker::actors::*;
use slog::{warn, Logger};

use crypto::hash::BlockHash;
use networking::p2p::peer::SendMessage;
use networking::PeerId;
use storage::OperationsMetaStorage;
use tezos_messages::p2p::encoding::prelude::{
    GetBlockHeadersMessage, GetOperationsForBlocksMessage, OperationsForBlock, PeerMessageResponse,
};

use crate::chain_feeder::ChainFeederRef;
use crate::state::peer_state::{DataQueues, MissingOperations, PeerState};
use crate::state::StateError;

/// Shareable ref between threads
pub type DataRequesterRef = Arc<DataRequester>;

/// Requester manages global request/response queues for data
/// and also manages local queues for every peer.
pub struct DataRequester {
    operations_meta_storage: OperationsMetaStorage,

    /// Chain feeder - actor, which is responsible to apply_block to context
    block_applier: ChainFeederRef,
}

impl DataRequester {
    pub fn new(
        operations_meta_storage: OperationsMetaStorage,
        block_applier: ChainFeederRef,
    ) -> Self {
        Self {
            operations_meta_storage,
            block_applier,
        }
    }

    /// Tries to schedule blocks downloading from peer
    ///
    /// Returns true if was scheduled and p2p message was sent
    pub fn fetch_block_headers(
        &self,
        blocks_to_download: Vec<Arc<BlockHash>>,
        peer: &PeerId,
        peer_queues: &DataQueues,
        log: &Logger,
    ) -> Result<bool, StateError> {
        // check if empty
        if blocks_to_download.is_empty() {
            return Ok(false);
        }

        // get available capacity
        let available_capacity = peer_queues.available_queued_block_headers_capacity()?;

        // get queue locks
        let mut peer_queued_block_headers = peer_queues.queued_block_headers.lock()?;

        // fillter non-queued
        let mut blocks_to_download = blocks_to_download
            .into_iter()
            .filter(|block_hash| !peer_queued_block_headers.contains(block_hash.as_ref()))
            .collect::<Vec<Arc<BlockHash>>>();

        // trim to max capacity
        let blocks_to_download = if available_capacity < blocks_to_download.len() {
            blocks_to_download.drain(0..available_capacity).collect()
        } else {
            blocks_to_download
        };

        // if empty finish
        if blocks_to_download.is_empty() {
            Ok(false)
        } else {
            // add to queue
            let _ = peer_queued_block_headers.extend(blocks_to_download.clone());
            // release lock
            drop(peer_queued_block_headers);

            // send p2p msg - now we can fire msg to peer
            tell_peer(
                GetBlockHeadersMessage::new(
                    blocks_to_download
                        .iter()
                        .map(|b| b.as_ref().clone())
                        .collect::<Vec<BlockHash>>(),
                )
                .into(),
                peer,
            );

            // peer request stats
            match peer_queues.block_request_last.write() {
                Ok(mut request_last) => *request_last = Instant::now(),
                Err(e) => {
                    warn!(log, "Failed to update block_request_last from peer"; "reason" => format!("{}", e))
                }
            }

            Ok(true)
        }
    }

    /// Tries to schedule blocks operations downloading from peer
    ///
    /// Returns true if was scheduled and p2p message was sent
    pub fn fetch_block_operations(
        &self,
        blocks_to_download: Vec<Arc<BlockHash>>,
        peer: &PeerId,
        peer_queues: &DataQueues,
        log: &Logger,
    ) -> Result<bool, StateError> {
        // check if empty
        if blocks_to_download.is_empty() {
            return Ok(false);
        }

        // get available capacity
        let available_capacity = peer_queues.available_queued_block_operations_capacity()?;

        // get queue locks
        let mut peer_queued_block_headers = peer_queues.queued_block_operations.lock()?;

        // fillter non-queued
        let mut blocks_to_download = blocks_to_download
            .into_iter()
            .filter(|block_hash| !peer_queued_block_headers.contains_key(block_hash.as_ref()))
            .collect::<Vec<Arc<BlockHash>>>();

        // trim to max capacity
        let blocks_to_download = if available_capacity < blocks_to_download.len() {
            blocks_to_download.drain(0..available_capacity).collect()
        } else {
            blocks_to_download
        };

        // collect missing validation_passes
        let blocks_to_download: Vec<(Arc<BlockHash>, MissingOperations)> = blocks_to_download
            .into_iter()
            .filter_map(|b| {
                if let Ok(Some(metadata)) = self.operations_meta_storage.get(&b) {
                    if let Some(missing_operations) = metadata.get_missing_validation_passes() {
                        if !missing_operations.is_empty() {
                            Some((b, missing_operations.iter().map(|vp| *vp as i8).collect()))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    None
                }
            })
            .collect();

        // if empty finish
        if blocks_to_download.is_empty() {
            Ok(false)
        } else {
            // add to queue
            let _ = peer_queued_block_headers.extend(blocks_to_download.clone());
            // release lock
            drop(peer_queued_block_headers);

            // send p2p msg - now we can fire msg to peer
            tell_peer(
                GetOperationsForBlocksMessage::new(
                    blocks_to_download
                        .into_iter()
                        .flat_map(|(block, missing_ops)| {
                            missing_ops
                                .into_iter()
                                .map(move |vp| OperationsForBlock::new(block.as_ref().clone(), vp))
                        })
                        .collect(),
                )
                .into(),
                peer,
            );

            // peer request stats
            match peer_queues.block_operations_request_last.write() {
                Ok(mut request_last) => *request_last = Instant::now(),
                Err(e) => {
                    warn!(log, "Failed to update block_operations_request_last from peer"; "reason" => format!("{}", e))
                }
            }

            Ok(true)
        }
    }

    /// Handle received block.
    ///
    /// Returns:
    ///     None - if was not scheduled for peer => unexpected block
    ///     Some(lock) - if was scheduled, lock is released, when comes out of the scope
    pub fn block_header_received(
        &self,
        block_hash: &BlockHash,
        peer: &mut PeerState,
        log: &Logger,
    ) -> Result<Option<RequestedBlockDataLock>, StateError> {
        // if was not scheduled, just return None
        if !peer
            .queues
            .queued_block_headers
            .lock()?
            .contains(block_hash)
        {
            warn!(log, "Received unexpected block header from peer"; "block_header_hash" => block_hash.to_base58_check());
            peer.message_stats.increment_unexpected_response_block();
            return Ok(None);
        }

        // peer response stats
        match peer.queues.block_response_last.write() {
            Ok(mut response_last) => *response_last = Instant::now(),
            Err(e) => {
                warn!(log, "Failed to update block_response_last from peer"; "reason" => format!("{}", e))
            }
        }

        // if contains, return data lock, when this lock will go out if the scope, then drop will be triggered, and queues will be emptied
        Ok(Some(RequestedBlockDataLock {
            block_hash: Arc::new(block_hash.clone()),
            queued_block_headers: peer.queues.queued_block_headers.clone(),
        }))
    }

    /// Handle received block operations which we requested.
    ///
    /// Returns:
    ///     None - if was not scheduled for peer => unexpected block/operations
    ///     Some(lock) - if was scheduled, lock is released, when comes out of the scope
    pub fn block_operations_received(
        &self,
        operations_for_block: &OperationsForBlock,
        peer: &mut PeerState,
        log: &Logger,
    ) -> Result<Option<RequestedOperationDataLock>, StateError> {
        let block_hash = operations_for_block.block_hash();
        let validation_pass = operations_for_block.validation_pass();

        // if was not scheduled, just return None
        match peer
            .queues
            .queued_block_operations
            .lock()?
            .get_mut(block_hash)
        {
            Some(missing_operations) => {
                if !missing_operations.contains(&validation_pass) {
                    warn!(log, "Received unexpected block header operation's validation pass from peer"; "block_header_hash" => block_hash.to_base58_check(), "validation_pass" => validation_pass);
                    peer.message_stats
                        .increment_unexpected_response_operations();
                    return Ok(None);
                }
            }
            None => {
                warn!(log, "Received unexpected block header operation from peer"; "block_header_hash" => block_hash.to_base58_check(), "validation_pass" => validation_pass);
                peer.message_stats
                    .increment_unexpected_response_operations();
                return Ok(None);
            }
        }

        // peer response stats
        match peer.queues.block_operations_response_last.write() {
            Ok(mut response_last) => *response_last = Instant::now(),
            Err(e) => {
                warn!(log, "Failed to update block_operations_response_last from peer"; "reason" => format!("{}", e))
            }
        }

        // if contains, return data lock, when this lock will go out if the scope, then drop will be triggered, and queues will be emptied
        Ok(Some(RequestedOperationDataLock {
            validation_pass,
            block_hash: Arc::new(block_hash.clone()),
            queued_block_operations: peer.queues.queued_block_operations.clone(),
        }))
    }
}

/// Simple lock, when we want to remove data from queues,
/// but make sure that it was handled, and nobody will put the same data to queue, while we are handling them
///
/// When this lock goes out of the scope, then queues will be clear for block_hash
pub struct RequestedBlockDataLock {
    block_hash: Arc<BlockHash>,
    queued_block_headers: Arc<Mutex<HashSet<Arc<BlockHash>>>>,
}

impl Drop for RequestedBlockDataLock {
    fn drop(&mut self) {
        if let Ok(mut queue) = self.queued_block_headers.lock() {
            queue.remove(&self.block_hash);
        }
    }
}

/// Simple lock, when we want to remove data from queues,
/// but make sure that it was handled, and nobody will put the same data to queue, while we are handling them
///
/// When this lock goes out of the scope, then queues will be clear for block_hash
pub struct RequestedOperationDataLock {
    validation_pass: i8,
    block_hash: Arc<BlockHash>,
    queued_block_operations: Arc<Mutex<HashMap<Arc<BlockHash>, MissingOperations>>>,
}

impl Drop for RequestedOperationDataLock {
    fn drop(&mut self) {
        if let Ok(mut queue) = self.queued_block_operations.lock() {
            if let Some(missing_operations) = queue.get_mut(&self.block_hash) {
                missing_operations.remove(&self.validation_pass);
                if missing_operations.is_empty() {
                    queue.remove(&self.block_hash);
                }
            }
        }
    }
}

fn tell_peer(msg: Arc<PeerMessageResponse>, peer: &PeerId) {
    peer.peer_ref.tell(SendMessage::new(msg), None);
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc::channel;
    use std::sync::{Arc, Mutex};
    use std::thread;

    use riker::actors::*;
    use serial_test::serial;
    use slog::Level;

    use crypto::hash::ChainId;
    use networking::p2p::network_channel::NetworkChannel;
    use storage::persistent::PersistentStorage;
    use storage::tests_common::TmpStorage;
    use storage::{operations_meta_storage, OperationsMetaStorage};
    use tezos_messages::p2p::encoding::prelude::OperationsForBlock;

    use crate::chain_feeder::{ChainFeeder, ChainFeederRef};
    use crate::chain_feeder_channel::ChainFeederChannel;
    use crate::shell_channel::ShellChannel;
    use crate::state::data_requester::DataRequester;
    use crate::state::tests::block;
    use crate::state::tests::prerequisites::{
        create_logger, create_test_actor_system, create_test_tokio_runtime, test_peer,
    };

    macro_rules! assert_block_queue_contains {
        ($expected:expr, $queues:expr, $block:expr) => {{
            assert_eq!(
                $expected,
                $queues
                    .queued_block_headers
                    .lock()
                    .unwrap()
                    .contains($block)
            );
        }};
    }

    macro_rules! assert_operations_queue_contains {
        ($expected:expr, $queues:expr, $block:expr, $validation_passes:expr) => {{
            assert_eq!(
                $expected,
                $queues
                    .queued_block_operations
                    .lock()
                    .unwrap()
                    .contains_key($block)
            );

            match $queues.queued_block_operations.lock().unwrap().get($block) {
                Some(missing_operations) => assert_eq!(missing_operations, $validation_passes),
                None => {
                    if $expected {
                        panic!("test failed");
                    }
                }
            };
        }};
    }

    macro_rules! hash_set {
        ( $( $x:expr ),* ) => {
            {
                let mut temp_set = HashSet::new();
                $(
                    temp_set.insert($x);
                )*
                temp_set
            }
        };
    }

    #[test]
    #[serial]
    fn test_requester_fetch_and_receive_block() -> Result<(), failure::Error> {
        // prerequizities
        let log = create_logger(Level::Debug);
        let tokio_runtime = create_test_tokio_runtime();
        let actor_system = create_test_actor_system(log.clone());
        let network_channel =
            NetworkChannel::actor(&actor_system).expect("Failed to create network channel");
        let storage = TmpStorage::create_to_out_dir("__test_requester_fetch_and_receive_block")?;
        let mut peer1 = test_peer(&actor_system, network_channel.clone(), &tokio_runtime, 7777);

        // requester instance
        let data_requester = DataRequester::new(
            OperationsMetaStorage::new(storage.storage()),
            chain_feeder_mock(&actor_system, storage.storage().clone())?,
        );

        // try schedule nothiing
        assert!(matches!(
            data_requester.fetch_block_headers(vec![], &peer1.peer_id, &peer1.queues, &log),
            Ok(false)
        ));

        // try schedule block1
        let block1 = block(1);
        assert!(matches!(
            data_requester.fetch_block_headers(
                vec![block1.clone()],
                &peer1.peer_id,
                &peer1.queues,
                &log
            ),
            Ok(true)
        ));

        // scheduled just for the peer1
        assert_block_queue_contains!(true, peer1.queues, &block1);

        // try schedule block1 once more
        assert!(matches!(
            data_requester.fetch_block_headers(
                vec![block1.clone()],
                &peer1.peer_id,
                &peer1.queues,
                &log
            ),
            Ok(false)
        ));

        // try receive be peer1 - hold lock
        let scheduled = data_requester.block_header_received(&block1, &mut peer1, &log)?;
        assert!(scheduled.is_some());

        // block is still scheduled
        assert_block_queue_contains!(true, peer1.queues, &block1);

        // try schedule block1 once more while holding the lock (should not succeed, because block1 was not removed from queues, becuase we still hold the lock)
        assert!(matches!(
            data_requester.fetch_block_headers(
                vec![block1.clone()],
                &peer1.peer_id,
                &peer1.queues,
                &log
            ),
            Ok(false)
        ));

        // now drop/release lock
        drop(scheduled);

        // block is not scheduled now
        assert_block_queue_contains!(false, peer1.queues, &block1);

        // we can reschedule it once more now
        assert!(matches!(
            data_requester.fetch_block_headers(
                vec![block1.clone()],
                &peer1.peer_id,
                &peer1.queues,
                &log
            ),
            Ok(true)
        ));

        Ok(())
    }

    #[test]
    #[serial]
    fn test_requester_fetch_and_receive_block_operations() -> Result<(), failure::Error> {
        // prerequizities
        let log = create_logger(Level::Debug);
        let tokio_runtime = create_test_tokio_runtime();
        let actor_system = create_test_actor_system(log.clone());
        let network_channel =
            NetworkChannel::actor(&actor_system).expect("Failed to create network channel");
        let storage =
            TmpStorage::create_to_out_dir("__test_requester_fetch_and_receive_block_operations")?;
        let mut peer1 = test_peer(&actor_system, network_channel.clone(), &tokio_runtime, 7777);

        // requester instance
        let data_requester = DataRequester::new(
            OperationsMetaStorage::new(storage.storage()),
            chain_feeder_mock(&actor_system, storage.storage().clone())?,
        );

        // prepare missing operations in db for block with 4 validation_pass
        let block1 = block(1);
        OperationsMetaStorage::new(storage.storage()).put(
            &block1,
            &operations_meta_storage::Meta::new(
                4,
                10,
                ChainId::from_base58_check("NetXgtSLGNJvNye")?,
            ),
        )?;

        // try schedule nothiing
        assert!(matches!(
            data_requester.fetch_block_operations(vec![], &peer1.peer_id, &peer1.queues, &log),
            Ok(false)
        ));

        // try schedule block1
        assert!(matches!(
            data_requester.fetch_block_operations(
                vec![block1.clone()],
                &peer1.peer_id,
                &peer1.queues,
                &log
            ),
            Ok(true)
        ));

        // scheduled just for the peer1
        assert_operations_queue_contains!(true, peer1.queues, &block1, &hash_set![0, 1, 2, 3]);

        // try schedule block1 once more
        assert!(matches!(
            data_requester.fetch_block_operations(
                vec![block1.clone()],
                &peer1.peer_id,
                &peer1.queues,
                &log
            ),
            Ok(false)
        ));

        // try receive be peer1 - hold lock - validation pass 0
        let scheduled = data_requester.block_operations_received(
            &OperationsForBlock::new(block1.as_ref().clone(), 0),
            &mut peer1,
            &log,
        )?;
        assert!(scheduled.is_some());

        // block is still scheduled
        assert_operations_queue_contains!(true, peer1.queues, &block1, &hash_set![0, 1, 2, 3]);

        // try schedule block1 once more while holding the lock (should not succeed, because block1 was not removed from queues, becuase we still hold the lock)
        assert!(matches!(
            data_requester.fetch_block_operations(
                vec![block1.clone()],
                &peer1.peer_id,
                &peer1.queues,
                &log
            ),
            Ok(false)
        ));

        // now drop/release lock
        drop(scheduled);

        // block is still scheduled but less validation passes
        assert_operations_queue_contains!(true, peer1.queues, &block1, &hash_set![1, 2, 3]);

        // we can reschedule it once more now
        assert!(matches!(
            data_requester.fetch_block_operations(
                vec![block1.clone()],
                &peer1.peer_id,
                &peer1.queues,
                &log
            ),
            Ok(false)
        ));

        // download all missing
        assert!(data_requester
            .block_operations_received(
                &OperationsForBlock::new(block1.as_ref().clone(), 1),
                &mut peer1,
                &log,
            )?
            .is_some());
        assert_operations_queue_contains!(true, peer1.queues, &block1, &hash_set![2, 3]);

        assert!(data_requester
            .block_operations_received(
                &OperationsForBlock::new(block1.as_ref().clone(), 3),
                &mut peer1,
                &log,
            )?
            .is_some());
        assert_operations_queue_contains!(true, peer1.queues, &block1, &hash_set![2]);

        assert!(data_requester
            .block_operations_received(
                &OperationsForBlock::new(block1.as_ref().clone(), 2),
                &mut peer1,
                &log,
            )?
            .is_some());
        assert_operations_queue_contains!(false, peer1.queues, &block1, &HashSet::default());

        // we can reschedule it once more now
        assert!(matches!(
            data_requester.fetch_block_operations(
                vec![block1.clone()],
                &peer1.peer_id,
                &peer1.queues,
                &log
            ),
            Ok(true)
        ));

        Ok(())
    }

    fn chain_feeder_mock(
        actor_system: &ActorSystem,
        persistent_storage: PersistentStorage,
    ) -> Result<ChainFeederRef, failure::Error> {
        // run actor's
        let shell_channel =
            ShellChannel::actor(&actor_system).expect("Failed to create shell channel");
        let chain_feeder_channel = ChainFeederChannel::actor(&actor_system)
            .expect("Failed to create chain feeder channel");

        let (block_applier_event_sender, _) = channel();
        let block_applier_run = Arc::new(AtomicBool::new(false));

        actor_system
            .actor_of_props::<ChainFeeder>(
                "mocked_chain_feeder",
                Props::new_args((
                    shell_channel,
                    chain_feeder_channel,
                    persistent_storage,
                    Arc::new(Mutex::new(block_applier_event_sender)),
                    block_applier_run,
                    Arc::new(Mutex::new(Some(thread::spawn(|| Ok(()))))),
                )),
            )
            .map_err(|e| e.into())
    }
}
