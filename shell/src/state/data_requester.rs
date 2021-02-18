// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

//! We need to fetch different data from p2p or send data to protocol validation
//! Main purpose of this module is to synchronize this request/responses to prevent unneceserry duplicated calls

use std::collections::HashSet;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use riker::actors::*;
use slog::{warn, Logger};

use crypto::hash::BlockHash;
use networking::p2p::peer::SendMessage;
use networking::PeerId;
use tezos_messages::p2p::encoding::prelude::{GetBlockHeadersMessage, PeerMessageResponse};

use crate::chain_feeder::ChainFeederRef;
use crate::state::peer_state::{DataQueues, PeerState};
use crate::state::StateError;

/// Limit to how many blocks to request from peers
const BLOCK_HEADERS_MAX_QUEUE_SIZE: u16 = 10000;
/// Limit to how many block operations to request from peers
const BLOCK_OPERATIONS_MAX_QUEUE_SIZE: u16 = 10000;

/// Shareable ref between threads
pub type DataRequesterRef = Arc<DataRequester>;

/// Requester manages global request/response queues for data
/// and also manages local queues for every peer.
pub struct DataRequester {
    /// Chain feeder - actor, which is responsible to apply_block to context
    block_applier: ChainFeederRef,

    /// Global queues, used for synchronizing and limiting the same requests from different peers
    queues: Arc<DataQueues>,
}

impl DataRequester {
    pub fn new(block_applier: ChainFeederRef) -> Self {
        Self {
            block_applier,
            queues: Arc::new(DataQueues::new(
                BLOCK_HEADERS_MAX_QUEUE_SIZE,
                BLOCK_OPERATIONS_MAX_QUEUE_SIZE,
            )),
        }
    }

    /// Tries to schedule blocks downloading from peer
    ///
    /// Returns true if was scheduled and p2p message was sent
    pub fn fetch_block_headers(
        &self,
        mut blocks_to_download: Vec<Arc<BlockHash>>,
        peer: &PeerId,
        peer_queues: &DataQueues,
        log: &Logger,
    ) -> Result<bool, StateError> {
        // check if empty
        if blocks_to_download.is_empty() {
            return Ok(false);
        }

        // trim to max capacity
        let global_available_capacity = self.queues.available_queued_block_headers_capacity()?;
        let blocks_to_download = if global_available_capacity < blocks_to_download.len() {
            blocks_to_download
                .drain(0..global_available_capacity)
                .collect()
        } else {
            blocks_to_download
        };

        // get queue locks
        let mut global_queued_block_headers = self.queues.queued_block_headers.lock()?;
        let mut peer_queued_block_headers = peer_queues.queued_block_headers.lock()?;

        // fillter non-queued
        let blocks_to_download = blocks_to_download
            .into_iter()
            .filter(|block_hash| {
                !global_queued_block_headers.contains(block_hash.as_ref())
                    && !peer_queued_block_headers.contains(block_hash.as_ref())
            })
            .collect::<Vec<Arc<BlockHash>>>();

        // if empty finish
        if blocks_to_download.is_empty() {
            Ok(false)
        } else {
            // add to queues
            let _ = global_queued_block_headers.extend(blocks_to_download.clone());
            let _ = peer_queued_block_headers.extend(blocks_to_download.clone());

            // send p2p msg - now we can fire msg to peer
            let blocks_to_download = blocks_to_download
                .iter()
                .map(|b| b.as_ref().clone())
                .collect::<Vec<BlockHash>>();

            tell_peer(GetBlockHeadersMessage::new(blocks_to_download).into(), peer);

            // release locks
            drop(peer_queued_block_headers);
            drop(global_queued_block_headers);

            // peer request stats
            match peer_queues.block_request_last.write() {
                Ok(mut block_request_last) => *block_request_last = Instant::now(),
                Err(e) => {
                    warn!(log, "Failed to update block_request_last from peer"; "reason" => format!("{}", e))
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
    ) -> Result<Option<RequestedDataLock>, StateError> {
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
            Ok(mut block_response_last) => *block_response_last = Instant::now(),
            Err(e) => {
                warn!(log, "Failed to update block_response_last from peer"; "reason" => format!("{}", e))
            }
        }

        // if contains, return data lock, when this lock will go out if the scope, then drop will be triggered, and queues will be emptied
        Ok(Some(RequestedDataLock {
            block_hash: Arc::new(block_hash.clone()),
            peer_queued_block_headers: peer.queues.queued_block_headers.clone(),
            global_queued_block_headers: self.queues.queued_block_headers.clone(),
        }))
    }
}

/// Simple lock, when we want to remove data from queues,
/// but make sure that it was handled, and nobody will put the same data to queue, while we are handling them
///
/// When this lock goes out of the scope, then queues will be clear for block_hash
pub struct RequestedDataLock {
    block_hash: Arc<BlockHash>,
    global_queued_block_headers: Arc<Mutex<HashSet<Arc<BlockHash>>>>,
    peer_queued_block_headers: Arc<Mutex<HashSet<Arc<BlockHash>>>>,
}

impl Drop for RequestedDataLock {
    fn drop(&mut self) {
        if let Ok(mut global_queued_block_headers) = self.global_queued_block_headers.lock() {
            global_queued_block_headers.remove(&self.block_hash);
        }
        if let Ok(mut peer_queued_block_headers) = self.peer_queued_block_headers.lock() {
            peer_queued_block_headers.remove(&self.block_hash);
        }
    }
}

fn tell_peer(msg: Arc<PeerMessageResponse>, peer: &PeerId) {
    peer.peer_ref.tell(SendMessage::new(msg), None);
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicBool;
    use std::sync::mpsc::channel;
    use std::sync::{Arc, Mutex};
    use std::thread;

    use riker::actors::*;
    use slog::Level;

    use networking::p2p::network_channel::NetworkChannel;
    use storage::tests_common::TmpStorage;

    use crate::chain_feeder::{ChainFeeder, ChainFeederRef};
    use crate::chain_feeder_channel::ChainFeederChannel;
    use crate::shell_channel::ShellChannel;
    use crate::state::data_requester::DataRequester;
    use crate::state::tests::block;
    use crate::state::tests::prerequisites::{
        create_logger, create_test_actor_system, create_test_tokio_runtime, test_peer,
    };

    #[macro_export]
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

    #[test]
    fn test_requester_fetch_and_receive_block() -> Result<(), failure::Error> {
        // prerequizities
        let log = create_logger(Level::Debug);
        let tokio_runtime = create_test_tokio_runtime();
        let actor_system = create_test_actor_system(log.clone());
        let network_channel =
            NetworkChannel::actor(&actor_system).expect("Failed to create network channel");
        let mut peer1 = test_peer(&actor_system, network_channel.clone(), &tokio_runtime, 7777);
        let mut peer2 = test_peer(&actor_system, network_channel, &tokio_runtime, 7778);

        // requester instance
        let data_requester = DataRequester::new(chain_feeder_mock(&actor_system)?);

        // try schedule nothging
        assert!(matches!(
            data_requester.fetch_block_headers(vec![], &peer1.peer_id, &peer1.queues, &log),
            Ok(false)
        ));

        // try schedule block
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
        assert!(matches!(
            data_requester.fetch_block_headers(
                vec![block1.clone()],
                &peer2.peer_id,
                &peer2.queues,
                &log
            ),
            Ok(false)
        ));

        // scheduled just for the peer1
        assert_block_queue_contains!(true, data_requester.queues, &block1);
        assert_block_queue_contains!(true, peer1.queues, &block1);
        assert_block_queue_contains!(false, peer2.queues, &block1);

        // try receive with peer2 - None, means was not scheduled
        assert!(matches!(
            data_requester.block_header_received(&block1, &mut peer2, &log),
            Ok(None)
        ));

        // try receive be peer1 - hold lock
        let scheduled = data_requester.block_header_received(&block1, &mut peer1, &log)?;
        assert!(scheduled.is_some());

        // try schedule for peer2 (should not succeed, because block1 was not removed from queues, becuase we still hold the lock)
        assert!(matches!(
            data_requester.fetch_block_headers(
                vec![block1.clone()],
                &peer2.peer_id,
                &peer2.queues,
                &log
            ),
            Ok(false)
        ));
        assert_block_queue_contains!(false, peer2.queues, &block1);

        // do some processing
        if let Some(scheduled_lock) = scheduled.as_ref() {
            // try schedule for peer2 (should not succeed, because block1 was not removed from queues, becuase we still hold the lock)
            assert!(matches!(
                data_requester.fetch_block_headers(
                    vec![block1.clone()],
                    &peer2.peer_id,
                    &peer2.queues,
                    &log,
                ),
                Ok(false)
            ));
            assert_block_queue_contains!(false, peer2.queues, &block1);
            // do more processing ...

            drop(scheduled_lock);
        }

        // drop the lock (this should remove from queues)
        drop(scheduled);

        // check queues
        assert_block_queue_contains!(false, data_requester.queues, &block1);
        assert_block_queue_contains!(false, peer1.queues, &block1);
        assert_block_queue_contains!(false, peer2.queues, &block1);

        // now we can schedule block for peer2 and it should succeed
        assert!(matches!(
            data_requester.fetch_block_headers(
                vec![block1.clone()],
                &peer2.peer_id,
                &peer2.queues,
                &log
            ),
            Ok(true)
        ));
        assert_block_queue_contains!(true, data_requester.queues, &block1);
        assert_block_queue_contains!(false, peer1.queues, &block1);
        assert_block_queue_contains!(true, peer2.queues, &block1);

        Ok(())
    }

    fn chain_feeder_mock(actor_system: &ActorSystem) -> Result<ChainFeederRef, failure::Error> {
        // temp storate
        let storage = TmpStorage::create_to_out_dir("__chain_feeder_mock")?;

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
                    storage.storage().clone(),
                    Arc::new(Mutex::new(block_applier_event_sender)),
                    block_applier_run,
                    Arc::new(Mutex::new(Some(thread::spawn(|| Ok(()))))),
                )),
            )
            .map_err(|e| e.into())
    }
}
