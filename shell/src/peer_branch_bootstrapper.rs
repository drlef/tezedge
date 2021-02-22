// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use riker::actors::*;
use slog::{debug, error, warn, Logger};

use crypto::hash::{BlockHash, ChainId};
use networking::PeerId;
use storage::{BlockMetaStorage, BlockMetaStorageReader, OperationsMetaStorage};
use tezos_messages::p2p::encoding::block_header::Level;

use crate::chain_feeder::{ApplyCompletedBlock, ChainFeederRef};
use crate::chain_feeder_channel::{
    subscribe_to_chain_feeder_block_applied_channel, ChainFeederChannelMsg, ChainFeederChannelRef,
};
use crate::shell_channel::{ShellChannelMsg, ShellChannelRef, ShellChannelTopic};
use crate::state::bootstrap_state::{BootstrapState, InnerBlockState};
use crate::state::data_requester::DataRequesterRef;
use crate::state::peer_state::DataQueues;
use crate::state::synchronization_state::PeerBranchSynchronizationDone;
use crate::state::StateError;
use crate::subscription::subscribe_to_actor_terminated;

const MAX_BOOTSTRAP_BRANCHES_PER_PEER: usize = 2;
const MAX_TRIES_FOR_LOOPING_EXISTING_DATA_IN_ONE_RUN: usize = 5000;
const SCHEDULE_ONE_TIMER_NO_DATA_DELAY: Duration = Duration::from_secs(5);
const SCHEDULE_ONE_TIMER_DATA_DELAY: Duration = Duration::from_millis(10);
const PROCESS_BLOCK_APPLY_INTERVAL: Duration = Duration::from_secs(20);

/// We can controll speedup of downloading blocks from network
const MAX_BOOTSTRAP_INTERVAL_AHEAD_LOOK_COUNT: i8 = 2;

/// After this timeout peer will be disconnected if no activity is done on any pipeline
/// So if peer does not change any branch bootstrap, we will disconnect it
const STALE_BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(60);

/// If we have empty bootstrap pipelines for along time, we disconnect peer
const MISSING_NEW_BOOTSTRAP_TIMEOUT: Duration = Duration::from_secs(60 * 2);

/// We controll frequecncy of apply block request from this actor, we dont want to exhausted chain_feeder
const APPLY_BLOCK_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Message commands [`PeerBranchBootstrapper`] to disconnect peer if any of bootstraping pipelines are stalled
#[derive(Clone, Debug)]
pub struct DisconnectStalledBootstraps {
    timeout: Duration,
}

#[derive(Clone, Debug)]
pub struct StartBranchBootstraping {
    chain_id: Arc<ChainId>,
    last_applied_block: Arc<BlockHash>,
    missing_history: Vec<Arc<BlockHash>>,
    to_level: Arc<Level>,
}

impl StartBranchBootstraping {
    pub fn new(
        chain_id: Arc<ChainId>,
        last_applied_block: Arc<BlockHash>,
        missing_history: Vec<Arc<BlockHash>>,
        to_level: Arc<Level>,
    ) -> Self {
        Self {
            chain_id,
            last_applied_block,
            missing_history,
            to_level,
        }
    }
}

/// This message should be trriggered, when all operations for the block are downloaded
#[derive(Clone, Debug)]
pub struct UpdateOperationsState {
    block_hash: Arc<BlockHash>,
}

impl UpdateOperationsState {
    pub fn new(block_hash: Arc<BlockHash>) -> Self {
        Self { block_hash }
    }
}

#[derive(Clone, Debug)]
pub struct UpdateBlockState {
    block_hash: Arc<BlockHash>,
    predecessor_block_hash: Arc<BlockHash>,
    new_state: Arc<InnerBlockState>,
}

impl UpdateBlockState {
    pub fn new(
        block_hash: Arc<BlockHash>,
        predecessor_block_hash: Arc<BlockHash>,
        new_state: InnerBlockState,
    ) -> Self {
        Self {
            block_hash,
            predecessor_block_hash,
            new_state: Arc::new(new_state),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PingProcessDataDownload;

#[derive(Clone, Debug)]
pub struct PingProcessBlockApply;

#[derive(Clone, Debug)]
pub struct BlockAlreadyApplied {
    pub block_hash: Arc<BlockHash>,
}

#[actor(
    StartBranchBootstraping,
    UpdateBlockState,
    UpdateOperationsState,
    DisconnectStalledBootstraps,
    BlockAlreadyApplied,
    PingProcessDataDownload,
    PingProcessBlockApply,
    SystemEvent,
    ChainFeederChannelMsg
)]
pub struct PeerBranchBootstrapper {
    peer: Arc<PeerId>,
    peer_queues: Arc<DataQueues>,
    queued_block_headers_for_apply: HashMap<Arc<BlockHash>, Instant>,

    shell_channel: ShellChannelRef,
    chain_feeder_channel: ChainFeederChannelRef,
    block_meta_storage: BlockMetaStorage,
    operations_meta_storage: OperationsMetaStorage,

    requester: DataRequesterRef,
    block_applier: ChainFeederRef,

    bootstrap_state: Vec<BootstrapState>,
    empty_bootstrap_state: Option<Instant>,
    // TODO: TE-369 - rate limiter
    // Indicates that we triggered check_chain_completeness
    // (means, waiting in actor's mailbox)
    // this is optimization
    // check_process_data_triggered: AtomicBool,
}

pub type PeerBranchBootstrapperRef = ActorRef<PeerBranchBootstrapperMsg>;

impl PeerBranchBootstrapper {
    /// Create new actor instance.
    pub fn actor(
        sys: &ActorSystem,
        peer: Arc<PeerId>,
        peer_queues: Arc<DataQueues>,
        requester: DataRequesterRef,
        shell_channel: ShellChannelRef,
        chain_feeder_channel: ChainFeederChannelRef,
        block_applier: ChainFeederRef,
        block_meta_storage: BlockMetaStorage,
        operations_meta_storage: OperationsMetaStorage,
    ) -> Result<PeerBranchBootstrapperRef, CreateError> {
        sys.actor_of_props::<PeerBranchBootstrapper>(
            &format!("{}-branch-bootstrap", &peer.peer_ref.name()),
            Props::new_args((
                peer,
                peer_queues,
                requester,
                shell_channel,
                chain_feeder_channel,
                block_applier,
                block_meta_storage,
                operations_meta_storage,
            )),
        )
    }

    fn process_data_download(
        &mut self,
        ctx: &Context<PeerBranchBootstrapperMsg>,
        myself: PeerBranchBootstrapperRef,
        log: &Logger,
    ) {
        let PeerBranchBootstrapper {
            peer,
            peer_queues,
            bootstrap_state,
            requester,
            block_meta_storage,
            operations_meta_storage,
            ..
        } = self;

        let mut was_scheduled = false;
        bootstrap_state.iter_mut().for_each(|bootstrap| {
            // schedule next block downloading
            was_scheduled |= schedule_block_downloading(
                peer,
                peer_queues,
                bootstrap,
                requester,
                block_meta_storage,
                operations_meta_storage,
                log,
            );

            // schedule next block downloading
            was_scheduled |= schedule_operations_downloading(
                peer,
                peer_queues,
                bootstrap,
                requester,
                operations_meta_storage,
                log,
            );
        });

        // if scheduled, lets ping quick, if no data, then wait
        if was_scheduled {
            ctx.schedule_once(
                SCHEDULE_ONE_TIMER_DATA_DELAY,
                myself,
                None,
                PingProcessDataDownload,
            );
        } else {
            ctx.schedule_once(
                SCHEDULE_ONE_TIMER_NO_DATA_DELAY,
                myself,
                None,
                PingProcessDataDownload,
            );
        }

        self.handle_resolved_bootstraps();
    }

    fn process_block_apply(
        &mut self,
        _: &Context<PeerBranchBootstrapperMsg>,
        myself: PeerBranchBootstrapperRef,
        log: &Logger,
    ) {
        let PeerBranchBootstrapper {
            bootstrap_state,
            block_meta_storage,
            queued_block_headers_for_apply,
            block_applier,
            ..
        } = self;

        bootstrap_state.iter_mut().for_each(|bootstrap| {
            // schedule next block for apply
            schedule_block_applying(
                bootstrap,
                queued_block_headers_for_apply,
                block_meta_storage,
                myself.clone(),
                block_applier,
                log,
            );
        });

        self.handle_resolved_bootstraps();
    }

    fn handle_resolved_bootstraps(&mut self) {
        let PeerBranchBootstrapper {
            bootstrap_state,
            peer,
            shell_channel,
            ..
        } = self;

        bootstrap_state.retain(|b| {
            if b.is_done() {
                shell_channel.tell(
                    Publish {
                        msg: ShellChannelMsg::PeerBranchSynchronizationDone(
                            PeerBranchSynchronizationDone::new(peer.clone(), b.to_level().clone()),
                        ),
                        topic: ShellChannelTopic::ShellCommands.into(),
                    },
                    None,
                );

                false
            } else {
                true
            }
        });

        if self.bootstrap_state.is_empty() {
            if self.empty_bootstrap_state.is_none() {
                self.empty_bootstrap_state = Some(Instant::now());
            }
        }
    }
}

impl
    ActorFactoryArgs<(
        Arc<PeerId>,
        Arc<DataQueues>,
        DataRequesterRef,
        ShellChannelRef,
        ChainFeederChannelRef,
        ChainFeederRef,
        BlockMetaStorage,
        OperationsMetaStorage,
    )> for PeerBranchBootstrapper
{
    fn create_args(
        (
            peer,
            peer_queues,
            requester,
            shell_channel,
            chain_feeder_channel,
            block_applier,
            block_meta_storage,
            operations_meta_storage,
        ): (
            Arc<PeerId>,
            Arc<DataQueues>,
            DataRequesterRef,
            ShellChannelRef,
            ChainFeederChannelRef,
            ChainFeederRef,
            BlockMetaStorage,
            OperationsMetaStorage,
        ),
    ) -> Self {
        PeerBranchBootstrapper {
            peer,
            peer_queues,
            requester,
            queued_block_headers_for_apply: Default::default(),
            bootstrap_state: Default::default(),
            shell_channel,
            chain_feeder_channel,
            block_applier,
            block_meta_storage,
            operations_meta_storage,
            empty_bootstrap_state: None,
        }
    }
}

impl Actor for PeerBranchBootstrapper {
    type Msg = PeerBranchBootstrapperMsg;

    fn pre_start(&mut self, ctx: &Context<Self::Msg>) {
        subscribe_to_actor_terminated(ctx.system.sys_events(), ctx.myself());
        // from this channel we receive just applied blocks
        subscribe_to_chain_feeder_block_applied_channel(&self.chain_feeder_channel, ctx.myself());

        ctx.schedule::<Self::Msg, _>(
            STALE_BOOTSTRAP_TIMEOUT,
            STALE_BOOTSTRAP_TIMEOUT,
            ctx.myself(),
            None,
            DisconnectStalledBootstraps {
                timeout: STALE_BOOTSTRAP_TIMEOUT,
            }
            .into(),
        );
        ctx.schedule::<Self::Msg, _>(
            PROCESS_BLOCK_APPLY_INTERVAL,
            PROCESS_BLOCK_APPLY_INTERVAL,
            ctx.myself(),
            None,
            PingProcessBlockApply.into(),
        );
    }

    fn recv(&mut self, ctx: &Context<Self::Msg>, msg: Self::Msg, sender: Sender) {
        self.receive(ctx, msg, sender);
    }

    fn sys_recv(
        &mut self,
        ctx: &Context<Self::Msg>,
        msg: SystemMsg,
        sender: Option<BasicActorRef>,
    ) {
        if let SystemMsg::Event(evt) = msg {
            self.receive(ctx, evt, sender);
        }
    }
}

impl Receive<ChainFeederChannelMsg> for PeerBranchBootstrapper {
    type Msg = PeerBranchBootstrapperMsg;

    fn receive(&mut self, ctx: &Context<Self::Msg>, msg: ChainFeederChannelMsg, _: Sender) {
        let ChainFeederChannelMsg::BlockApplied(block_hash) = msg;
        let PeerBranchBootstrapper {
            bootstrap_state, ..
        } = self;

        bootstrap_state.iter_mut().for_each(|bootstrap| {
            bootstrap.block_applied(&block_hash);
        });

        // process blocks, just without sending to feeder, we want to take some time to feeder to process successors
        self.process_data_download(ctx, ctx.myself(), &ctx.system.log());
        self.process_block_apply(ctx, ctx.myself(), &ctx.system.log());
    }
}

impl Receive<SystemEvent> for PeerBranchBootstrapper {
    type Msg = PeerBranchBootstrapperMsg;

    fn receive(
        &mut self,
        ctx: &Context<Self::Msg>,
        msg: SystemEvent,
        _sender: Option<BasicActorRef>,
    ) {
        if let SystemEvent::ActorTerminated(evt) = msg {
            if self.peer.peer_ref.uri().eq(evt.actor.uri()) {
                warn!(ctx.system.log(), "Stopping peer's branch bootstrapper, because peer is terminated";
                                        "peer_id" => self.peer.peer_id_marker.clone(), "peer_ip" => self.peer.peer_address.to_string(), "peer" => self.peer.peer_ref.name(), "peer_uri" => self.peer.peer_ref.uri().to_string());
                ctx.stop(ctx.myself());
            }
        }
    }
}

impl Receive<StartBranchBootstraping> for PeerBranchBootstrapper {
    type Msg = PeerBranchBootstrapperMsg;

    fn receive(
        &mut self,
        ctx: &Context<Self::Msg>,
        msg: StartBranchBootstraping,
        _: Option<BasicActorRef>,
    ) {
        debug!(ctx.system.log(), "Start bootstrapping process";
            "last_applied_block" => msg.last_applied_block.to_base58_check(),
            "missing_history" => msg.missing_history
                .iter()
                .map(|b| b.to_base58_check())
                .collect::<Vec<String>>()
                .join(", "),
            "to_level" => &msg.to_level,
            "peer_id" => self.peer.peer_id_marker.clone(), "peer_ip" => self.peer.peer_address.to_string(), "peer" => self.peer.peer_ref.name(), "peer_uri" => self.peer.peer_ref.uri().to_string(),
        );

        // check closed pipelines
        self.handle_resolved_bootstraps();

        if self.bootstrap_state.len() >= MAX_BOOTSTRAP_BRANCHES_PER_PEER {
            debug!(ctx.system.log(), "Peer has started already maximum ({}) pipeline, so we dont start new one", MAX_BOOTSTRAP_BRANCHES_PER_PEER;
                                    "peer_id" => self.peer.peer_id_marker.clone(), "peer_ip" => self.peer.peer_address.to_string(), "peer" => self.peer.peer_ref.name(), "peer_uri" => self.peer.peer_ref.uri().to_string());
            return;
        }

        // add new bootstrap
        self.bootstrap_state.push(BootstrapState::new(
            msg.chain_id,
            msg.last_applied_block,
            msg.missing_history,
            msg.to_level,
        ));
        self.empty_bootstrap_state = None;

        self.process_data_download(ctx, ctx.myself(), &ctx.system.log());
        self.process_block_apply(ctx, ctx.myself(), &ctx.system.log());
    }
}

impl Receive<PingProcessDataDownload> for PeerBranchBootstrapper {
    type Msg = PeerBranchBootstrapperMsg;

    fn receive(
        &mut self,
        ctx: &Context<Self::Msg>,
        _: PingProcessDataDownload,
        _: Option<BasicActorRef>,
    ) {
        self.process_data_download(ctx, ctx.myself(), &ctx.system.log());
    }
}

impl Receive<PingProcessBlockApply> for PeerBranchBootstrapper {
    type Msg = PeerBranchBootstrapperMsg;

    fn receive(
        &mut self,
        ctx: &Context<Self::Msg>,
        _: PingProcessBlockApply,
        _: Option<BasicActorRef>,
    ) {
        self.process_block_apply(ctx, ctx.myself(), &ctx.system.log());
    }
}

impl Receive<UpdateBlockState> for PeerBranchBootstrapper {
    type Msg = PeerBranchBootstrapperMsg;

    fn receive(
        &mut self,
        ctx: &Context<Self::Msg>,
        msg: UpdateBlockState,
        _: Option<BasicActorRef>,
    ) {
        self.bootstrap_state.iter_mut().for_each(|bootstrap| {
            bootstrap.block_downloaded(
                &msg.block_hash,
                &msg.new_state,
                msg.predecessor_block_hash.clone(),
            )
        });

        self.process_data_download(ctx, ctx.myself(), &ctx.system.log());
        self.process_block_apply(ctx, ctx.myself(), &ctx.system.log());
    }
}

impl Receive<UpdateOperationsState> for PeerBranchBootstrapper {
    type Msg = PeerBranchBootstrapperMsg;

    fn receive(
        &mut self,
        ctx: &Context<Self::Msg>,
        msg: UpdateOperationsState,
        _: Option<BasicActorRef>,
    ) {
        let PeerBranchBootstrapper {
            bootstrap_state, ..
        } = self;

        // check pipelines
        bootstrap_state.iter_mut().for_each(|bootstrap| {
            bootstrap.block_operations_downloaded(&msg.block_hash);
        });

        // kick another processing
        self.process_data_download(ctx, ctx.myself(), &ctx.system.log());
        self.process_block_apply(ctx, ctx.myself(), &ctx.system.log());
    }
}

impl Receive<BlockAlreadyApplied> for PeerBranchBootstrapper {
    type Msg = PeerBranchBootstrapperMsg;

    fn receive(
        &mut self,
        ctx: &Context<Self::Msg>,
        msg: BlockAlreadyApplied,
        _: Option<BasicActorRef>,
    ) {
        let PeerBranchBootstrapper {
            bootstrap_state, ..
        } = self;

        bootstrap_state.iter_mut().for_each(|bootstrap| {
            bootstrap.block_applied(&msg.block_hash);
        });

        self.process_data_download(ctx, ctx.myself(), &ctx.system.log());
        self.process_block_apply(ctx, ctx.myself(), &ctx.system.log());
    }
}

impl Receive<DisconnectStalledBootstraps> for PeerBranchBootstrapper {
    type Msg = PeerBranchBootstrapperMsg;

    fn receive(
        &mut self,
        ctx: &Context<Self::Msg>,
        msg: DisconnectStalledBootstraps,
        _: Option<BasicActorRef>,
    ) {
        self.handle_resolved_bootstraps();

        // check for any stalled bootstrap
        let mut is_stalled = self
            .bootstrap_state
            .iter()
            .any(|bootstrap| bootstrap.is_stalled(msg.timeout));

        if let Some(empty_bootstrap_state) = self.empty_bootstrap_state {
            if empty_bootstrap_state.elapsed() > MISSING_NEW_BOOTSTRAP_TIMEOUT {
                warn!(ctx.system.log(), "Peer did not sent new curent_head/current_branch for a long time";
                    "peer_id" => self.peer.peer_id_marker.clone(), "peer_ip" => self.peer.peer_address.to_string(), "peer" => self.peer.peer_ref.name(), "peer_uri" => self.peer.peer_ref.uri().to_string(),
                );
                is_stalled = true;
            }
        }

        // if stalled, just disconnect peer
        if is_stalled {
            warn!(ctx.system.log(), "Disconnecting peer, because of stalled bootstrap pipeline";
                "peer_id" => self.peer.peer_id_marker.clone(), "peer_ip" => self.peer.peer_address.to_string(), "peer" => self.peer.peer_ref.name(), "peer_uri" => self.peer.peer_ref.uri().to_string(),
            );

            // TODO: unsubscribe from channel ?
            // TODO: plus pridat stop IF na spracovanie akcii
            ctx.system.stop(ctx.myself());
            ctx.system.stop(self.peer.peer_ref.clone());
        }
    }
}

fn block_metadata(
    block_hash: &BlockHash,
    block_meta_storage: &BlockMetaStorage,
    operations_meta_storage: &OperationsMetaStorage,
) -> Result<Option<(InnerBlockState, Arc<BlockHash>)>, StateError> {
    Ok(match block_meta_storage.get(block_hash)? {
        Some(metadata) => match metadata.predecessor() {
            Some(predecessor) => Some((
                InnerBlockState {
                    block_downloaded: metadata.is_downloaded(),
                    applied: metadata.is_applied(),
                    operations_downloaded: operations_meta_storage.is_complete(block_hash)?,
                },
                Arc::new(predecessor.clone()),
            )),
            None => None,
        },
        None => None,
    })
}

fn schedule_block_downloading(
    peer: &Arc<PeerId>,
    peer_queues: &DataQueues,
    bootstrap: &mut BootstrapState,
    requester: &DataRequesterRef,
    block_meta_storage: &BlockMetaStorage,
    operations_meta_storage: &OperationsMetaStorage,
    log: &Logger,
) -> bool {
    // get peer's actual queued items and available capacity for operations
    let (already_queued, available_queue_capacity) = match peer_queues
        .get_already_queued_block_headers_and_max_capacity()
    {
        Ok(queued_and_capacity) => queued_and_capacity,
        Err(e) => {
            warn!(log, "Failed to get available blocks queue capacity for peer (so ignore this step run)"; "reason" => e,
                        "peer_id" => peer.peer_id_marker.clone(), "peer_ip" => peer.peer_address.to_string(), "peer" => peer.peer_ref.name(), "peer_uri" => peer.peer_ref.uri().to_string());
            return false;
        }
    };

    // get blocks to download
    match bootstrap.find_next_blocks_to_download(
        available_queue_capacity,
        already_queued,
        MAX_BOOTSTRAP_INTERVAL_AHEAD_LOOK_COUNT,
        |block_hash| block_metadata(block_hash, block_meta_storage, operations_meta_storage),
    ) {
        Ok(blocks_to_download) => {
            // try schedule
            match requester.fetch_block_headers(blocks_to_download, peer, peer_queues, log) {
                Ok(was_scheduled) => was_scheduled,
                Err(e) => {
                    warn!(log, "Failed to schedule blocks for peer"; "reason" => e,
                        "peer_id" => peer.peer_id_marker.clone(), "peer_ip" => peer.peer_address.to_string(), "peer" => peer.peer_ref.name(), "peer_uri" => peer.peer_ref.uri().to_string());
                    false
                }
            }
        }
        Err(e) => {
            warn!(log, "Failed to find blocks for scheduling for peer"; "reason" => e,
                        "peer_id" => peer.peer_id_marker.clone(), "peer_ip" => peer.peer_address.to_string(), "peer" => peer.peer_ref.name(), "peer_uri" => peer.peer_ref.uri().to_string());
            false
        }
    }
}

fn schedule_operations_downloading(
    peer: &mut Arc<PeerId>,
    peer_queues: &DataQueues,
    bootstrap: &mut BootstrapState,
    requester: &DataRequesterRef,
    operations_meta_storage: &mut OperationsMetaStorage,
    log: &Logger,
) -> bool {
    // get peer's actual queued items and available capacity for operations
    let (already_queued, available_queue_capacity) = match peer_queues
        .get_already_queued_block_operations_and_max_capacity()
    {
        Ok(queued_and_capacity) => queued_and_capacity,
        Err(e) => {
            warn!(log, "Failed to get available operations queue capacity for peer (so ignore this step run)"; "reason" => e,
                        "peer_id" => peer.peer_id_marker.clone(), "peer_ip" => peer.peer_address.to_string(), "peer" => peer.peer_ref.name(), "peer_uri" => peer.peer_ref.uri().to_string());
            return false;
        }
    };

    // get blocks to download
    match bootstrap.find_next_block_operations_to_download(
        available_queue_capacity,
        already_queued,
        MAX_BOOTSTRAP_INTERVAL_AHEAD_LOOK_COUNT,
        |block_hash| {
            operations_meta_storage
                .is_complete(block_hash)
                .map_err(StateError::from)
        },
    ) {
        Ok(blocks_to_download) => {
            // try schedule
            match requester.fetch_block_operations(blocks_to_download, peer, peer_queues, log) {
                Ok(was_scheduled) => was_scheduled,
                Err(e) => {
                    warn!(log, "Failed to schedule blocks for missing operations for peer"; "reason" => e,
                        "peer_id" => peer.peer_id_marker.clone(), "peer_ip" => peer.peer_address.to_string(), "peer" => peer.peer_ref.name(), "peer_uri" => peer.peer_ref.uri().to_string());
                    false
                }
            }
        }
        Err(e) => {
            warn!(log, "Failed to find blocks with missing operations for scheduling for peer"; "reason" => e,
                        "peer_id" => peer.peer_id_marker.clone(), "peer_ip" => peer.peer_address.to_string(), "peer" => peer.peer_ref.name(), "peer_uri" => peer.peer_ref.uri().to_string());
            false
        }
    }
}

fn schedule_block_applying(
    bootstrap: &mut BootstrapState,
    queued_block_headers_for_apply: &mut HashMap<Arc<BlockHash>, Instant>,
    block_meta_storage: &mut BlockMetaStorage,
    myself: PeerBranchBootstrapperRef,
    block_applier: &ChainFeederRef,
    log: &Logger,
) {
    let mut max_tries = 0;
    let next_block_to_apply: Option<Arc<BlockHash>> = loop {
        max_tries += 1;
        // this means we are looping through existing data, which somebody downloaded before, so we wait for maybe applied
        if max_tries > MAX_TRIES_FOR_LOOPING_EXISTING_DATA_IN_ONE_RUN {
            break None;
        }

        let block = bootstrap.next_block_to_apply();
        match block {
            Some(block) => {
                // check metadata, if any other peer processed this block
                match block_meta_storage.get(&block) {
                    Ok(Some(metadata)) => {
                        // if completed, lets continue
                        if metadata.is_applied() {
                            // update current state and continue
                            bootstrap.block_applied(&block);
                        } else {
                            break Some(block);
                        }
                    }
                    Ok(None) => {
                        // missing metadata for operations, which are created when inserting header
                        break None;
                    }
                    Err(e) => {
                        error!(log, "Failed to read block header metadata"; "block" => block.to_base58_check(), "reason" => e);
                        break None;
                    }
                }
            }
            None => break None,
        }
    };

    if let Some(block) = next_block_to_apply {
        let mut can_be_scheduled = {
            // lets check time-to-live reqest queue
            if let Some(request) = queued_block_headers_for_apply.get_mut(&block) {
                if request.elapsed() > APPLY_BLOCK_REQUEST_TIMEOUT {
                    *request = Instant::now();
                    true
                } else {
                    false
                }
            } else {
                queued_block_headers_for_apply.clear();
                queued_block_headers_for_apply.insert(block.clone(), Instant::now());
                true
            }
        };

        // last check
        if let Ok(true) = block_meta_storage.is_applied(&block) {
            can_be_scheduled = false;
        }

        if can_be_scheduled {
            block_applier.tell(
                ApplyCompletedBlock::new(
                    block.as_ref().clone(),
                    bootstrap.chain_id().clone(),
                    None,
                    Some(myself),
                    Instant::now(),
                ),
                None,
            );
        }
    }
}
