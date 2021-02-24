extern crate nom;

use nom::{IResult, Err,
          error::{Error, ErrorKind},
          number::complete::{be_u16, be_u32}};

use crate::p2p::encoding::peer::{PeerMessage, PeerMessageResponse};
use crate::parsing::operations_for_blocks::operations_for_blocks_message;


fn peer_message(input: &[u8]) -> IResult<&[u8], PeerMessage> {
    let (input, message_type) = be_u16(input)?;
    match message_type {
        0x61 => operations_for_blocks_message(input).map(|(input, operations_for_blocks_message)| (input, PeerMessage::OperationsForBlocks(operations_for_blocks_message))),
        _ => unimplemented!("Unrecognized peer message type"),
    }
}

pub fn peer_message_response(input: &[u8]) -> IResult<&[u8], PeerMessageResponse> {
    let (input, message_size) = be_u32(input)?;
    if input.len() != message_size as usize {
        return Err(Err::Failure(Error::new(input, ErrorKind::LengthValue)));
    }
    peer_message(input)
        .map(|(input, peer_message)| (input, peer_message.into()))
}
