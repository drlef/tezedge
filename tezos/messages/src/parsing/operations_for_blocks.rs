extern crate nom;

use nom::{IResult,
          multi::many0,
          sequence::tuple,
          bytes::complete::take,
          number::complete::{be_i8, be_u8, be_u32}};

use crypto::hash::{HashType, BlockHash, OperationListListHash};
use crate::p2p::encoding::operations_for_blocks::{
    OperationsForBlocksMessage, OperationsForBlock, Path, PathLeft, PathRight};
use crate::p2p::encoding::operation::Operation;


// Intermediary type used during parsing but not for final representation
#[derive(Clone, PartialEq, Debug)]
pub enum PathType {
    Left,
    Right,
    Op,
}

impl From<u8> for PathType {
    fn from(i: u8) -> Self {
        match i {
            0xf0 => PathType::Left,
            0x0f => PathType::Right,
            0x00 => PathType::Op,
            _ => unimplemented!("not a recognised path type"),
        }
    }
}


fn block_hash(input: &[u8]) -> IResult<&[u8], BlockHash> {
    take(HashType::BlockHash.size())(input)
        .map(|(input, block_hash)| (input, block_hash.to_vec()))
}

fn operation_list_list_hash(input: &[u8]) -> IResult<&[u8], OperationListListHash> {
    take(HashType::OperationListListHash.size())(input)
        .map(|(input, block_hash)| (input, block_hash.to_vec()))
}

fn operations_for_block(input: &[u8]) -> IResult<&[u8], OperationsForBlock> {
    tuple((block_hash, be_i8))(input)
        .map(|(input, (block_hash, verification_pass))| (input, OperationsForBlock::new(block_hash, verification_pass)))
}

fn type_of_path(input: &[u8]) -> IResult<&[u8], PathType> {
    be_u8(input)
        .map(|(input, path_type)| (input, path_type.into()))
}

fn path(input: &[u8]) -> IResult<&[u8], Path> {
    let mut path_types: Vec<PathType> = vec![];
    let mut right_hashes: Vec<OperationListListHash> = vec![];
    let mut left_hashes: Vec<OperationListListHash> = vec![];
    let mut left_hash_count = 0;
    let mut offset = 0;

    let (input, path_type) = type_of_path(input)?;
    path_types.push(path_type);

    // read up to the Op
    while *path_types.last().unwrap() != PathType::Op {
        let input = &input[offset..];
        let input = match path_types.last() {
            Some(PathType::Left) => {
                left_hash_count += 1;
                input
            }
            Some(PathType::Right) => {
                let (input, hash) = operation_list_list_hash(input)?;
                right_hashes.push(hash);
                offset += HashType::OperationListListHash.size();
                input
            },
            Some(PathType::Op) => panic!("shouldn't be here!"),
            None => panic!("scary"),
        };
        let (_, path_type) = type_of_path(input)?;
        path_types.push(path_type);
        offset += 1;
    }

    // TODO must be a better way. can we pass input between while iterations?
    let input = &input[offset..];
    offset = 0;

    // read all the hashes for left branches
    while left_hash_count > 0 {
        let input = &input[offset..];
        let (_input, hash) = operation_list_list_hash(input)?;
        left_hashes.push(hash);
        offset += HashType::OperationListListHash.size();
        left_hash_count -= 1;
    }

    let input = &input[offset..];

    // Left hashes are reversed in input, so reverse them to be able to pop them.
    left_hashes.reverse();
    // We want to build the path backwards, from Op to root.
    path_types.reverse();

    let mut path: Path = Path::Op;
    for path_type in &path_types {
        let next_path = match path_type {
            PathType::Left => Path::Left(Box::new(PathLeft::new(path, left_hashes.pop().unwrap(), Default::default()))),
            PathType::Right => Path::Right(Box::new(PathRight::new(right_hashes.pop().unwrap(), path, Default::default()))),
            PathType::Op => Path::Op,
        };
        path = next_path;
    }

    Ok((input, path))
}

fn operation(input: &[u8]) -> IResult<&[u8], Operation> {
    let (input, (no_bytes, branch)) = tuple((be_u32, block_hash))(input)
        .map(|(input, (no_bytes, branch))| (input, (no_bytes - HashType::BlockHash.size() as u32, branch)))?;

    let (input, op_data) = take(no_bytes)(input)?;
    Ok((input, Operation::new(branch, op_data.to_vec())))
}

fn operations(input: &[u8]) -> IResult<&[u8], Vec<Operation>> {
    many0(operation)(input)
}

pub fn operations_for_blocks_message(input: &[u8]) -> IResult<&[u8], OperationsForBlocksMessage> {
    tuple((operations_for_block, path, operations))(input)
        .map(|(input, (operations_for_block, path, operations))| {
            (input, OperationsForBlocksMessage::new(operations_for_block, path, operations))
        })
}
