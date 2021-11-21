use super::*;
use crate::{
    kv::tables::{self, CumulativeData},
    models::*,
    MutableTransaction,
};
use ethereum_types::*;

pub async fn initialize_genesis<'db, Tx>(txn: &Tx, chainspec: ChainSpec) -> anyhow::Result<bool>
where
    Tx: MutableTransaction<'db>,
{
    let genesis = chainspec.genesis.number;
    if txn.get(&tables::CanonicalHeader, genesis).await?.is_some() {
        return Ok(false);
    }

    let mut state_buffer = Buffer::new(txn, genesis, None);
    state_buffer.begin_block(genesis);
    // Allocate accounts
    if let Some(balances) = chainspec.balances.get(&genesis) {
        for (&address, &balance) in balances {
            state_buffer
                .update_account(
                    address,
                    None,
                    Some(Account {
                        balance,
                        ..Default::default()
                    }),
                )
                .await?;
        }
    }

    state_buffer.write_to_db().await?;

    crate::stages::promote_clean_state(txn).await?;
    crate::stages::promote_clean_code(txn).await?;
    let state_root = crate::stages::generate_interhashes(txn).await?;

    let header = BlockHeader {
        parent_hash: H256::zero(),
        beneficiary: chainspec.genesis.author,
        state_root,
        logs_bloom: Bloom::zero(),
        difficulty: chainspec.genesis.seal.difficulty(),
        number: genesis,
        gas_limit: chainspec.genesis.gas_limit,
        gas_used: 0,
        timestamp: chainspec.genesis.timestamp,
        extra_data: chainspec.genesis.seal.extra_data(),
        mix_hash: chainspec.genesis.seal.mix_hash(),
        nonce: chainspec.genesis.seal.nonce(),
        base_fee_per_gas: None,

        receipts_root: EMPTY_ROOT,
        ommers_hash: EMPTY_LIST_HASH,
        transactions_root: EMPTY_ROOT,
    };
    let block_hash = header.hash();

    txn.set(&tables::Header, ((genesis, block_hash), header.clone()))
        .await?;
    txn.set(&tables::CanonicalHeader, (genesis, block_hash))
        .await?;
    txn.set(&tables::HeaderNumber, (block_hash, genesis))
        .await?;
    txn.set(
        &tables::HeadersTotalDifficulty,
        ((genesis, block_hash), header.difficulty),
    )
    .await?;

    txn.set(
        &tables::BlockBody,
        (
            (genesis, block_hash),
            BodyForStorage {
                base_tx_id: 0.into(),
                tx_amount: 0,
                uncles: vec![],
            },
        ),
    )
    .await?;

    txn.set(
        &tables::CumulativeIndex,
        (genesis, CumulativeData { gas: 0, tx_num: 0 }),
    )
    .await?;

    txn.set(&tables::LastHeader, (Default::default(), block_hash))
        .await?;

    txn.set(&tables::Config, (block_hash, chainspec)).await?;

    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        kv::traits::{MutableKV, Transaction},
        new_mem_database,
    };
    use hex_literal::hex;

    #[tokio::test]
    async fn init_mainnet_genesis() {
        let db = new_mem_database().unwrap();
        let tx = db.begin_mutable().await.unwrap();

        assert!(
            initialize_genesis(&tx, crate::res::chainspec::MAINNET.clone())
                .await
                .unwrap()
        );

        let genesis_hash = tx
            .get(&tables::CanonicalHeader, 0.into())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            genesis_hash,
            hex!("d4e56740f876aef8c010b86a40d5f56745a118d0906a34e69aec8c0db1cb8fa3").into()
        );
    }
}
