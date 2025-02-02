use crate::downloader::headers::{
    header_slice_status_watch::HeaderSliceStatusWatch,
    header_slice_verifier, header_slices,
    header_slices::{HeaderSlice, HeaderSliceStatus, HeaderSlices},
    preverified_hashes_config::PreverifiedHashesConfig,
};
use parking_lot::RwLockUpgradableReadGuard;
use std::{ops::DerefMut, sync::Arc};
use tracing::*;

/// Checks that block hashes are matching the expected ones and sets Verified status.
pub struct VerifyStagePreverified {
    header_slices: Arc<HeaderSlices>,
    pending_watch: HeaderSliceStatusWatch,
    preverified_hashes: PreverifiedHashesConfig,
}

impl VerifyStagePreverified {
    pub fn new(
        header_slices: Arc<HeaderSlices>,
        preverified_hashes: PreverifiedHashesConfig,
    ) -> Self {
        Self {
            header_slices: header_slices.clone(),
            pending_watch: HeaderSliceStatusWatch::new(
                HeaderSliceStatus::Downloaded,
                header_slices,
                "VerifyStagePreverified",
            ),
            preverified_hashes,
        }
    }

    pub async fn execute(&mut self) -> anyhow::Result<()> {
        debug!("VerifyStagePreverified: start");
        self.pending_watch.wait().await?;

        debug!(
            "VerifyStagePreverified: verifying {} slices",
            self.pending_watch.pending_count()
        );
        self.verify_pending()?;
        debug!("VerifyStagePreverified: done");
        Ok(())
    }

    fn verify_pending(&self) -> anyhow::Result<()> {
        self.header_slices.for_each(|slice_lock| {
            let slice = slice_lock.upgradable_read();
            if slice.status == HeaderSliceStatus::Downloaded {
                let is_verified = self.verify_slice(&slice);

                let mut slice = RwLockUpgradableReadGuard::upgrade(slice);
                if is_verified {
                    self.header_slices
                        .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Verified);
                } else {
                    self.header_slices
                        .set_slice_status(slice.deref_mut(), HeaderSliceStatus::Invalid);
                }
            }
            None
        })
    }

    /// The algorithm verifies that the edges of the slice match to the preverified hashes,
    /// and that all blocks down to the root of the slice are connected by the parent_hash field.
    ///
    /// For example, if we have a HeaderSlice[192...384]
    /// (with block headers from 192 to 384 inclusive), it verifies that:
    ///
    /// hash(slice[384]) == preverified hash(384)
    /// hash(slice[383]) == slice[384].parent_hash
    /// hash(slice[382]) == slice[383].parent_hash
    /// ...
    /// hash(slice[192]) == slice[193].parent_hash
    /// hash(slice[192]) == preverified hash(192)
    ///
    /// Thus verifying hashes of all the headers.
    fn verify_slice(&self, slice: &HeaderSlice) -> bool {
        if slice.headers.is_none() {
            return false;
        }
        let headers = slice.headers.as_ref().unwrap();

        if headers.is_empty() {
            return false;
        }

        let first = headers.first().unwrap();
        let first_hash = first.hash();
        let expected_first_hash = self.preverified_hash(slice.start_block_num.0);
        if expected_first_hash.is_none() {
            return false;
        }
        if first_hash != *expected_first_hash.unwrap() {
            return false;
        }

        let last = headers.last().unwrap();
        let last_hash = last.hash();
        let expected_last_hash =
            self.preverified_hash(slice.start_block_num.0 + headers.len() as u64 - 1);
        if expected_last_hash.is_none() {
            return false;
        }
        if last_hash != *expected_last_hash.unwrap() {
            return false;
        }

        header_slice_verifier::verify_slice_is_linked_by_parent_hash(headers)
    }

    fn preverified_hash(&self, block_num: u64) -> Option<&ethereum_types::H256> {
        let preverified_step_size = header_slices::HEADER_SLICE_SIZE as u64;
        if block_num % preverified_step_size != 0 {
            return None;
        }
        let index = block_num / preverified_step_size;
        self.preverified_hashes.hashes.get(index as usize)
    }
}

#[async_trait::async_trait]
impl super::stage::Stage for VerifyStagePreverified {
    async fn execute(&mut self) -> anyhow::Result<()> {
        VerifyStagePreverified::execute(self).await
    }
}
