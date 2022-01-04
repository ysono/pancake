use crate::storage::engine_ssi::container::LSMTree;
use anyhow::Result;

impl<'a, K, V> LSMTree<K, V> {
    /// GC deprecated nodes of the internal linked list.
    pub fn delete_dangling_slices(&self) -> Result<()> {
        let min_used_list_ver = self.list_ver_state.trailing();
        loop {
            match self.dangling_slices.peek() {
                Some(dangl_slice) if dangl_slice.list_ver < min_used_list_ver => {
                    let dangl_slice = self.dangling_slices.pop().unwrap();
                    dangl_slice.drop_dirs_and_memory()?;
                }
                _ => break,
            }
        }
        Ok(())
    }

    pub fn is_cleanup_done(&self) -> bool {
        self.dangling_slices.peek().is_none()
    }
}
