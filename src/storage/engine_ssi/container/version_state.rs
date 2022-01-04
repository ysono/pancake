use hashbag::HashBag;
use std::hash::Hash;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, MutexGuard};

pub struct VersionStateLocked<Ver> {
    leading: Ver,
    gap: HashBag<Ver>,
}

impl<Ver> VersionStateLocked<Ver>
where
    Ver: From<u64> + DerefMut<Target = u64> + Hash + Eq + Copy,
{
    fn get_and_inc_leading(&mut self) -> Ver {
        let penult = self.leading;
        *self.leading += 1;
        penult
    }

    fn hold_leading(&mut self) -> Ver {
        self.gap.insert(self.leading);
        self.leading
    }

    fn unhold(&mut self, ver_to_unhold: Ver) {
        self.gap.remove(&ver_to_unhold);
    }

    fn advance_trailing(&self, mut trail: Ver) -> Ver {
        while *trail < *self.leading && self.gap.contains(&trail) == 0 {
            *trail += 1;
        }
        trail
    }
}

pub struct VersionState<Ver> {
    locked: Mutex<VersionStateLocked<Ver>>,

    /// Must be modified under the guard of `locked.lock()`.
    /// Can be read locklessly.
    trailing: AtomicU64,
}

impl<Ver> VersionState<Ver>
where
    Ver: DerefMut<Target = u64> + From<u64> + Hash + Eq + Copy,
{
    pub fn new(initial: Ver) -> Self {
        Self {
            locked: Mutex::new(VersionStateLocked {
                leading: initial,
                gap: HashBag::new(),
            }),
            trailing: AtomicU64::new(*initial),
        }
    }

    /// @return The leading Ver.
    pub async fn leading(&self) -> Ver {
        let guard = self.locked.lock().await;
        guard.leading
    }

    /// @return The penultimate Ver.
    pub async fn get_and_inc_leading(&self) -> Ver {
        let mut guard = self.locked.lock().await;
        let penultimate = guard.get_and_inc_leading();
        self.do_advance_trailing(&guard);
        penultimate
    }

    /// @return The leading Ver.
    pub async fn hold_leading(&self) -> Ver {
        let mut guard = self.locked.lock().await;
        guard.hold_leading()
    }

    pub async fn unhold<CB>(&self, ver_to_unhold: Ver, on_trailing_advanced: CB)
    where
        CB: FnOnce(),
    {
        let mut guard = self.locked.lock().await;
        guard.unhold(ver_to_unhold);
        let did_advance = self.do_advance_trailing(&guard);
        if did_advance {
            on_trailing_advanced();
        }
    }

    /// @return The leading Ver.
    pub async fn hold_leading_and_unhold<CB>(
        &self,
        ver_to_unhold: Ver,
        on_trailing_advanced: CB,
    ) -> Ver
    where
        CB: FnOnce(),
    {
        let mut guard = self.locked.lock().await;
        if guard.leading == ver_to_unhold {
            return guard.leading;
        } else {
            let leading = guard.hold_leading();
            guard.unhold(ver_to_unhold);
            let did_advance = self.do_advance_trailing(&guard);
            if did_advance {
                on_trailing_advanced();
            }
            return leading;
        }
    }

    fn do_advance_trailing(&self, guard: &MutexGuard<VersionStateLocked<Ver>>) -> bool {
        let trail_orig = self.trailing();
        let trail = guard.advance_trailing(trail_orig);
        let did_advance = trail_orig != trail;
        if did_advance {
            self.trailing.store(*trail, Ordering::SeqCst);
        }
        did_advance
    }

    pub fn trailing(&self) -> Ver {
        let trail = self.trailing.load(Ordering::SeqCst);
        Ver::from(trail)
    }
}
