use anyhow::{anyhow, Result};
use std::collections::HashMap;
use std::hash::Hash;

pub struct Multiset<T> {
    dict: HashMap<T, usize>,
}

impl<T> Multiset<T>
where
    T: Hash + Eq,
{
    pub fn default() -> Self {
        Self {
            dict: Default::default(),
        }
    }

    pub fn len(&self) -> usize {
        self.dict.len()
    }

    pub fn add(&mut self, elem: T) {
        self.dict
            .entry(elem)
            .and_modify(|count| *count += 1)
            .or_insert(1);
    }

    /// @return the post-removal count
    pub fn remove(&mut self, elem: &T) -> Result<usize> {
        match self.dict.get_mut(elem) {
            None => return Err(anyhow!("not found")),
            Some(count) => {
                if *count > 1 {
                    *count -= 1;
                    return Ok(*count);
                } else {
                    self.dict.remove(elem);
                    return Ok(0);
                }
            }
        };
    }

    pub fn count(&self, elem: &T) -> usize {
        if let Some(count) = self.dict.get(elem) {
            return *count;
        }
        return 0;
    }

    pub fn contains(&self, elem: &T) -> bool {
        self.count(elem) > 0
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn add_then_remove() {
        let mut ms = Multiset::default();

        let key = "asdf".to_owned();

        assert_eq!(ms.dict.get(&key), None);
        assert_eq!(ms.count(&key), 0);
        assert_eq!(ms.contains(&key), false);

        ms.add(key.clone());
        assert_eq!(ms.dict.get(&key), Some(&1));
        assert_eq!(ms.count(&key), 1);
        assert_eq!(ms.contains(&key), true);

        ms.add(key.clone());
        assert_eq!(ms.dict.get(&key), Some(&2));
        assert_eq!(ms.count(&key), 2);
        assert_eq!(ms.contains(&key), true);

        let remove_result = ms.remove(&key);
        assert!(remove_result.is_ok());
        assert_eq!(ms.dict.get(&key), Some(&1));
        assert_eq!(ms.count(&key), 1);
        assert_eq!(ms.contains(&key), true);

        let remove_result = ms.remove(&key);
        assert!(remove_result.is_ok());
        assert_eq!(ms.dict.get(&key), None);
        assert_eq!(ms.count(&key), 0);
        assert_eq!(ms.contains(&key), false);

        let remove_result = ms.remove(&key);
        assert!(remove_result.is_err());
        assert_eq!(ms.dict.get(&key), None);
        assert_eq!(ms.count(&key), 0);
        assert_eq!(ms.contains(&key), false);
    }
}
