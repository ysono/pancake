use std::iter;

pub struct IteratorCache<I, T> {
    iter: I,
    cache: Vec<T>,
}

impl<I, T> IteratorCache<I, T>
where
    I: Iterator<Item = T>,
{
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            cache: vec![],
        }
    }

    pub fn iter<'s>(&'s mut self) -> impl 's + Iterator<Item = &'s T> {
        let mut cache_i = 0;
        let iter_fn = move || {
            if cache_i < self.cache.len() {
                let ret = &(self.cache[cache_i]);
                let ret = unsafe { &*(ret as *const T) };
                cache_i += 1;
                return Some(ret);
            } else {
                match self.iter.next() {
                    Some(item) => {
                        self.cache.push(item);
                        let ret = &(self.cache[cache_i]);
                        let ret = unsafe { &*(ret as *const T) };
                        cache_i += 1;
                        return Some(ret);
                    }
                    None => return None,
                }
            }
        };
        iter::from_fn(iter_fn)
    }
}
