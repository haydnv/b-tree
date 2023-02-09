use std::cmp::Ordering;
use std::ops::Bound;

use collate::Collate;

use super::range::Range;

trait CollateRange: Collate
where
    <Self as Collate>::Value: PartialEq,
{
    /// Given a collection of slices, return the start and end indices which match the given range.
    fn bisect<V: AsRef<[Self::Value]>>(
        &self,
        slice: &[V],
        range: &Range<Self::Value>,
    ) -> (usize, usize) {
        debug_assert!(self.is_sorted(slice));

        let left = bisect_left(slice, |key| self.compare_range(key, range));
        let right = bisect_right(slice, |key| self.compare_range(key, range));
        (left, right)
    }

    /// Given a collection of slices, return the leftmost insert point matching the given key.
    fn bisect_left<V: AsRef<[Self::Value]>>(&self, slice: &[V], key: &[Self::Value]) -> usize {
        debug_assert!(self.is_sorted(slice));

        if slice.as_ref().is_empty() || key.as_ref().is_empty() {
            0
        } else {
            bisect_left(slice, |at| self.compare_slice(at, key))
        }
    }

    /// Given a collection of slices, return the rightmost insert point matching the given key.
    fn bisect_right<V: AsRef<[Self::Value]>>(&self, slice: &[V], key: &[Self::Value]) -> usize {
        debug_assert!(self.is_sorted(slice));
        let slice = slice.as_ref();

        if slice.is_empty() {
            0
        } else if key.is_empty() {
            slice.len()
        } else {
            bisect_right(slice, |at| self.compare_slice(at, key))
        }
    }

    /// Returns the ordering of the given key relative to the given range.
    fn compare_range(&self, key: &[Self::Value], range: &Range<Self::Value>) -> Ordering {
        use Bound::*;
        use Ordering::*;

        if !range.prefix().is_empty() {
            let prefix_rel = self.compare_slice(key, range.prefix());
            if prefix_rel != Equal || key.len() < range.len() {
                return prefix_rel;
            }
        }

        if !range.has_bounds() {
            return Equal;
        }

        let target = &key[range.prefix().len()];

        match range.start() {
            Unbounded => {}
            Included(value) => match self.compare(target, value) {
                Less => return Less,
                _ => {}
            },
            Excluded(value) => match self.compare(target, value) {
                Less | Equal => return Less,
                _ => {}
            },
        }

        match range.end() {
            Unbounded => {}
            Included(value) => match self.compare(target, value) {
                Greater => return Greater,
                _ => {}
            },
            Excluded(value) => match self.compare(target, value) {
                Greater | Equal => return Greater,
                _ => {}
            },
        }

        Equal
    }

    /// Returns the relative ordering of the `left` slice with respect to `right`.
    fn compare_slice<L: AsRef<[Self::Value]>, R: AsRef<[Self::Value]>>(
        &self,
        left: L,
        right: R,
    ) -> Ordering {
        let left = left.as_ref();
        let right = right.as_ref();

        use Ordering::*;

        for i in 0..Ord::min(left.len(), right.len()) {
            match self.compare(&left[i], &right[i]) {
                Equal => {}
                rel => return rel,
            };
        }

        if left.is_empty() && !right.is_empty() {
            Less
        } else if !left.is_empty() && right.is_empty() {
            Greater
        } else {
            Equal
        }
    }

    /// Returns `true` if the given slice is in sorted order.
    fn is_sorted<V: AsRef<[Self::Value]>>(&self, slice: &[V]) -> bool {
        if slice.len() < 2 {
            return true;
        }

        let order = self.compare_slice(slice[1].as_ref(), slice[0].as_ref());
        for i in 1..slice.len() {
            match self.compare_slice(slice[i].as_ref(), slice[i - 1].as_ref()) {
                Ordering::Equal => {}
                rel if rel == order => {}
                _rel => return false,
            }
        }

        true
    }
}

impl<T: Collate> CollateRange for T where <T as Collate>::Value: PartialEq {}

fn bisect_left<'a, V: 'a, W: AsRef<[V]>, F: Fn(&'a [V]) -> Ordering>(
    slice: &'a [W],
    cmp: F,
) -> usize {
    let mut start = 0;
    let mut end = slice.len();

    while start < end {
        let mid = (start + end) / 2;

        if cmp(slice[mid].as_ref()) == Ordering::Less {
            start = mid + 1;
        } else {
            end = mid;
        }
    }

    start
}

fn bisect_right<'a, V: 'a, W: AsRef<[V]>, F: Fn(&'a [V]) -> Ordering>(
    slice: &'a [W],
    cmp: F,
) -> usize {
    let mut start = 0;
    let mut end = slice.len();

    while start < end {
        let mid = (start + end) / 2;

        if cmp(slice[mid].as_ref()) == Ordering::Greater {
            end = mid;
        } else {
            start = mid + 1;
        }
    }

    end
}

#[cfg(test)]
mod tests {
    use std::ops::Deref;

    use collate::Collator;

    use super::*;

    struct Key {
        inner: Vec<i32>,
    }

    impl Deref for Key {
        type Target = [i32];

        fn deref(&self) -> &[i32] {
            &self.inner
        }
    }

    impl AsRef<[i32]> for Key {
        fn as_ref(&self) -> &[i32] {
            self.deref()
        }
    }

    struct Block {
        keys: Vec<Key>,
    }

    impl Deref for Block {
        type Target = [Key];

        fn deref(&self) -> &[Key] {
            &self.keys
        }
    }

    impl AsRef<[Key]> for Block {
        fn as_ref(&self) -> &[Key] {
            self.deref()
        }
    }

    #[test]
    fn test_bisect() {
        let block = Block {
            keys: vec![
                Key {
                    inner: vec![0, -1, 1],
                },
                Key {
                    inner: vec![1, 0, 2, 2],
                },
                Key {
                    inner: vec![1, 1, 0],
                },
                Key {
                    inner: vec![1, 1, 0],
                },
                Key {
                    inner: vec![2, 0, -1],
                },
                Key { inner: vec![2, 1] },
            ],
        };

        let collator = Collator::<i32>::default();
        assert!(collator.is_sorted(&block));

        assert_eq!(collator.bisect_left(&block, &[0, 0, 0]), 1);
        assert_eq!(collator.bisect_right(&block, &[0, 0, 0]), 1);

        assert_eq!(collator.bisect_left(&block, &[0]), 0);
        assert_eq!(collator.bisect_right(&block, &[0]), 1);

        assert_eq!(collator.bisect_left(&block, &[1]), 1);
        assert_eq!(collator.bisect_right(&block, &[1]), 4);

        assert_eq!(collator.bisect_left(&block, &[1, 1, 0]), 2);
        assert_eq!(collator.bisect_right(&block, &[1, 1, 0]), 4);

        assert_eq!(collator.bisect_left(&block, &[1, 1, 0, -1]), 2);
        assert_eq!(collator.bisect_right(&block, &[1, 1, 0, -1]), 4);
        assert_eq!(collator.bisect_right(&block, &[1, 1, 0, 1]), 4);

        assert_eq!(collator.bisect_left(&block, &[3]), 6);
        assert_eq!(collator.bisect_right(&block, &[3]), 6);
    }

    #[test]
    fn test_range() {
        // TODO: why do some of these asserts fail?
        let block = Block {
            keys: vec![
                Key {
                    inner: vec![0, -1, 1],
                },
                Key {
                    inner: vec![1, -1, 2],
                },
                Key {
                    inner: vec![1, 0, -1],
                },
                Key {
                    inner: vec![1, 0, 2, 2],
                },
                Key {
                    inner: vec![1, 1, 0],
                },
                Key {
                    inner: vec![1, 1, 0],
                },
                Key {
                    inner: vec![1, 2, 1],
                },
                Key { inner: vec![2, 1] },
                Key { inner: vec![3, 1] },
            ],
        };

        let collator = Collator::<i32>::default();
        assert!(collator.is_sorted(&block));

        // let range = Range::from(([], 0..1));
        // assert_eq!(collator.bisect(&block, &range), (0, 1));

        // let range = Range::from(([1], 0..1));
        // assert_eq!(collator.bisect(&block, &range), (2, 4));

        assert_eq!(collator.bisect(&block, &Range::default()), (0, block.len()));
    }

    #[test]
    fn test_range_contains() {
        let outer = Range::with_prefix(vec![1]);
        let inner = Range::with_prefix(vec![1, 2]);
        assert!(outer.contains(&inner, &Collator::default()));
        assert!(!inner.contains(&outer, &Collator::default()));

        let outer = Range::with_prefix(vec![1, 2]);
        let inner = Range::new(vec![1, 2], 1..3);
        assert!(outer.contains(&inner, &Collator::default()));
        assert!(!inner.contains(&outer, &Collator::default()));

        let outer = Range::with_prefix(vec![1, 2, 2]);
        let inner = Range::new(vec![1, 2], 1..2);
        assert!(!outer.contains(&inner, &Collator::default()));
        assert!(!inner.contains(&outer, &Collator::default()));

        let outer = Range::new(vec![1, 2], 3..4);
        let inner = Range::with_prefix(vec![1, 2, 3]);
        assert!(outer.contains(&inner, &Collator::default()));
        assert!(!inner.contains(&outer, &Collator::default()));
    }
}
