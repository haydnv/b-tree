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
        block: &[V],
        range: &Range<Self::Value>,
    ) -> (usize, usize);

    /// Given a collection of slices, return the leftmost insert point matching the given key.
    fn bisect_left<V: AsRef<[Self::Value]>>(&self, block: &[V], key: &[Self::Value]) -> usize;

    /// Given a collection of slices, return the rightmost insert point matching the given key.
    fn bisect_right<V: AsRef<[Self::Value]>>(&self, block: &[V], key: &[Self::Value]) -> usize;

    /// Returns the ordering of the given key relative to the given range.
    fn compare_range(&self, key: &[Self::Value], range: &Range<Self::Value>) -> Ordering;

    /// Returns the relative ordering of the `left` slice with respect to `right`.
    fn compare_slice(&self, left: &[Self::Value], right: &[Self::Value]) -> Ordering;

    /// Returns `true` if the given block is in sorted order.
    fn is_sorted<V: AsRef<[Self::Value]>>(&self, block: &[V]) -> bool;
}

impl<T: Collate> CollateRange for T
where
    <T as Collate>::Value: PartialEq,
{
    fn bisect<V: AsRef<[Self::Value]>>(
        &self,
        block: &[V],
        range: &Range<Self::Value>,
    ) -> (usize, usize) {
        debug_assert!(self.is_sorted(block));

        let left = bisect_left(block, |key| self.compare_range(key, range));
        let right = bisect_right(block, |key| self.compare_range(key, range));
        (left, right)
    }

    fn bisect_left<V: AsRef<[Self::Value]>>(&self, block: &[V], key: &[Self::Value]) -> usize {
        debug_assert!(self.is_sorted(block));

        if block.as_ref().is_empty() || key.as_ref().is_empty() {
            0
        } else {
            bisect_left(block, |at| self.compare_slice(at, key))
        }
    }

    fn bisect_right<V: AsRef<[Self::Value]>>(&self, block: &[V], key: &[Self::Value]) -> usize {
        debug_assert!(self.is_sorted(block));
        let slice = block.as_ref();

        if slice.is_empty() {
            0
        } else if key.is_empty() {
            slice.len()
        } else {
            bisect_right(slice, |at| self.compare_slice(at, key))
        }
    }

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

    fn compare_slice(&self, left: &[Self::Value], right: &[Self::Value]) -> Ordering {
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

fn bisect_left<'a, V: 'a, W: AsRef<[V]>, F: Fn(&'a [V]) -> Ordering>(
    slice: &'a [W],
    cmp: F,
) -> usize {
    let mut start = 0;
    let mut end = slice.len();

    while start < end {
        let mid = (start + end) >> 1;

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
        let mid = (start + end) >> 1;

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
    use collate::Collator;

    use super::*;

    #[test]
    fn test_bisect() {
        let block = vec![
            vec![0, -1, 1],
            vec![1, 0, 2, 2],
            vec![1, 1, 0],
            vec![1, 1, 0],
            vec![2, 0, -1],
            vec![2, 1],
        ];

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
        let block = vec![
            vec![0, -1, 1],
            vec![1, -1, 2],
            vec![1, 0, -1],
            vec![1, 0, 2, 2],
            vec![1, 1, 0],
            vec![1, 1, 0],
            vec![1, 2, 1],
            vec![2, 1],
            vec![3, 1],
        ];

        let collator = Collator::<i32>::default();
        assert!(collator.is_sorted(&block));

        let range = Range::from(([], 0..1));
        assert_eq!(collator.bisect(&block, &range), (0, 1));

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
