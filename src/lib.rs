extern crate futures;

use futures::{Async, Poll, Stream};



struct WindowVec<'a, T: 'a> {
    v: &'a mut Vec<T>,
    offset: usize,
}


impl<'a, T> WindowVec<'a, T> {
    fn new(v: &mut Vec<T>, offset: usize) -> WindowVec<T> {
        WindowVec {
            v: v,
            offset: offset,
        }
    }

    fn off(&self, i: usize) -> usize {
        let res = if self.v.len() == 0 {
            0
        } else {
            (i + self.offset) % self.v.len()
        };

        println!("  converted {} --> {} (offset {}, len {})", i, res, self.offset, self.v.len());
        res
    }

    #[inline]
    fn len(&self) -> usize {
        self.v.len()
    }

    #[inline]
    fn index(&mut self, i: usize) -> &mut T {
        let off = self.off(i);
        &mut self.v[off]
    }

    #[inline]
    fn remove(&mut self, i: usize) {
        let off = self.off(i);
        self.v.remove(off);
    }
}


/// A `MultiStream` is a `Stream` implementation that abstracts over a set of underlying `Streams`,
/// returning a single stream that contains items from every child stream.  This is very similar to
/// the `merge` function on the `Stream` type, but has support for arbitrary numbers of underlying
/// `Streams`.  It also attempts to be "fair", and as such will poll all underlying streams in
/// round-robin order.
pub struct MultiStream<T> {
    streams: Vec<T>,
    counter: usize,
}

impl<T> MultiStream<T>
    where T: Stream
{
    /// Creates a new, empty `MultiStream`.
    pub fn new() -> MultiStream<T> {
        MultiStream {
            streams: vec![],
            counter: 0,
        }
    }

    /// Adds a new `Stream` to this `MultiStream`.
    pub fn add(&mut self, v: T) {
        self.streams.push(v);
    }
}


impl<T> Stream for MultiStream<T>
    where T: Stream
{
    type Item = T::Item;
    type Error = T::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // TODO: handle 'draining' case?

        {
            let mut streams = WindowVec::new(&mut self.streams, self.counter);
            let mut i = 0;
            while i < streams.len() {
                println!("polling index: {}", i);
                let res = match streams.index(i).poll() {
                    Ok(res) => res,
                    Err(e) => return Err(e),
                };

                match res {
                    Async::Ready(Some(item)) => {
                        // Save the counter variable so we can start from here next time.
                        // TODO: this isn't right :/
                        self.counter = (self.counter + 1) % streams.len();
                        return Ok(Async::Ready(Some(item)));
                    },
                    Async::Ready(None) => {
                        // This stream is done - we remove it from our streams array entirely.  Note
                        // that we don't increment our counter here, since the item at `i + 1` just got
                        // shifted to the current element at index `i`.
                        drop(streams.remove(i));
                    },
                    Async::NotReady => {
                        // Ignore and continue onward.
                        i += 1;
                    },
                }
            }
        }

        // If we get here, there are two cases: firstly, every child-stream is finished, at which
        // case we (TODO: might?) be finished, or we have child streams, and we need to continue
        // polling in the future.
        if self.streams.len() == 0 {
            Ok(Async::Ready(None))
        } else {
            Ok(Async::NotReady)
        }
    }
}

#[macro_export]
macro_rules! multistream {
    ($($e:expr),*) => ({
        let mut _temp = $crate::MultiStream::new();
        $(_temp.add($e);)*
        _temp
    })
}


#[cfg(test)]
mod tests {
    use super::MultiStream;
    use futures::{Async, Stream};
    use futures::stream::{once, repeat};

    #[test]
    fn test_basic() {
        let one = once::<u32, ()>(Ok(1));
        let two = once(Ok(2));
        let three = once(Ok(3));

        let mut ms = MultiStream::new();
        ms.add(one); ms.add(two); ms.add(three);

        assert_eq!(ms.poll(), Ok(Async::Ready(Some(1))));
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(2))));
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(3))));
        assert_eq!(ms.poll(), Ok(Async::Ready(None)));
    }

    #[test]
    fn test_fair() {
        let one = repeat::<_, bool>(1);
        let two = repeat(2);
        let three = repeat(3);

        let mut ms = multistream![one, two, three];

        assert_eq!(ms.poll(), Ok(Async::Ready(Some(1))));
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(2))));
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(3))));
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(1))));
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(2))));
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(3))));
    }

    #[test]
    fn test_remove_finished() {
        // Test that we properly handle removing a finished stream.

        let one = repeat::<_, bool>(1).boxed();
        let two = once(Ok(2)).boxed();
        let three = repeat(3).boxed();
        let four = once(Ok(4)).boxed();

        let mut ms = multistream![one, two, three, four];

        // Initial iteration gets everything
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(1))));
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(2))));
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(3))));
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(4))));

        // Next iteration gets only repeating ones
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(1))));
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(3))));

        // Ditto
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(1))));
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(3))));
    }

    #[test]
    fn test_macro() {
        let one = once::<u32, ()>(Ok(1));
        let two = once(Ok(2));
        let three = once(Ok(3));

        let mut ms = multistream![one, two, three];

        assert_eq!(ms.poll(), Ok(Async::Ready(Some(1))));
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(2))));
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(3))));
        assert_eq!(ms.poll(), Ok(Async::Ready(None)));
    }
}
