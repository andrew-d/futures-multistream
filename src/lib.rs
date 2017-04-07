extern crate futures;

use futures::{Async, Poll, Stream};


/// A `MultiStream` is a `Stream` implementation that abstracts over a set of underlying `Streams`,
/// returning a single stream that contains items from every child stream.  This is very similar to
/// the `merge` function on the `Stream` type, but has support for arbitrary numbers of underlying
/// `Streams`.  It also attempts to be "fair", such that every time an underlying `Stream` returns
/// an item, it is swapped to the end of the internal list to give other `Stream`s a chance to make
/// progress.
///
/// NOTE: It is still possible to end up in a bad state, such that items `n` and `end` are each
/// being swapped with each other, starving streams in the range `[n+1, end)`.  This will be fixed
/// eventually.
pub struct MultiStream<T> {
    streams: Vec<T>,
}

impl<T> MultiStream<T>
    where T: Stream
{
    /// Creates a new, empty `MultiStream`.
    pub fn new() -> MultiStream<T> {
        MultiStream {
            streams: vec![],
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

        let mut i = 0;
        while i < self.streams.len() {
            // TODO: do we return future errors?  swap?
            let res = match self.streams[i].poll() {
                Ok(res) => res,
                Err(e) => return Err(e),
            };

            match res {
                Async::Ready(Some(item)) => {
                    // We have an item.  Swap the current stream to the end (for fairness), and
                    // then return the current one.
                    let end = self.streams.len() - 1;
                    self.streams.swap(i, end);

                    return Ok(Async::Ready(Some(item)));
                },
                Async::Ready(None) => {
                    // This stream is done - we remove it from our streams array entirely.  Note
                    // that we don't increment here, since the item at `i + 1` just got shifted to
                    // the current element at index `i`.
                    drop(self.streams.remove(i));
                },
                Async::NotReady => {
                    // Ignore and continue onward.
                    i += 1
                },
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
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(3))));   // this is swapped to the beginning
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(2))));
        assert_eq!(ms.poll(), Ok(Async::Ready(None)));
    }
}
