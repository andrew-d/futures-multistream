extern crate futures;

use futures::{Async, Poll, Sink, Stream};
use futures::sink::BoxSink;
use futures::stream::BoxStream;
use futures::sync::mpsc::{channel, SendError};


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
    newstream: Option<BoxStream<T, ()>>,
}

impl<T> MultiStream<T>
    where T: Stream + Send + 'static
{
    /// Creates a new, empty `MultiStream`, along with a `Stream` that allows adding more child
    /// `Stream`s to this `MultiStream`.
    pub fn new() -> (MultiStream<T>, BoxSink<T, SendError<T>>) {
        let (send, recv) = channel(10);     // TODO: configurable?

        (MultiStream {
            streams: vec![],
            newstream: Some(recv.boxed()),
        }, Box::new(send))
    }
}


impl<T> Stream for MultiStream<T>
    where T: Stream + Send
{
    type Item = T::Item;
    type Error = T::Error;

    fn poll(&mut self) -> Poll<Option<Self::Item>, Self::Error> {
        // Read all new streams and add them to the poll set.
        let mut finished = false;
        if let Some(ref mut stream) = self.newstream {
            loop {
                match stream.poll() {
                    Ok(Async::Ready(Some(item))) => {
                        self.streams.push(item);
                    },
                    Ok(Async::Ready(None)) => {
                        finished = true;
                        break;
                    },
                    Ok(Async::NotReady) => break,
                    Err(_) => {
                        // TODO: figure out what we do here?
                        break;
                    },
                };
            }
        }

        // If our new-child-Stream is finished, then we remove it so we never poll it again.
        if finished {
            self.newstream = None;
        }

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

/*
#[macro_export]
macro_rules! multistream {
    ($($e:expr),*) => ({
        let (mut _tempms, mut _tempstr) = $crate::MultiStream::new();
        $(_tempstr.send($e).unwrap();)*
        _temp
    })
}
*/


#[cfg(test)]
mod tests {
    use super::MultiStream;
    use std::sync::atomic::{AtomicBool, Ordering};
    use futures::{Async, Future, Sink, Stream};
    use futures::stream::{BoxStream, iter, once, repeat};

    fn vec_stream<T>(v: Vec<T>) -> BoxStream<T, ()>
        where T: Send + 'static,
    {
        Box::new(iter(v.into_iter().map(|x| Ok(x))))
    }

    #[test]
    fn test_basic() {
        let one = once::<u32, ()>(Ok(1));
        let two = once(Ok(2));
        let three = once(Ok(3));

        let (mut ms, sender) = MultiStream::new();

        // NOTE: the `unreachable!` is so we satisfy the type bound:
        //      Self::SinkError: From<S::Error>
        let sent = sender.send_all(
            vec_stream(vec![
                one, two, three,
            ]).map_err(|_| panic!())
        );

        let run = AtomicBool::new(false);
        let test = sent.then(|_| {
            assert_eq!(ms.poll(), Ok(Async::Ready(Some(1))));
            assert_eq!(ms.poll(), Ok(Async::Ready(Some(3))));   // this is swapped to the beginning
            assert_eq!(ms.poll(), Ok(Async::Ready(Some(2))));
            assert_eq!(ms.poll(), Ok(Async::Ready(None)));
            run.store(true, Ordering::SeqCst);

            Ok::<(), ()>(())
        });

        // Complete the above future (which runs our assertions).
        test.wait().unwrap();

        // We assert here that we actually ran our test (i.e. nothing broke that would cause the
        // above closure/future to not run).
        assert!(run.load(Ordering::SeqCst));
    }

    /*
    #[test]
    fn test_macro() {
        let one = once::<u32, ()>(Ok(1));
        let two = once(Ok(2));
        let three = once(Ok(3));

        let mut ms = multistream![one, two, three];

        assert_eq!(ms.poll(), Ok(Async::Ready(Some(1))));
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(3))));   // this is swapped to the beginning
        assert_eq!(ms.poll(), Ok(Async::Ready(Some(2))));
        assert_eq!(ms.poll(), Ok(Async::Ready(None)));
    }
    */
}
