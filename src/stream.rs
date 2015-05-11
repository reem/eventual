use {
    receipt,
    Async,
    Pair,
    Future,
    Complete,
    Cancel,
    Receipt,
    AsyncResult,
    AsyncError
};
use super::core::{self, Core};
use std::fmt;

/*
 *
 * ===== Stream =====
 *
 */

#[must_use = "streams are lazy and do nothing unless consumed"]
pub struct Stream<'a, T: Send + 'a, E: Send + 'a> {
    core: Option<StreamCore<'a, T, E>>,
}

/// Convenience type alias for the realized head of a stream
pub type Head<'a, T, E> = Option<(T, Stream<'a, T, E>)>;

// Shorthand for the core type for Streams
pub type StreamCore<'a, T, E> = Core<'a, Head<'a, T, E>, E>;

impl<'a, T: Send + 'a, E: Send + 'a> Stream<'a, T, E> {
    /// Creates a new `Stream`, returning it with the associated `Sender`.
    pub fn pair() -> (Sender<'a, T, E>, Stream<'a, T, E>) {
        let core = Core::new();
        let stream = Stream { core: Some(core.clone()) };

        (Sender { core: Some(core) }, stream)
    }

    /// Returns a Stream that will immediately succeed with the supplied value.
    ///
    /// ```
    /// use eventual::*;
    ///
    /// let stream = Stream::<i32, &'a str>::empty();
    /// assert!(stream.iter().next().is_none());
    /// ```
    pub fn empty() -> Stream<'a, T, E> {
        Stream { core: Some(Core::with_value(Ok(None))) }
    }

    /// Asyncronously collects the items from the `Stream`, returning them sorted by order of
    /// arrival.
    pub fn collect(self) -> Future<'a, Vec<T>, E> {
        let buffer = Vec::new();
        self.reduce(buffer, |mut vec, item| { vec.push(item); return vec })
    }

    /// Synchronously iterate over the `Stream`
    pub fn iter(mut self) -> StreamIter<'a, T, E> {
        StreamIter { core: Some(core::take(&mut self.core)) }
    }

    /*
     *
     * ===== Computation Builders =====
     *
     */

    /// Sequentially yields each value to the supplied function. Returns a
    /// future representing the completion of the final yield.
    pub fn each<F: Fn(T) + Send + 'a>(self, f: F) -> Future<'a, (), E> {
        let (complete, ret) = Future::pair();

        complete.receive(move |res| {
            if let Ok(complete) = res {
                self.do_each(f, complete);
            }
        });

        ret
    }

    // Perform the iteration
    fn do_each<F: Fn(T) + Send + 'a>(self, f: F, complete: Complete<'a, (), E>) {
        self.receive(move |head| {
            match head {
                Ok(Some((v, rest))) => {
                    f(v);
                    rest.do_each(f, complete);
                }
                Ok(None) => {
                    complete.complete(());
                }
                Err(AsyncError::Failed(e)) => {
                    complete.fail(e);
                }
                _ => {}
            }
        });
    }

    /// Returns a new stream containing the values of the original stream that
    /// match the given predicate.
    pub fn filter<F: Fn(&T) -> bool + Send + 'a>(self, f: F) -> Stream<'a, T, E> {
        let (sender, stream) = Stream::pair();
        self.do_filter(f, sender);
        stream
    }

    fn do_filter<F, A>(self, f: F, sender: A)
            where F: Fn(&T) -> bool + Send + 'a,
                  A: Async<'a, Value=Sender<'a, T, E>> {

        // Wait for the consumer to express interest
        sender.receive(move |res| {
            if let Ok(sender) = res {
                self.receive(move |head| {
                    match head {
                        Ok(Some((v, rest))) => {
                            if f(&v) {
                                rest.do_filter(f, sender.send(v));
                            } else {
                                rest.do_filter(f, sender);
                            }
                        }
                        Ok(None) => {}
                        Err(AsyncError::Failed(e)) => sender.fail(e),
                        Err(AsyncError::Aborted) => sender.abort(),
                    }
                });
            }
        });
    }

    /// Returns a new stream representing the application of the specified
    /// function to each value of the original stream.
    pub fn map<F: Fn(T) -> U + Send + 'a, U: Send>(self, f: F) -> Stream<'a, U, E> {
        self.map_async(move |val| Ok(f(val)))
    }

    /// Returns a new stream representing the application of the specified
    /// function to each value of the original stream. Each iteration waits for
    /// the async result of the mapping to realize before continuing on to the
    /// next value.
    pub fn map_async<F, U>(self, action: F) -> Stream<'a, U::Value, E>
            where F: Fn(T) -> U + Send + 'a,
                  U: Async<'a, Error=E> {

        let (sender, ret) = Stream::pair();

        sender.receive(move |res| {
            if let Ok(sender) = res {
                self.do_map(sender, action);
            }
        });

        ret
    }

    fn do_map<F, U>(self, sender: Sender<'a, U::Value, E>, f: F)
            where F: Fn(T) -> U + Send + 'a,
                  U: Async<'a, Error=E> {

        self.receive(move |head| {
            match head {
                Ok(Some((v, rest))) => {
                    f(v).receive(move |res| {
                        match res {
                            Ok(val) => {
                                sender.send(val).receive(move |res| {
                                    if let Ok(sender) = res {
                                        rest.do_map(sender, f);
                                    }
                                });
                            }
                            Err(AsyncError::Failed(e)) => sender.fail(e),
                            Err(AsyncError::Aborted) => sender.abort(),
                        }
                    });
                }
                Ok(None) => {}
                Err(AsyncError::Failed(e)) => sender.fail(e),
                Err(AsyncError::Aborted) => sender.abort(),
            }
        });
    }

    /// Returns a new stream with an identical sequence of values as the
    /// original. If the original stream errors, apply the given function on
    /// the error and use the result as the error of the new stream.
    pub fn map_err<F, U>(self, f: F) -> Stream<'a, T, U>
            where F: FnOnce(E) -> U + Send + 'a,
                  U: Send + 'a {
        let (sender, stream) = Stream::pair();

        sender.receive(move |res| {
            if let Ok(sender) = res {
                self.do_map_err(sender, f);
            }
        });

        stream
    }

    fn do_map_err<F, U>(self, sender: Sender<T, U>, f: F)
            where F: FnOnce(E) -> U + Send + 'a,
                  U: Send + 'a {
        self.receive(move |res| {
            match res {
                Ok(Some((val, rest))) => {
                    sender.send(val).receive(move |res| {
                        if let Ok(sender) = res {
                            rest.do_map_err(sender, f);
                        }
                    });
                }
                Ok(None) => {}
                Err(AsyncError::Failed(e)) => sender.fail(f(e)),
                Err(AsyncError::Aborted) => sender.abort(),
            }
        });
    }

    pub fn process<F, U>(self, in_flight: usize, f: F) -> Stream<'a, U::Value, E>
            where F: Fn(T) -> U + Send + 'a,
                  U: Async<'a, Error=E> {
        use process::process;
        process(self, in_flight, f)
    }

    /// Aggregate all the values of the stream by applying the given function
    /// to each value and the result of the previous application. The first
    /// iteration is seeded with the given initial value.
    ///
    /// Returns a future that will be completed with the result of the final
    /// iteration.
    pub fn reduce<F: Fn(U, T) -> U + Send + 'a, U: Send + 'a>(self, init: U, f: F) -> Future<'a, U, E> {
        self.reduce_async(init, move |curr, val| Ok(f(curr, val)))
    }

    /// Aggregate all the values of the stream by applying the given function
    /// to each value and the realized result of the previous application. The
    /// first iteration is seeded with the given initial value.
    ///
    /// Returns a future that will be completed with the result of the final
    /// iteration.
    pub fn reduce_async<F, U, X>(self, init: X, action: F) -> Future<'a, X, E>
            // TODO: Remove X generic, blocked on rust-lang/rust#23728
            where F: Fn(X, T) -> U + Send + 'a,
                  U: Async<'a, Value=X, Error=E>,
                  X: Send + 'a {

        let (sender, ret) = Future::pair();

        sender.receive(move |res| {
            if let Ok(sender) = res {
                self.do_reduce(sender, init, action);
            }
        });

        ret
    }

    fn do_reduce<F, U>(self, complete: Complete<U::Value, E>, curr: U::Value, f: F)
            where F: Fn(U::Value, T) -> U + Send + 'a,
                  U: Async<'a, Error=E> {

        self.receive(move |head| {
            match head {
                Ok(Some((v, rest))) => {
                    f(curr, v).receive(move |res| {
                        match res {
                            Ok(curr) => rest.do_reduce(complete, curr, f),
                            Err(AsyncError::Failed(e)) => complete.fail(e),
                            Err(AsyncError::Aborted) => drop(complete),
                        }
                    });
                }
                Ok(None) => complete.complete(curr),
                Err(AsyncError::Failed(e)) => complete.fail(e),
                Err(AsyncError::Aborted) => drop(complete),
            }
        });
    }

    /// Returns a stream representing the `n` first values of the original
    /// stream.
    pub fn take(self, n: u64) -> Stream<'a, T, E> {
        let (sender, stream) = Stream::pair();

        self.do_take(n, sender);
        stream
    }

    fn do_take<A>(self, n: u64, sender: A) where A: Async<'a, Value=Sender<'a, T, E>> {
        if n == 0 {
            return;
        }

        sender.receive(move |res| {
            if let Ok(sender) = res {
                self.receive(move |res| {
                    match res {
                        Ok(Some((v, rest))) => {
                            rest.do_take(n - 1, sender.send(v));
                        }
                        Ok(None) => {}
                        Err(AsyncError::Failed(e)) => sender.fail(e),
                        Err(AsyncError::Aborted) => sender.abort(),
                    }
                });
            }
        });
    }

    pub fn take_while<F>(self, _f: F) -> Stream<'a, T, E>
            where F: Fn(&T) -> bool + Send + 'a {
        unimplemented!();
    }

    pub fn take_until<A>(self, cond: A) -> Stream<'a, T, E>
            where A: Async<'a, Error=E> {

        super::select((cond, self))
            .and_then(move |(i, (cond, stream))| {
                if i == 0 {
                    Ok(None)
                } else {
                    match stream.expect() {
                        Ok(Some((v, rest))) => {
                            Ok(Some((v, rest.take_until(cond))))
                        }
                        _ => Ok(None),
                    }
                }
            }).to_stream()
    }

    /*
     *
     * ===== Internal Helpers =====
     *
     */

    fn from_core(core: StreamCore<'a, T, E>) -> Stream<'a, T, E> {
        Stream { core: Some(core) }
    }
}

impl<'a, T: Send + 'a, E: Send + 'a> Async<'a> for Stream<'a, T, E> {
    type Value = Head<'a, T, E>;
    type Error = E;
    type Cancel = Receipt<'a, Stream<'a, T, E>>;

    fn is_ready(&self) -> bool {
        core::get(&self.core).consumer_is_ready()
    }

    fn is_err(&self) -> bool {
        core::get(&self.core).consumer_is_err()
    }

    fn poll(mut self) -> Result<AsyncResult<Head<'a, T, E>, E>, Stream<'a, T, E>> {
        let mut core = core::take(&mut self.core);

        match core.consumer_poll() {
            Some(res) => Ok(res),
            None => Err(Stream { core: Some(core) })
        }
    }

    fn ready<F: FnOnce(Stream<'a, T, E>) + Send + 'a>(mut self, f: F) -> Receipt<'a, Stream<'a, T, E>> {
        let core = core::take(&mut self.core);

        match core.consumer_ready(move |core| f(Stream::from_core(core))) {
            Some(count) => receipt::new(core, count),
            None => receipt::none(),
        }
    }

    fn await(mut self) -> AsyncResult<Head<'a, T, E>, E> {
        core::take(&mut self.core).consumer_await()
    }
}

impl<'a, T: Send + 'a, E: Send + 'a> Pair for Stream<'a, T, E> {
    type Tx = Sender<'a, T, E>;

    fn pair() -> (Sender<'a, T, E>, Stream<'a, T, E>) {
        Stream::pair()
    }
}

impl<'a, T: Send + 'a, E: Send + 'a> fmt::Debug for Stream<'a, T, E> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Stream<?>")
    }
}

impl<'a, T: Send + 'a, E: Send + 'a> Drop for Stream<'a, T, E> {
    fn drop(&mut self) {
        if self.core.is_some() {
            core::take(&mut self.core).cancel();
        }
    }
}

impl<'a, T: Send + 'a, E: Send + 'a> Cancel<Stream<'a, T, E>> for Receipt<'a, Stream<'a, T, E>> {
    fn cancel(self) -> Option<Stream<'a, T, E>> {
        let (core, count) = receipt::parts(self);

        if !core.is_some() {
            return None;
        }

        if core::get(&core).consumer_ready_cancel(count) {
            return Some(Stream { core: core });
        }

        None
    }
}

/*
 *
 * ===== Sender =====
 *
 */

/// The sending half of `Stream::pair()`. Can only be owned by a single task at
/// a time.
pub struct Sender<'a, T: Send + 'a, E: Send + 'a> {
    core: Option<StreamCore<'a, T, E>>,
}

impl<'a, T: Send, E: Send> Sender<'a, T, E> {

    // TODO: This fn would be nice to have, but isn't possible with the current
    // implementation of `send()` which requires the value slot of `Stream` to
    // always be available.
    //
    // I initially thought that `try_send` would be possible to implement if
    // the consumer was currently waiting for a value, but this doesn't work in
    // sync mode since the value is temporarily stored in the stream's value
    // slot and there would be a potential race condition.
    //
    // pub fn try_send(&self, val: T) -> Result<(), T>;

    /// Attempts to send a value to its `Stream`. Consumes self and returns a
    /// future representing the operation completing successfully and interest
    /// in the next value being expressed.
    pub fn send(mut self, val: T) -> BusySender<'a, T, E> {
        let mut core = core::take(&mut self.core);
        let val = Some((val, Stream { core: Some(core.clone()) }));

        // Complete the value
        core.complete(Ok(val), false);
        BusySender { core: Some(core) }
    }

    /// Terminated the stream with the given error.
    pub fn fail(mut self, err: E) {
        core::take(&mut self.core).complete(Err(AsyncError::failed(err)), true);
    }

    /// Fails the paired `Stream` with a cancellation error. This will
    /// eventually go away when carllerche/syncbox#10 lands. It is currently
    /// needed to keep the state correct (see async::sequence)
    pub fn abort(mut self) {
        core::take(&mut self.core).complete(Err(AsyncError::aborted()), true);
    }

    /// Send all the values in the given source
    pub fn send_all<S: Source<'a, Value=T>>(self, src: S) -> Future<'a, Self, (S::Error, Self)> {
        src.send_all(self)
    }

    /*
     *
     * ===== Internal Helpers =====
     *
     */

    fn from_core(core: StreamCore<'a, T, E>) -> Sender<'a, T, E> {
        Sender { core: Some(core) }
    }
}

impl<'a, T: Send + 'a, E: Send + 'a> Async<'a> for Sender<'a, T, E> {
    type Value = Sender<'a, T, E>;
    type Error = ();
    type Cancel = Receipt<'a, Sender<'a, T, E>>;

    fn is_ready(&self) -> bool {
        core::get(&self.core).producer_is_ready()
    }

    fn is_err(&self) -> bool {
        core::get(&self.core).producer_is_err()
    }

    fn poll(mut self) -> Result<AsyncResult<Sender<'a, T, E>, ()>, Sender<'a, T, E>> {
        debug!("Sender::poll; is_ready={}", self.is_ready());

        let core = core::take(&mut self.core);

        match core.producer_poll() {
            Some(res) => Ok(res.map(Sender::from_core)),
            None => Err(Sender { core: Some(core) })
        }
    }

    fn ready<F: FnOnce(Sender<'a, T, E>) + Send + 'a>(mut self, f: F) -> Receipt<'a, Sender<'a, T, E>> {
        core::take(&mut self.core).producer_ready(move |core| {
            f(Sender::from_core(core))
        });

        receipt::none()
    }
}

impl<'a, T: Send + 'a, E: Send + 'a> Drop for Sender<'a, T, E> {
    fn drop(&mut self) {
        if self.core.is_some() {
            debug!("Sender::drop(); cancelling future");
            // Get the core
            let mut core = core::take(&mut self.core);
            core.complete(Ok(None), true);
        }
    }
}

/*
 *
 * ===== BusySender =====
 *
 */

pub struct BusySender<'a, T: Send + 'a, E: Send + 'a> {
    core: Option<StreamCore<'a, T, E>>,
}

impl<'a, T: Send, E: Send> BusySender<'a, T, E> {
    /*
     *
     * ===== Internal Helpers =====
     *
     */

    fn from_core(core: StreamCore<'a, T, E>) -> BusySender<'a, T, E> {
        BusySender { core: Some(core) }
    }
}

impl<'a, T: Send + 'a, E: Send + 'a> Async<'a> for BusySender<'a, T, E> {
    type Value = Sender<'a, T, E>;
    type Error = ();
    type Cancel = Receipt<'a, BusySender<'a, T, E>>;

    fn is_ready(&self) -> bool {
        core::get(&self.core).consumer_is_ready()
    }

    fn is_err(&self) -> bool {
        core::get(&self.core).consumer_is_err()
    }

    fn poll(mut self) -> Result<AsyncResult<Sender<'a, T, E>, ()>, BusySender<'a, T, E>> {
        debug!("Sender::poll; is_ready={}", self.is_ready());

        let core = core::take(&mut self.core);

        match core.producer_poll() {
            Some(res) => Ok(res.map(Sender::from_core)),
            None => Err(BusySender { core: Some(core) })
        }
    }

    fn ready<F: FnOnce(BusySender<'a, T, E>) + Send + 'a>(mut self, f: F) -> Receipt<'a, BusySender<'a, T, E>> {
        core::take(&mut self.core).producer_ready(move |core| {
            f(BusySender::from_core(core))
        });

        receipt::none()
    }
}

impl<'a, T: Send + 'a, E: Send + 'a> Drop for BusySender<'a, T, E> {
    fn drop(&mut self) {
        if self.core.is_some() {
            let core = core::take(&mut self.core);

            core.producer_ready(|mut core| {
                if core.producer_is_ready() {
                    core.complete(Ok(None), true);
                }
            });
        }
    }
}

impl<'a, T: Send + 'a, E: Send + 'a> fmt::Debug for Sender<'a, T, E> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Sender<?>")
    }
}

/*
 *
 * ===== Sink / Source =====
 *
 */

pub trait Source<'a> {
    type Value: Send + 'a;
    type Error: Send + 'a;

    fn send_all<E2: Send + 'a>(self, sender: Sender<'a, Self::Value, E2>) ->
        Future<'a, Sender<'a, Self::Value, E2>, (Self::Error, Sender<'a, Self::Value, E2>)>;
}

impl<'a, T: Send + 'a, E: Send + 'a> Source<'a> for Future<'a, T, E> {
    type Value = T;
    type Error = E;

    fn send_all<E2: Send>(self, sender: Sender<'a, T, E2>)
        -> Future<'a, Sender<'a, T, E2>, (E, Sender<'a, T, E2>)> {
        let (tx, rx) = Future::pair();

        self.receive(move |res| {
            match res {
                Ok(val) => {
                    sender.send(val).receive(move |res| {
                        if let Ok(sender) = res {
                            tx.complete(sender);
                        }
                    });
                }
                Err(AsyncError::Failed(e)) => tx.fail((e, sender)),
                Err(AsyncError::Aborted) => drop(tx),
            }
        });

        rx
    }
}

impl<'a, T: Send + 'a, E: Send + 'a> Source<'a> for Stream<'a, T, E> {
    type Value = T;
    type Error = E;

    fn send_all<E2: Send>(self, sender: Sender<'a, T, E2>)
        -> Future<'a, Sender<'a, T, E2>, (E, Sender<'a, T, E2>)> {
        let (tx, rx) = Future::pair();
        send_stream(self, sender, tx);
        rx
    }
}

// Perform the send
fn send_stream<'a, T: Send + 'a, E: Send + 'a, E2: Send + 'a>(
    src: Stream<'a, T, E>,
    dst: Sender<'a, T, E2>,
    complete: Complete<'a, Sender<'a, T, E2>, (E, Sender<'a, T, E2>)>) {

    src.receive(move |res| {
        match res {
            Ok(Some((val, rest))) => {
                dst.send(val).receive(move |res| {
                    if let Ok(dst) = res {
                        send_stream(rest, dst, complete);
                    }
                });
            }
            Ok(None) => complete.complete(dst),
            Err(AsyncError::Failed(e)) => complete.fail((e, dst)),
            Err(AsyncError::Aborted) => drop(complete),
        }
    })
}

/*
 *
 * ===== Receipt<Sender<'a, T, E>> =====
 *
 */

impl<'a, T: Send + 'a, E: Send + 'a> Cancel<Sender<'a, T, E>> for Receipt<'a, Sender<'a, T, E>> {
    fn cancel(self) -> Option<Sender<'a, T, E>> {
        None
    }
}

/*
 *
 * ===== Receipt<BusySender<'a, T, E>> =====
 *
 */

impl<'a, T: Send + 'a, E: Send + 'a> Cancel<BusySender<'a, T, E>> for Receipt<'a, BusySender<'a, T, E>> {
    fn cancel(self) -> Option<BusySender<'a, T, E>> {
        None
    }
}

/*
 *
 * ===== StreamIter =====
 *
 */

pub struct StreamIter<'a, T: Send + 'a, E: Send + 'a> {
    core: Option<StreamCore<'a, T, E>>,
}

impl<'a, T: Send + 'a, E: Send + 'a> Iterator for StreamIter<'a, T, E> {
    type Item = T;

    fn next(&mut self) -> Option<T> {
        use std::mem;

        match core::get_mut(&mut self.core).consumer_await() {
            Ok(Some((h, mut rest))) => {
                mem::replace(&mut self.core, Some(core::take(&mut rest.core)));
                Some(h)
            }
            Ok(None) => {
                let _ = core::take(&mut self.core);
                None
            }
            Err(_) => unimplemented!(),
        }
    }
}

impl<'a, T: Send + 'a, E: Send + 'a> Drop for StreamIter<'a, T, E> {
    fn drop(&mut self) {
        if self.core.is_some() {
            core::take(&mut self.core).cancel();
        }
    }
}

pub fn from_core<'a, T: Send + 'a, E: Send + 'a>(core: StreamCore<'a, T, E>) -> Stream<'a, T, E> {
    Stream { core: Some(core) }
}
