use super::{Async, Future, Complete, AsyncError};
use std::cell::UnsafeCell;
use std::sync::Arc;
use std::sync::atomic::{self, AtomicIsize};
use std::sync::atomic::Ordering;

pub fn join<'a, J: Join<'a, T, E>, T: Send + 'a, E: Send + 'a>(asyncs: J) -> Future<'a, T, E> {
    let (complete, future) = Future::pair();

    // Don't do any work until the consumer registers interest in the completed
    // value.
    complete.receive(move |res| {
        if let Ok(complete) = res {
            asyncs.join(complete);
        }
    });

    future
}

pub trait Join<'a, T: Send + 'a, E: Send + 'a>: Send {
    fn join(self, complete: Complete<'a, T, E>);
}

/// Stores the values as they are completed, before the join succeeds
///
trait Partial<R> {
    fn consume(&mut self) -> R;
}

/// In progress completed values for Vec
///
impl<T> Partial<Vec<T>> for Vec<Option<T>> {
    fn consume(&mut self) -> Vec<T> {
        let mut ret = Vec::with_capacity(self.len());

        for i in 0..self.len() {
            ret.push(self[i].take().unwrap())
        }

        ret
    }
}

/// In progress completed values for 2-tuples
///
impl<T1, T2> Partial<(T1, T2)> for (Option<T1>, Option<T2>) {
    fn consume(&mut self) -> (T1, T2) {
        (self.0.take().unwrap(), self.1.take().unwrap())
    }
}

/// In progress completed values for 3-tuples
///
impl<T1, T2, T3> Partial<(T1, T2, T3)> for (Option<T1>, Option<T2>, Option<T3>) {
    fn consume(&mut self) -> (T1, T2, T3) {
        (self.0.take().unwrap(), self.1.take().unwrap(), self.2.take().unwrap())
    }
}

/// Join in progress state
///
struct Progress<'a, P: Partial<R>, R: Send + 'a, E: Send + 'a> {
    inner: Arc<UnsafeCell<ProgressInner<'a, P, R, E>>>,
}

unsafe impl<'a, P: Partial<R>, R: Send, E: Send> Sync for Progress<'a, P, R, E> {}
unsafe impl<'a, P: Partial<R>, R: Send, E: Send> Send for Progress<'a, P, R, E> {}

impl<'a, P: Partial<R>, R: Send, E: Send> Progress<'a, P, R, E> {
    fn new(vals: P, complete: Complete<'a, R, E>, remaining: isize) -> Progress<'a, P, R, E> {
        let inner = Arc::new(UnsafeCell::new(ProgressInner {
            vals: vals,
            complete: Some(complete),
            remaining: AtomicIsize::new(remaining),
        }));

        Progress { inner: inner }
    }

    fn succeed(&self) {
        let complete = self.inner_mut().complete.take()
            .expect("complete already consumed");

        // Set an acquire fence to make sure that all values have been acquired
        atomic::fence(Ordering::Acquire);

        debug!("completing join");
        complete.complete(self.inner_mut().vals.consume());
    }

    fn fail(&self, err: AsyncError<E>) {
        if self.inner().remaining.swap(-1, Ordering::Relaxed) > 0 {
            let complete = self.inner_mut().complete.take()
                .expect("complete already consumed");

            // If not an execution error, it is a cancellation error, in which
            // case, our complete will go out of scope and propagate up a
            // cancellation.
            if let AsyncError::Failed(e) = err {
                complete.fail(e);
            }
        }
    }

    fn vals_mut<'r>(&'r self) -> &'r mut P {
        &mut self.inner_mut().vals
    }

    fn dec(&self) -> isize {
        self.inner().remaining.fetch_sub(1, Ordering::Release) - 1
    }

    fn inner(&self) -> &ProgressInner<P, R, E> {
        use std::mem;
        unsafe { mem::transmute(self.inner.get()) }
    }

    fn inner_mut(&self) -> &mut ProgressInner<P, R, E> {
        use std::mem;
        unsafe { mem::transmute(self.inner.get()) }
    }
}

impl<'a, P: Partial<R>, R: Send + 'a, E: Send + 'a> Clone for Progress<'a, P, R, E> {
    fn clone(&self) -> Progress<'a, P, R, E> {
        Progress { inner: self.inner.clone() }
    }
}

struct ProgressInner<'a, P: Partial<R>, R: Send + 'a, E: Send + 'a> {
    vals: P,
    complete: Option<Complete<'a, R, E>>,
    remaining: AtomicIsize,
}

macro_rules! expr {
    ($e: expr) => { $e };
}

macro_rules! component {
    ($async:ident, $progress:ident, $id:tt) => {{
        let $progress = $progress.clone();

        $async.receive(move |res| {
            debug!(concat!("dependent future complete; id=", $id, "; success={}"), res.is_ok());

            // Get a pointer to the value staging area (Option<T>). Values will
            // be stored here until the join is complete
            let slot = expr!(&mut $progress.vals_mut().$id);

            match res {
                Ok(v) => {
                    // Set the value
                    *slot = Some(v);

                    // Track that the value has been received
                    if $progress.dec() == 0 {
                        debug!("last future completed -- completing join");
                        // If all values have been received, successfully
                        // complete the future
                        $progress.succeed();
                    }
                }
                Err(e) => {
                    $progress.fail(e);
                }
            }
        });
    }};
}

/*
 *
 * ===== Join for Vec =====
 *
 */

impl<'a, A: Async<'a>> Join<'a, Vec<A::Value>, A::Error> for Vec<A> {
    fn join(self, complete: Complete<'a, Vec<A::Value>, A::Error>) {
        let mut vec = Vec::with_capacity(self.len());

        for _ in 0..self.len() {
            vec.push(None);
        }

        // Setup the in-progress state
        let progress = Progress::new(
            vec,
            complete,
            self.len() as isize);

        for (i, async) in self.into_iter().enumerate() {
            let progress = progress.clone();

            async.receive(move |res| {
                debug!(concat!("dependent future complete; id={}; success={}"), i, res.is_ok());

                // Get a pointer to the value staging area (Option<T>). Values will
                // be stored here until the join is complete

                let slot = &mut progress.vals_mut()[i];

                match res {
                    Ok(v) => {
                        // Set the value
                        *slot = Some(v);

                        // Track that the value has been received
                        if progress.dec() == 0 {
                            debug!("last future completed -- completing join");
                            // If all values have been received, successfully
                            // complete the future
                            progress.succeed();
                        }
                    }
                    Err(e) => {
                        progress.fail(e);
                    }
                }
            });
        }
    }
}

/*
 *
 * ===== Join for Tuples =====
 *
 */

impl<'a, A1: Async<'a, Error=E>, A2: Async<'a, Error=E>, E> Join<'a, (A1::Value, A2::Value), E> for (A1, A2)
        where E: Send + 'a,
              A1::Value: Send,
              A2::Value: Send {

    fn join(self, complete: Complete<'a, (<A1 as Async>::Value, <A2 as Async>::Value), E>) {
        let (a1, a2) = self;
        let p = Progress::new((None, None), complete, 2);

        component!(a1, p, 0);
        component!(a2, p, 1);
    }
}

impl<'a, A1: Async<'a, Error=E>, A2: Async<'a, Error=E>, A3: Async<'a, Error=E>, E> Join<'a, (A1::Value, A2::Value, A3::Value), E> for (A1, A2, A3)
        where E: Send + 'a,
              A1::Value: Send,
              A2::Value: Send,
              A3::Value: Send {

    fn join(self, complete: Complete<'a, (A1::Value, A2::Value, A3::Value), E>) {
        let (a1, a2, a3) = self;
        let p = Progress::new((None, None, None), complete, 3);

        component!(a1, p, 0);
        component!(a2, p, 1);
        component!(a3, p, 2);
    }
}
