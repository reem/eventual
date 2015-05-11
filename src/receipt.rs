use super::Async;
use super::core::Core;
use std::marker::PhantomData;

pub struct Receipt<'a, A: Async<'a>> {
    core: Option<Core<'a, A::Value, A::Error>>,
    count: u64,
    marker: PhantomData<A>,
}

unsafe impl<'a, A: Async<'a>> Send for Receipt<'a, A> { }

pub fn new<'a, A, T: Send, E: Send>(core: Core<'a, T, E>, count: u64) -> Receipt<'a, A>
        where A: Async<'a, Value=T, Error=E> {
    Receipt {
        core: Some(core),
        count: count,
        marker: PhantomData,
    }
}

pub fn none<'a, A: Async<'a>>() -> Receipt<'a, A> {
    Receipt {
        core: None,
        count: 0,
        marker: PhantomData,
    }
}

pub fn parts<'a, A, T: Send, E: Send>(receipt: Receipt<'a, A>) -> (Option<Core<'a, T, E>>, u64)
        where A: Async<'a, Value=T, Error=E> {
    (receipt.core, receipt.count)
}
