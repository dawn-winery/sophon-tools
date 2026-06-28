//! Boxed cloneable type-erased callback

pub type UpdaterFnBox<'a, U> = Box<dyn UpdaterFn<'a, U>>;

pub trait UpdaterFn<'a, U>: Fn(U) + Send + 'a {
    fn clone_box(&self) -> UpdaterFnBox<'a, U>;
}

impl<'a, T, U> UpdaterFn<'a, U> for T
where
    T: Clone,
    T: Fn(U),
    T: Send,
    T: 'a,
{
    fn clone_box(&self) -> UpdaterFnBox<'a, U> {
        Box::new(T::clone(self))
    }
}

impl<'a, U: 'static> Clone for UpdaterFnBox<'a, U> {
    fn clone(&self) -> Self {
        (**self).clone_box()
    }
}
