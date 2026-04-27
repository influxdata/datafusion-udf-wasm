/// Assert that the given function panics and return the panic message.
pub(crate) fn should_panic<F>(f: F) -> String
where
    F: FnOnce(),
{
    match std::panic::catch_unwind(std::panic::AssertUnwindSafe(f)) {
        Ok(()) => panic!("did not panic"),
        Err(msg) => {
            if let Some(msg) = msg.downcast_ref::<&str>() {
                msg.to_string()
            } else if let Some(msg) = msg.downcast_ref::<String>() {
                msg.clone()
            } else {
                panic!("cannot extract message")
            }
        }
    }
}
