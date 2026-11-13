//! The pinned home of the senders: one thread, pinned first, whose runtime runs each execution edge
//! and the link actor, so that their loops never sleep and a command leaves on the next pass.

use std::convert::Infallible;
use std::sync::mpsc::{SyncSender, sync_channel};
use std::thread::JoinHandle;

use tokio::runtime::{Builder, Handle};
use tokio::sync::oneshot;

use crate::runtime::pin::spawn_pinned;
use crate::warn;

use super::EngineError;

const THREAD_NAME: &str = "polysim-sender";

/// The pinned senders' thread and the runtime on it. Dropping it stops the runtime and joins the
/// thread, so drop it only after each edge and the link actor on it have stopped.
pub struct PinnedSenders {
    handle: Handle,
    /// Dropping this sender closes the channel, which is the one stop signal: nothing is ever sent.
    stop: Option<oneshot::Sender<Infallible>>,
    thread: Option<JoinHandle<()>>,
}

impl PinnedSenders {
    /// Starts the thread, pins it to `core_id` before anything else runs on it, and builds its
    /// runtime. With no core the thread floats, which only a host that cannot pin allows.
    ///
    /// # Errors
    /// [`EngineError::SenderPin`] when the thread cannot pin to `core_id` on linux, and
    /// [`EngineError::Runtime`] when its runtime cannot be built. Either way no task runs on it.
    pub fn spawn(core_id: Option<usize>) -> Result<PinnedSenders, EngineError> {
        let (started, ready) = sync_channel(1);
        let (stop, stopped) = oneshot::channel();
        let thread = spawn_pinned(THREAD_NAME, core_id, move || run(&started, stopped)).map_err(
            |refused| EngineError::SenderPin {
                core_id: refused.core_id,
            },
        )?;
        let handle = ready
            .recv()
            .expect("the sender thread answers before it can end, since starting cannot panic");
        let handle = match handle {
            Ok(handle) => handle,
            Err(refusal) => {
                join(thread);
                return Err(refusal);
            }
        };
        Ok(PinnedSenders {
            handle,
            stop: Some(stop),
            thread: Some(thread),
        })
    }

    pub fn handle(&self) -> &Handle {
        &self.handle
    }
}

impl Drop for PinnedSenders {
    fn drop(&mut self) {
        drop(self.stop.take());
        if let Some(thread) = self.thread.take() {
            join(thread);
        }
    }
}

fn run(started: &SyncSender<Result<Handle, EngineError>>, stopped: oneshot::Receiver<Infallible>) {
    crate::log::register_thread("sender");
    let runtime = match Builder::new_current_thread().enable_all().build() {
        Ok(runtime) => runtime,
        Err(error) => return answer(started, Err(EngineError::Runtime(error))),
    };
    answer(started, Ok(runtime.handle().clone()));
    let Err(_closed) = runtime.block_on(stopped);
}

/// The starting thread waits for this answer, so a failed send means it has stopped waiting and no
/// task will ever run on this runtime.
fn answer(started: &SyncSender<Result<Handle, EngineError>>, answer: Result<Handle, EngineError>) {
    if started.send(answer).is_err() {
        warn!("the sender thread started after its start was abandoned");
    }
}

fn join(thread: JoinHandle<()>) {
    if thread.join().is_err() {
        warn!("the sender thread ended in a panic, which the panic hook has already reported");
    }
}
