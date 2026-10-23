//! One bounded mailbox per owner keeps blocking I/O off Tokio's async workers.
use crate::{Error, Result};
use tokio::sync::{mpsc, oneshot};

type Job<T> = Box<dyn FnOnce(&mut T) + Send>;

pub(crate) struct BlockingWorker<T> {
    sender: mpsc::Sender<Job<T>>,
    finished: oneshot::Receiver<()>,
}

impl<T: Send + 'static> BlockingWorker<T> {
    pub(crate) fn new(mut state: T) -> Result<Self> {
        let (sender, mut receiver) = mpsc::channel::<Job<T>>(1);
        let (done, finished) = oneshot::channel();
        // Dedicated threads must not occupy Tokio's shared blocking pool while
        // idle: two idle pipeline workers could exhaust a small runtime pool.
        std::thread::Builder::new()
            .name("synthflow-io".into())
            .spawn(move || {
                while let Some(job) = receiver.blocking_recv() {
                    job(&mut state);
                }
                // State (including the output lock) outlives every queued write,
                // even if the async caller is aborted while waiting for a reply.
                drop(state);
                let _ = done.send(());
            })
            .map_err(|_| Error::RunFailed("cannot start file I/O worker".into()))?;
        Ok(Self { sender, finished })
    }

    pub(crate) async fn call<R: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut T) -> Result<R> + Send + 'static,
    ) -> Result<R> {
        let (sender, receiver) = oneshot::channel();
        self.sender
            .send(Box::new(move |state| {
                let _ = sender.send(operation(state));
            }))
            .await
            .map_err(|_| Error::RunFailed("file I/O worker stopped".into()))?;
        receiver
            .await
            .map_err(|_| Error::RunFailed("file I/O worker failed".into()))?
    }

    pub(crate) async fn close(self) -> Result<()> {
        drop(self.sender);
        self.finished
            .await
            .map_err(|_| Error::RunFailed("file I/O worker failed".into()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{sync::Arc, time::Duration};

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_work_leaves_the_async_runtime_responsive() {
        let worker = BlockingWorker::new(()).unwrap();
        let started = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = started.clone();
        {
            let operation = worker.call(move |_| {
                flag.store(true, std::sync::atomic::Ordering::SeqCst);
                std::thread::sleep(Duration::from_millis(150));
                Ok(())
            });
            tokio::pin!(operation);
            tokio::select! {
                result = &mut operation => panic!("blocking work finished before the timer: {result:?}"),
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
            assert!(started.load(std::sync::atomic::Ordering::SeqCst));
            operation.await.unwrap();
        }
        worker.close().await.unwrap();
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_callers_keep_the_lock_until_bounded_queued_writes_finish() {
        use crate::run::RunLock;
        use std::sync::{Mutex, mpsc as blocking_channel};

        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("output.jsonl");
        let events = Arc::new(Mutex::new(Vec::new()));
        let worker =
            BlockingWorker::new((RunLock::acquire(&output).unwrap(), events.clone())).unwrap();
        let (release_first, first_gate) = blocking_channel::channel();
        let (first_started, started) = oneshot::channel();
        let mut first = Box::pin(worker.call(move |(_, events)| {
            let _ = first_started.send(());
            first_gate.recv_timeout(Duration::from_secs(5)).unwrap();
            events.lock().unwrap().push(1);
            Ok(())
        }));
        tokio::select! {
            result = &mut first => panic!("first write should be blocked: {result:?}"),
            result = tokio::time::timeout(Duration::from_secs(2), started) => {
                result.expect("first write starts").unwrap();
            }
        }

        let (release_second, second_gate) = blocking_channel::channel();
        let (second_started, started) = oneshot::channel();
        let mut second = Box::pin(worker.call(move |(_, events)| {
            let _ = second_started.send(());
            second_gate.recv_timeout(Duration::from_secs(5)).unwrap();
            events.lock().unwrap().push(2);
            Ok(())
        }));
        assert!(futures_util::poll!(second.as_mut()).is_pending());
        assert_eq!(worker.sender.capacity(), 0, "only one write may be queued");
        let mut third = Box::pin(worker.call(|(_, events)| {
            events.lock().unwrap().push(3);
            Ok(())
        }));
        assert!(futures_util::poll!(third.as_mut()).is_pending());

        // Dropping the async callers cancels the third operation before it was
        // admitted. The running first and queued second operations must finish
        // under the lock even after the worker handle itself is dropped.
        drop(third);
        drop(second);
        drop(first);
        drop(worker);
        assert!(RunLock::acquire(&output).is_err());
        release_first.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(2), started)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*events.lock().unwrap(), [1]);
        assert!(
            RunLock::acquire(&output).is_err(),
            "queued write retains lock"
        );
        release_second.send(()).unwrap();
        let lock = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(lock) = RunLock::acquire(&output) {
                    break lock;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("lock released after the final queued write");
        assert_eq!(
            *events.lock().unwrap(),
            [1, 2],
            "cancelled third write never executes"
        );
        drop(lock);
    }
}
