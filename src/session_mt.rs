//! Multi-threaded session implementation
//!
//! This module provides a multi-threaded session loop for FUSE filesystems,
//! based on the design from libfuse's fuse_loop_mt.

use libc::{EAGAIN, EINTR, ENODEV, ENOENT};
use log::{debug, error, info, warn};
use std::cell::UnsafeCell;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use crate::channel::Channel;
use crate::ll::fuse_abi::fuse_opcode::{FUSE_FORGET, FUSE_BATCH_FORGET};
use crate::mnt::Mount;
use crate::request::Request;
use crate::session::{aligned_sub_buf, Session, SessionACL, BUFFER_SIZE};
use crate::Filesystem;

/// Default maximum number of worker threads
const DEFAULT_MAX_THREADS: usize = 10;

/// Default maximum idle threads (-1 means thread destruction is disabled)
const DEFAULT_MAX_IDLE_THREADS: i32 = -1;

/// Maximum reasonable number of threads to prevent resource exhaustion
const MAX_THREADS_LIMIT: usize = 100_000;

/// Configuration for multi-threaded session loop
#[derive(Debug, Clone)]
pub struct SessionConfig {
    /// Maximum number of worker threads
    pub max_threads: usize,
    /// Maximum number of idle threads before they are destroyed
    pub max_idle_threads: i32,
    /// Whether to clone the /dev/fuse file descriptor for each thread
    pub clone_fd: bool,
}

impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            max_threads: DEFAULT_MAX_THREADS,
            max_idle_threads: DEFAULT_MAX_IDLE_THREADS,
            clone_fd: false,
        }
    }
}

impl SessionConfig {
    /// Create a new configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of worker threads
    pub fn max_threads(mut self, max_threads: usize) -> Self {
        self.max_threads = max_threads.min(MAX_THREADS_LIMIT);
        self
    }

    /// Set the maximum number of idle threads
    pub fn max_idle_threads(mut self, max_idle_threads: i32) -> Self {
        self.max_idle_threads = max_idle_threads;
        self
    }

    /// Enable or disable fd cloning
    pub fn clone_fd(mut self, clone_fd: bool) -> Self {
        self.clone_fd = clone_fd;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> io::Result<()> {
        if self.max_threads == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "max_threads must be at least 1",
            ));
        }
        if self.max_threads > MAX_THREADS_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("max_threads cannot exceed {}", MAX_THREADS_LIMIT),
            ));
        }
        Ok(())
    }
}

/// Worker thread state
#[allow(dead_code)]
struct Worker {
    thread: Option<JoinHandle<()>>,
    id: usize,
}

impl Worker {
    fn new(id: usize, thread: JoinHandle<()>) -> Self {
        Self {
            thread: Some(thread),
            id,
        }
    }
}

/// Shared state for the multi-threaded session
struct MtState {
    /// Number of worker threads
    num_workers: AtomicUsize,
    /// Number of available worker threads
    num_available: AtomicUsize,
    /// Whether the session should exit
    exit: AtomicBool,
    /// Protected state for thread management
    inner: Mutex<MtStateInner>,
    /// Condition variable for signaling exit completion
    cvar: Condvar,
}

struct MtStateInner {
    /// Worker threads
    workers: Vec<Worker>,
    /// Error from worker threads
    error: Option<io::Error>,
}

impl MtState {
    fn new() -> Self {
        Self {
            num_workers: AtomicUsize::new(0),
            num_available: AtomicUsize::new(0),
            exit: AtomicBool::new(false),
            inner: Mutex::new(MtStateInner {
                workers: Vec::new(),
                error: None,
            }),
            cvar: Condvar::new(),
        }
    }
}

struct SyncUnsafeCell<T>(UnsafeCell<T>);

impl<T> SyncUnsafeCell<T> {
    fn new(value: T) -> Self {
        Self(UnsafeCell::new(value))
    }

    fn get(&self) -> *mut T {
        self.0.get()
    }
}
unsafe impl<T: Sync> Sync for SyncUnsafeCell<T> {}

/// Context shared between all worker threads
struct SessionContext {
    /// Protocol major version
    proto_major: AtomicU32,
    /// Protocol minor version
    proto_minor: AtomicU32,
    /// Whether the session is initialized
    initialized: AtomicBool,
    /// Whether the session is destroyed
    destroyed: AtomicBool,
    /// Session owner ID
    session_owner: u32,
}

impl SessionContext {
    fn new(proto_major: u32, proto_minor: u32, initialized: bool, session_owner: u32) -> Self {
        Self {
            proto_major: AtomicU32::new(proto_major),
            proto_minor: AtomicU32::new(proto_minor),
            initialized: AtomicBool::new(initialized),
            destroyed: AtomicBool::new(false),
            session_owner,
        }
    }
}

/// Helper struct to snapshot and sync session state
struct SessionState {
    proto_major: u32,
    proto_minor: u32,
    initialized: bool,
    destroyed: bool,
    _orig_major: u32,
    _orig_minor: u32,
    _orig_init: bool,
    _orig_dest: bool,
}

impl SessionState {
    fn new(context: &SessionContext) -> Self {
        let major = context.proto_major.load(Ordering::Relaxed);
        let minor = context.proto_minor.load(Ordering::Relaxed);
        let init = context.initialized.load(Ordering::Relaxed);
        let dest = context.destroyed.load(Ordering::Relaxed);
        Self {
            proto_major: major,
            proto_minor: minor,
            initialized: init,
            destroyed: dest,
            _orig_major: major,
            _orig_minor: minor,
            _orig_init: init,
            _orig_dest: dest,
        }
    }

    fn commit(self, context: &SessionContext) {
        if self.proto_major != self._orig_major {
            context.proto_major.store(self.proto_major, Ordering::Relaxed);
        }
        if self.proto_minor != self._orig_minor {
            context.proto_minor.store(self.proto_minor, Ordering::Relaxed);
        }
        if self.initialized != self._orig_init {
            context.initialized.store(self.initialized, Ordering::SeqCst);
        }
        if self.destroyed != self._orig_dest {
            context.destroyed.store(self.destroyed, Ordering::SeqCst);
        }
    }
}

/// Multi-threaded session runner
pub struct MtSession<FS: Filesystem> {
    state: Arc<MtState>,
    config: SessionConfig,
    filesystem: Arc<SyncUnsafeCell<FS>>,
    channel: Channel,
    mount: Arc<Mutex<Option<(PathBuf, Mount)>>>,
    context: Arc<SessionContext>,
    allowed: SessionACL,
    worker_counter: Arc<AtomicUsize>,
}

unsafe impl<FS: Filesystem + Send + Sync> Send for MtSession<FS> {}
unsafe impl<FS: Filesystem + Send + Sync> Sync for MtSession<FS> {}

impl<FS: Filesystem> std::fmt::Debug for MtSession<FS> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MtSession")
            .field("config", &self.config)
            .field("mount", &self.mount)
            .field("session_owner", &self.context.session_owner)
            .field("allowed", &self.allowed)
            .finish()
    }
}

impl<FS: Filesystem> MtSession<FS> {
    /// Request all workers to exit
    pub fn exit(&self) {
        self.state.exit.store(true, Ordering::Release);
        let _unused = self.state.inner.lock();
        self.state.cvar.notify_all();
    }
}

impl<FS: Filesystem + Send + Sync + 'static> MtSession<FS> {
    /// Create a new multi-threaded session from a regular session
    ///
    /// # Safety Requirements
    ///
    /// The filesystem type FS must be thread-safe (implement Sync). This means:
    /// - For read-only filesystems: No problem, they are naturally thread-safe
    /// - For filesystems with mutable state: Must use interior mutability (Mutex, RwLock, etc.)
    ///
    /// # Example
    ///
    /// ```ignore
    /// // For a read-only filesystem like HelloFS, just derive Sync
    /// struct HelloFS;  // Already Sync by default if no interior mutability
    ///
    /// // For a filesystem with state, use interior mutability:
    /// struct MyFS {
    ///     state: Arc<Mutex<State>>,  // Use Mutex/RwLock for mutable state
    /// }
    /// ```
    pub fn from_session(session: Session<FS>, config: SessionConfig) -> io::Result<Self> {
        config.validate()?;

        let manual_session = std::mem::ManuallyDrop::new(session);

        let channel = manual_session.ch.clone();
        let mount = manual_session.mount.clone();
        let session_owner = manual_session.session_owner;
        let proto_major = manual_session.proto_major;
        let proto_minor = manual_session.proto_minor;
        let allowed = manual_session.allowed;
        let initialized = manual_session.initialized;
        
        let filesystem = unsafe {
            std::ptr::read(&manual_session.filesystem as *const FS)
        };

        Ok(Self {
            state: Arc::new(MtState::new()),
            config,
            filesystem: Arc::new(SyncUnsafeCell::new(filesystem)),
            channel,
            mount,
            context: Arc::new(SessionContext::new(proto_major, proto_minor, initialized, session_owner)),
            allowed,
            worker_counter: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Run the multi-threaded session loop
    pub fn run(&mut self) -> io::Result<()> {
        let mode = if self.config.max_threads == 1 {
            "single-threaded"
        } else {
            "multi-threaded"
        };
        info!(
            "Initializing {} FUSE session with maximum {} worker threads",
            mode, self.config.max_threads
        );

        self.start_worker()?;

        let mut inner = self.state.inner.lock().unwrap();
        while self.state.num_workers.load(Ordering::Acquire) > 0 {
            if self.state.exit.load(Ordering::Acquire) && inner.workers.is_empty() {
                break;
            }
            inner = self.state.cvar.wait(inner).unwrap();
        }

        let result = if let Some(err) = inner.error.take() {
            Err(err)
        } else {
            Ok(())
        };

        info!("FUSE session ended");
        result
    }

    /// Start a new worker thread
    fn start_worker(&self) -> io::Result<()> {
        let worker_id = self.worker_counter.fetch_add(1, Ordering::Relaxed);

        let prev_workers = self.state.num_workers.fetch_add(1, Ordering::AcqRel);
        if prev_workers >= self.config.max_threads {
             self.state.num_workers.fetch_sub(1, Ordering::AcqRel);
             return Err(io::Error::new(io::ErrorKind::ResourceBusy, "Max threads reached"));
        }

        let state = self.state.clone();
        let config = self.config.clone();
        let filesystem = self.filesystem.clone();
        let context = self.context.clone();

        // Clone the channel if clone_fd is enabled, otherwise share the same channel
        let channel = if self.config.clone_fd {
            match self.channel.clone_fd() {
                Ok(ch) => ch,
                Err(e) => {
                    warn!("Failed to clone fd for worker {}, using shared channel: {}", worker_id, e);
                    self.channel.clone()
                }
            }
        } else {
            self.channel.clone()
        };

        let allowed = self.allowed;
        let worker_counter = self.worker_counter.clone();
        let master_channel = self.channel.clone();

        let res = thread::Builder::new()
            .name(format!("fuse-worker-{}", worker_id))
            .spawn(move || {
                worker_main(
                    worker_id,
                    state,
                    config,
                    filesystem,
                    channel,
                    context,
                    allowed,
                    worker_counter,
                    master_channel,
                )
            });

        match res {
            Ok(thread) => {
                let mut inner = self.state.inner.lock().unwrap();
                inner.workers.push(Worker::new(worker_id, thread));
                debug!("Worker thread {} initialized", worker_id);
                Ok(())
            }
            Err(e) => {
                self.state.num_workers.fetch_sub(1, Ordering::AcqRel);
                Err(e)
            }
        }
    }
}

/// Main function for worker threads
#[allow(unused_variables)]
#[allow(clippy::too_many_arguments)]
fn worker_main<FS: Filesystem + Send + Sync + 'static>(
    worker_id: usize,
    state: Arc<MtState>,
    config: SessionConfig,
    filesystem: Arc<SyncUnsafeCell<FS>>,
    channel: Channel,
    context: Arc<SessionContext>,
    allowed: SessionACL,
    worker_counter: Arc<AtomicUsize>,
    master_channel: Channel,
) {
    let mut buffer = vec![0u8; BUFFER_SIZE];
    let mut self_cleaned = false;

    let try_spawn_new_worker = |current_total: usize| {
        let prev = state.num_workers.fetch_add(1, Ordering::AcqRel);
        if prev >= config.max_threads {
            state.num_workers.fetch_sub(1, Ordering::AcqRel);
            return;
        }

        let new_id = worker_counter.fetch_add(1, Ordering::Relaxed);
        debug!("Worker {} spawning new worker thread {}", worker_id, new_id);

        let state_c = state.clone();
        let config_c = config.clone();
        let fs_c = filesystem.clone();
        let ctx_c = context.clone();
        let al_c = allowed;
        let wc_c = worker_counter.clone();
        let mc_c = master_channel.clone();

        let ch_c = if config.clone_fd {
            match master_channel.clone_fd() {
                Ok(ch) => ch,
                Err(_) => master_channel.clone(),
            }
        } else {
            master_channel.clone()
        };

        let builder = thread::Builder::new().name(format!("fuse-worker-{}", new_id));
        match builder.spawn(move || {
            worker_main(new_id, state_c, config_c, fs_c, ch_c, ctx_c, al_c, wc_c, mc_c)
        }) {
            Ok(t) => {
                let mut inner = state.inner.lock().unwrap();
                inner.workers.push(Worker::new(new_id, t));
            },
            Err(e) => {
                error!("Failed to spawn worker: {}", e);
                state.num_workers.fetch_sub(1, Ordering::AcqRel);
            }
        }
    };

    loop {
        if state.exit.load(Ordering::Relaxed) {
            debug!("Worker {} exiting (session exit)", worker_id);
            break;
        }

        state.num_available.fetch_add(1, Ordering::Release);

        let buf = aligned_sub_buf(&mut buffer, std::mem::align_of::<crate::ll::fuse_abi::fuse_in_header>());
        let res = channel.receive(buf);

        let prev_idle = state.num_available.fetch_sub(1, Ordering::Acquire);

        let size = match res {
            Ok(s) => s,
            Err(e) => {
                match e.raw_os_error() {
                    Some(ENOENT | EINTR | EAGAIN) => continue,
                    Some(ENODEV) => {
                        debug!("Worker {} exiting (ENODEV)", worker_id);
                        state.exit.store(true, Ordering::Release);
                        let _unused = state.inner.lock();
                        state.cvar.notify_all();
                        break;
                    },
                    _ => {
                        error!("Worker {} error receiving request: {}", worker_id, e);
                        let mut inner = state.inner.lock().unwrap();
                        inner.error = Some(e);
                        state.exit.store(true, Ordering::Release);
                        state.cvar.notify_all();
                        break;
                    }
                }
            }
        };

        // Thread pool exhausted
        if prev_idle <= 1 {
            let is_forget = if size >= std::mem::size_of::<crate::ll::fuse_abi::fuse_in_header>() {
                let header = unsafe { &*(buf.as_ptr() as *const crate::ll::fuse_abi::fuse_in_header) };
                header.opcode == FUSE_FORGET as u32 || header.opcode == FUSE_BATCH_FORGET as u32
            } else {
                false
            };

            if !is_forget && context.initialized.load(Ordering::Relaxed) {
                let current_workers = state.num_workers.load(Ordering::Relaxed);
                if current_workers < config.max_threads {
                    try_spawn_new_worker(current_workers);
                }
            }
        }

        // Process the Request
        if let Some(req) = Request::new(channel.sender(), &buf[..size]) {
            let fs_ref = unsafe { &mut *filesystem.get() };

            let mut state = SessionState::new(&context);

            req.dispatch_with_context(
                fs_ref,
                &allowed,
                context.session_owner,
                &mut state.proto_major,
                &mut state.proto_minor,
                &mut state.initialized,
                &mut state.destroyed,
            );

            state.commit(&context);
        }

        // Terminate excess idle threads
        if config.max_idle_threads != -1 {
            let current_idle = state.num_available.load(Ordering::Relaxed);
            if current_idle > config.max_idle_threads as usize {
                let mut inner = state.inner.lock().unwrap();

                let recheck_idle = state.num_available.load(Ordering::Relaxed);
                let recheck_workers = state.num_workers.load(Ordering::Relaxed);

                if recheck_idle > config.max_idle_threads as usize && recheck_workers > 1 {
                     if let Some(pos) = inner.workers.iter().position(|w| w.id == worker_id) {
                         inner.workers.remove(pos);
                     }
                     state.num_workers.fetch_sub(1, Ordering::AcqRel);
                     state.num_available.fetch_sub(1, Ordering::AcqRel);
                     self_cleaned = true;
                     debug!("Worker {} exiting (idle threads: {} > max: {})",
                            worker_id, recheck_idle, config.max_idle_threads);
                     break;
                }
            }
        }
    }
    if !self_cleaned {
        let mut inner = state.inner.lock().unwrap();
        if let Some(pos) = inner.workers.iter().position(|w| w.id == worker_id) {
            inner.workers.remove(pos);
        }
        state.num_workers.fetch_sub(1, Ordering::AcqRel);
    }

    state.cvar.notify_all();
}

impl<FS: Filesystem> Drop for MtSession<FS> {
    fn drop(&mut self) {
        self.exit();

        let workers = {
            let mut inner = self.state.inner.lock().unwrap();
            std::mem::take(&mut inner.workers)
        };
        for worker in workers {
            if let Some(thread) = worker.thread {
                let _ = thread.join();
            }
        }
    }
}
