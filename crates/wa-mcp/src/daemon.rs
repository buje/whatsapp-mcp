//! `wa-mcp daemon` : propriétaire de la session WhatsApp.
//!
//! ```text
//! Desktop (chats)  → serve (client) ─┐
//! Cowork / Code    → serve (client) ─┼→ wa-mcp daemon → WhatsApp
//! autre hôte       → serve (client) ─┘   (bridge, verrous, base)
//! ```
//!
//! Le démon tient le bridge, les verrous des comptes et la base, et écoute sur
//! `run/serve.sock` (voir `share`). Il n'est lié au stdio d'aucun hôte : chaque
//! client est servi par son propre `WaServer` au-dessus du même bridge, et un
//! hôte qui tue son `serve` n'emporte que sa propre connexion.
//!
//! Lancé par `serve` (`--idle-exit`), il s'arrête quand le dernier client est
//! parti depuis le délai de grâce. Lancé à la main, il reste jusqu'à Ctrl-C ou
//! SIGTERM.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context as _, Result};
use tokio::net::UnixListener;
use tokio::signal::unix::{SignalKind, signal};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::Instant;

use crate::bridge::{self, BridgeHandle};
use crate::ingest::{ChatUpdate, Ingest};
use crate::store::Store;
use crate::{Mode, lock, mcp, share};

pub struct Options {
    /// S'arrêter une fois le dernier client parti depuis `grace`.
    pub idle_exit: bool,
    pub grace: Duration,
}

/// État partagé par toutes les connexions du démon.
#[derive(Clone)]
struct Shared {
    data_dir: PathBuf,
    handle: BridgeHandle,
    store: Store,
    updates: tokio::sync::broadcast::Sender<ChatUpdate>,
    limiter: Arc<mcp::SendLimiter>,
    held: lock::Held,
}

pub async fn run(data_dir: PathBuf, bin: &Path, opts: Options) -> Result<()> {
    // Dans cet ordre : un seul démon par répertoire, puis les comptes (un `pair`
    // ou un `run` dans un terminal les tient peut-être), et seulement alors le
    // socket, qu'on peut remplacer sans risque puisqu'on est le seul démon.
    let _daemon_lock = share::daemon_lock(&data_dir)?;
    let accounts = crate::discover(&data_dir)?;
    let _locks = crate::lock_accounts(&data_dir, &accounts)?;
    let listener = share::bind(&data_dir)?;
    tracing::info!(
        socket = %share::socket_path(&data_dir).display(),
        accounts = accounts.len(),
        idle_exit = opts.idle_exit,
        "démon prêt"
    );

    let bridge::Bridge {
        handle,
        mut events,
        task,
    } = bridge::spawn(bin, &data_dir)?;
    let (store, store_thread) = Store::open(data_dir.clone());
    let (updates, _) = tokio::sync::broadcast::channel(1024);
    let mut tasks = JoinSet::new();
    for a in accounts {
        let h = handle.clone();
        tasks.spawn(async move {
            h.start_account(&a, None)
                .await
                .map_err(anyhow::Error::from)
                .map(|()| None)
        });
    }
    let (ingest, refresher) = Ingest::new(handle.clone(), store.clone(), updates.clone());
    let pump_handle = handle.clone();
    let mut pump = tokio::spawn(async move {
        crate::event_loop(
            &pump_handle,
            &ingest,
            &mut events,
            &mut tasks,
            None,
            Mode::Run,
            false,
        )
        .await
    });

    let shared = Shared {
        data_dir: data_dir.clone(),
        handle: handle.clone(),
        store: store.clone(),
        updates,
        limiter: Arc::default(),
        held: lock::Held::default(),
    };
    let result = serve(listener, shared, &mut pump, &opts).await;

    pump.abort();
    refresher.abort();
    let _ = std::fs::remove_file(share::socket_path(&data_dir));
    tracing::info!("démon arrêté");
    crate::shutdown(handle, store, task, store_thread, result).await
}

/// Accepte les clients jusqu'à l'arrêt : signal, bridge tombé, ou plus aucun
/// client depuis le délai de grâce (`--idle-exit`).
async fn serve(
    listener: UnixListener,
    shared: Shared,
    pump: &mut JoinHandle<Result<()>>,
    opts: &Options,
) -> Result<()> {
    // Les connexions vivent dans ce JoinSet : à l'arrêt du démon, elles tombent
    // avec lui et leurs clients sortent aussitôt.
    let mut connections = JoinSet::new();
    let mut sigterm = signal(SignalKind::terminate()).context("signal SIGTERM")?;
    // Sans client, l'horloge tourne dès le démarrage : un démon lancé pour un
    // client qui n'est jamais venu ne reste pas indéfiniment.
    let mut idle_since = opts.idle_exit.then(Instant::now);
    loop {
        let idle = async {
            match idle_since {
                Some(t) => tokio::time::sleep_until(t + opts.grace).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((stream, _)) => {
                    idle_since = None;
                    connections.spawn(connection(stream, shared.clone()));
                }
                Err(e) => {
                    tracing::warn!(error = %e, "connexion refusée");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
            },
            Some(_) = connections.join_next() => {
                if connections.is_empty() && opts.idle_exit {
                    tracing::info!(grace = ?opts.grace, "plus aucun client, arrêt après le délai de grâce");
                    idle_since = Some(Instant::now());
                }
            }
            () = idle => {
                tracing::info!("délai de grâce écoulé sans client");
                return Ok(());
            }
            pumped = &mut *pump => {
                return match pumped {
                    Ok(r) => r,
                    Err(e) => Err(anyhow::anyhow!("boucle d'événements : {e}")),
                };
            }
            _ = sigterm.recv() => {
                tracing::info!("SIGTERM");
                return Ok(());
            }
        }
    }
}

/// Un client : son propre `WaServer`, au-dessus de l'état partagé du démon.
async fn connection(stream: tokio::net::UnixStream, s: Shared) {
    let served = async {
        let (read, write, read_only) = share::accept(stream).await?;
        // Comptes du moment : ceux appairés depuis le démarrage compris.
        let accounts = crate::discover(&s.data_dir)?;
        let server = mcp::WaServer::new(
            s.data_dir, s.handle, s.store, accounts, read_only, s.updates,
        )
        .with_shared(s.limiter, s.held);
        server.open_databases().await?;
        use rmcp::ServiceExt as _;
        let service = server
            .serve((read, write))
            .await
            .context("transport du client")?;
        tracing::info!(read_only, "client connecté");
        let quit = service.waiting().await;
        tracing::info!(?quit, "client parti");
        anyhow::Ok(())
    };
    if let Err(e) = served.await {
        tracing::warn!(error = %format!("{e:#}"), "client abandonné");
    }
}
