//! Socket du démon et côté client de `serve`.
//!
//! La session WhatsApp appartient au démon (`wa-mcp daemon`, voir `daemon`) :
//! il tient le bridge, les verrous et la base, et écoute sur `run/serve.sock`
//! dans le répertoire de données. `wa-mcp serve` n'est qu'un client mince :
//! il se branche sur ce socket et y recopie son stdio, après avoir lancé le
//! démon, détaché, si aucun n'écoutait. Un hôte qui tue son `serve` ne coupe
//! donc jamais la session des autres hôtes.

use std::fs::File;
use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};

/// Première ligne envoyée par un client, suivie de `read-only` ou `read-write`.
const HELLO: &str = "wa-mcp-share/1";
/// Le temps laissé à un démon qui démarre pour ouvrir son socket.
const CONNECT_PATIENCE: Duration = Duration::from_secs(15);
const CONNECT_RETRY: Duration = Duration::from_millis(200);
/// Un client qui ne se présente pas dans ce délai est éconduit.
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
/// Au-delà, le journal du démon repart de zéro (l'ancien est gardé en `.1`).
const LOG_MAX_BYTES: u64 = 5 * 1024 * 1024;
/// Repère écrit dans le journal avant chaque lancement par `serve`.
const LOG_MARK: &str = "--- wa-mcp daemon lancé par serve";
/// Code de sortie d'un démon qui en trouve un autre déjà en place : pour le
/// client qui l'a lancé, ce n'est pas un échec, il rejoint l'autre.
pub const EXIT_ALREADY_RUNNING: i32 = 75;

/// Un autre démon tient déjà le répertoire de données.
#[derive(Debug, thiserror::Error)]
#[error("un démon wa-mcp tourne déjà sur {0}")]
pub struct AlreadyRunning(pub String);

/// `run/` : répertoire en 0700 qui porte le socket, le verrou et le journal du
/// démon. Le socket n'y est jamais joignable par un autre utilisateur, pas même
/// entre `bind` et `chmod`.
pub fn run_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("run")
}

pub fn socket_path(data_dir: &Path) -> PathBuf {
    run_dir(data_dir).join("serve.sock")
}

pub fn log_path(data_dir: &Path) -> PathBuf {
    run_dir(data_dir).join("daemon.log")
}

/// Verrou du démon : un seul par répertoire de données, comptes appairés ou non.
/// Le tenir rend aussi sûre la suppression d'un socket resté d'un démon tué.
pub fn daemon_lock(data_dir: &Path) -> Result<File> {
    let path = private_dir(&run_dir(data_dir))?.join("daemon.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .with_context(|| format!("ouverture de {}", path.display()))?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => {
            Err(AlreadyRunning(data_dir.display().to_string()).into())
        }
        Err(std::fs::TryLockError::Error(e)) => {
            Err(e).with_context(|| format!("verrou {}", path.display()))
        }
    }
}

/// Un démon tient-il le répertoire de données ? (pour `doctor`, sans se connecter)
pub fn daemon_running(data_dir: &Path) -> bool {
    let Ok(file) = File::open(run_dir(data_dir).join("daemon.lock")) else {
        return false;
    };
    matches!(file.try_lock(), Err(std::fs::TryLockError::WouldBlock))
}

/// Ouvre le socket du démon. À n'appeler qu'en tenant `daemon_lock`.
pub fn bind(data_dir: &Path) -> Result<UnixListener> {
    let path = private_dir(&run_dir(data_dir))?.join("serve.sock");
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("suppression de {}", path.display())),
    }
    let listener =
        UnixListener::bind(&path).with_context(|| format!("écoute sur {}", path.display()))?;
    // Ceinture et bretelles : le répertoire suffit déjà à fermer l'accès.
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("droits de {}", path.display()))?;
    Ok(listener)
}

/// Crée `dir` en 0700, ou le ramène à 0700 s'il existe. Refuse un lien
/// symbolique : on ne suit pas un chemin posé par quelqu'un d'autre.
fn private_dir(dir: &Path) -> Result<PathBuf> {
    match std::fs::DirBuilder::new().mode(0o700).create(dir) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
            let meta = std::fs::symlink_metadata(dir)
                .with_context(|| format!("lecture de {}", dir.display()))?;
            if !meta.is_dir() {
                bail!("{} existe et n'est pas un répertoire", dir.display());
            }
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
                .with_context(|| format!("droits de {}", dir.display()))?;
        }
        Err(e) => return Err(e).with_context(|| format!("création de {}", dir.display())),
    }
    Ok(dir.to_path_buf())
}

/// Côté démon : lit la présentation d'un client. Rend les deux moitiés du
/// flux, prêtes pour le protocole MCP, et le mode demandé par l'hôte du client.
pub async fn accept(
    stream: UnixStream,
) -> Result<(BufReader<OwnedReadHalf>, OwnedWriteHalf, bool)> {
    let (read, write) = stream.into_split();
    let mut read = BufReader::new(read);
    let mut line = String::new();
    tokio::time::timeout(HELLO_TIMEOUT, read.read_line(&mut line))
        .await
        .context("client muet")??;
    let read_only = match line.trim_end().split_once(' ') {
        Some((HELLO, "read-only")) => true,
        Some((HELLO, "read-write")) => false,
        _ => bail!("présentation de client inattendue : {:?}", line.trim_end()),
    };
    Ok((read, write, read_only))
}

/// `wa-mcp serve` : rejoint le démon (en le lançant au besoin) et recopie
/// stdio dans les deux sens, jusqu'à ce que l'hôte ou le démon s'en aille.
pub async fn client(data_dir: &Path, bridge: &Path, read_only: bool) -> Result<()> {
    let path = socket_path(data_dir);
    let stream = match UnixStream::connect(&path).await {
        Ok(s) => s,
        Err(_) => {
            let daemon = spawn_daemon(data_dir, bridge)?;
            connect(data_dir, daemon).await?
        }
    };
    tracing::info!(socket = %path.display(), read_only, "connecté au démon");
    let (mut from_daemon, mut to_daemon) = stream.into_split();
    let mode = if read_only { "read-only" } else { "read-write" };
    to_daemon
        .write_all(format!("{HELLO} {mode}\n").as_bytes())
        .await?;

    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let up = async {
        tokio::io::copy(&mut stdin, &mut to_daemon).await?;
        to_daemon.shutdown().await
    };
    let down = async {
        tokio::io::copy(&mut from_daemon, &mut stdout).await?;
        stdout.flush().await
    };
    tokio::select! {
        r = up => { r.context("hôte MCP → démon")?; tracing::info!("hôte MCP parti, fin du client"); }
        r = down => { r.context("démon → hôte MCP")?; tracing::info!("démon arrêté, fin du client"); }
    }
    Ok(())
}

/// Lance `wa-mcp daemon --idle-exit`, détaché : son propre groupe de process
/// (les signaux envoyés à l'hôte ne l'atteignent pas), stdio fermés, journal
/// dans `run/daemon.log`. Deux clients qui le lancent en même temps ne gênent
/// pas : le second démon trouve `daemon.lock` pris et s'arrête aussitôt.
fn spawn_daemon(data_dir: &Path, bridge: &Path) -> Result<tokio::process::Child> {
    let log = log_path(data_dir);
    private_dir(&run_dir(data_dir))?;
    if std::fs::metadata(&log).is_ok_and(|m| m.len() > LOG_MAX_BYTES) {
        let _ = std::fs::rename(&log, log.with_extension("log.1"));
    }
    let mut stderr = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&log)
        .with_context(|| format!("journal {}", log.display()))?;
    {
        use std::io::Write as _;
        let _ = writeln!(stderr, "{LOG_MARK} (client pid {}) ---", std::process::id());
    }
    let exe = std::env::current_exe().context("chemin de wa-mcp")?;
    let child = tokio::process::Command::new(exe)
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--bridge")
        .arg(bridge)
        .args(["daemon", "--idle-exit"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(stderr)
        .process_group(0)
        .kill_on_drop(false)
        .spawn()
        .context("lancement du démon")?;
    tracing::info!(pid = child.id(), log = %log.display(), "démon lancé");
    Ok(child)
}

/// Attend le socket du démon qu'on vient de lancer. S'il s'arrête en échec
/// (compte tenu par `pair` ou `run`, bridge introuvable...), on le dit tout de
/// suite avec la fin de son journal ; s'il s'arrête parce qu'un autre démon
/// était là (deux clients lancés ensemble), on rejoint cet autre démon.
async fn connect(data_dir: &Path, mut daemon: tokio::process::Child) -> Result<UnixStream> {
    let path = socket_path(data_dir);
    let log = log_path(data_dir);
    let deadline = tokio::time::Instant::now() + CONNECT_PATIENCE;
    let mut ours = true;
    loop {
        match UnixStream::connect(&path).await {
            Ok(s) => {
                if ours {
                    // Récolté s'il s'arrête avant nous ; il vit sa vie sinon.
                    tokio::spawn(async move {
                        let _ = daemon.wait().await;
                    });
                }
                return Ok(s);
            }
            Err(e) if tokio::time::Instant::now() >= deadline => {
                return Err(e).with_context(|| {
                    format!(
                        "le démon n'écoute pas sur {} ; fin de {} :\n{}",
                        path.display(),
                        log.display(),
                        log_tail(&log, 5)
                    )
                });
            }
            Err(_) => {}
        }
        if ours && let Ok(Some(status)) = daemon.try_wait() {
            ours = false;
            if status.code() != Some(EXIT_ALREADY_RUNNING) {
                bail!(
                    "le démon s'est arrêté au démarrage ({status}) ; fin de {} :\n{}",
                    log.display(),
                    log_tail(&log, 5)
                );
            }
        }
        tokio::time::sleep(CONNECT_RETRY).await;
    }
}

/// Dernières lignes écrites par le dernier démon lancé, pour dire pourquoi il
/// n'a pas démarré (le plus souvent : compte tenu par `pair` ou `run`).
fn log_tail(path: &Path, lines: usize) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let last_run = text.rfind(LOG_MARK).map_or(text.as_str(), |i| &text[i..]);
    let all: Vec<&str> = last_run.lines().skip(1).collect();
    all[all.len().saturating_sub(lines)..].join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("wa-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap_or_default();
        dir
    }

    fn mode(path: &Path) -> u32 {
        std::fs::metadata(path)
            .map(|m| m.permissions().mode() & 0o777)
            .unwrap_or_default()
    }

    #[tokio::test]
    async fn presentation_du_client() {
        let dir = temp("share");
        // Un `run/` trop ouvert (ancienne version, umask large) est resserré.
        std::fs::create_dir_all(run_dir(&dir)).unwrap_or_default();
        std::fs::set_permissions(run_dir(&dir), std::fs::Permissions::from_mode(0o755))
            .unwrap_or_default();
        let listener = bind(&dir).unwrap_or_else(|e| panic!("bind : {e}"));
        assert_eq!(mode(&run_dir(&dir)), 0o700);
        assert_eq!(mode(&socket_path(&dir)), 0o600);

        for (sent, expected) in [
            ("read-only", Some(true)),
            ("read-write", Some(false)),
            ("n'importe", None),
        ] {
            let path = socket_path(&dir);
            let client = tokio::spawn(async move {
                let mut s = UnixStream::connect(&path)
                    .await
                    .unwrap_or_else(|e| panic!("connect : {e}"));
                let _ = s.write_all(format!("{HELLO} {sent}\n").as_bytes()).await;
                s
            });
            let (stream, _) = listener
                .accept()
                .await
                .unwrap_or_else(|e| panic!("accept : {e}"));
            let got = accept(stream).await.ok().map(|(_, _, ro)| ro);
            assert_eq!(got, expected, "présentation {sent:?}");
            drop(client.await);
        }

        // Un socket orphelin (démon tué) est remplacé sans erreur.
        drop(listener);
        assert!(bind(&dir).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn un_seul_demon_par_repertoire() {
        let dir = temp("daemon-lock");
        assert!(!daemon_running(&dir));
        let first = daemon_lock(&dir).unwrap_or_else(|e| panic!("verrou : {e}"));
        assert!(daemon_running(&dir));
        assert!(
            daemon_lock(&dir).is_err(),
            "un second démon doit être refusé"
        );
        drop(first);
        assert!(!daemon_running(&dir));
        assert!(daemon_lock(&dir).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn run_refuse_un_lien_symbolique() {
        let dir = temp("symlink");
        let elsewhere = temp("symlink-target");
        std::os::unix::fs::symlink(&elsewhere, run_dir(&dir)).unwrap_or_default();
        assert!(bind(&dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&elsewhere);
    }
}
