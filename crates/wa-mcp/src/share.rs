//! Partage d'une instance `serve` entre plusieurs hôtes MCP.
//!
//! Un même hôte peut lancer `wa-mcp serve` plusieurs fois : Claude Desktop en
//! démarre une pour ses conversations et une autre pour les sessions Cowork et
//! Code. Le verrou du compte n'en laisse tourner qu'une, et c'est voulu (deux
//! clients sur la même session se déconnectent mutuellement).
//!
//! Plutôt que de s'arrêter, les instances suivantes deviennent des relais :
//! celle qui tient les verrous écoute sur `serve.sock` dans le répertoire de
//! données, les autres s'y branchent et y recopient leur stdio. Chaque
//! connexion est servie par son propre `WaServer` au-dessus du même bridge et
//! de la même base : une seule session WhatsApp, autant d'hôtes que voulu.
//!
//! Quand l'instance principale s'arrête, ses relais perdent leur socket et
//! sortent ; l'hôte les relance et l'un d'eux reprend les verrous.

use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context as _, Result, bail};
use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{UnixListener, UnixStream};

/// Première ligne envoyée par un relais, suivie de `read-only` ou `read-write`.
const HELLO: &str = "wa-mcp-share/1";
/// Le temps laissé à une instance principale qui démarre pour ouvrir son socket.
const CONNECT_PATIENCE: Duration = Duration::from_secs(10);
const CONNECT_RETRY: Duration = Duration::from_millis(250);
/// Un relais qui ne se présente pas dans ce délai est éconduit.
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);

pub fn socket_path(data_dir: &Path) -> PathBuf {
    data_dir.join("serve.sock")
}

/// Ouvre le socket de partage. À n'appeler qu'en tenant les verrous des
/// comptes : un socket resté d'un process tué est alors forcément orphelin.
pub fn bind(data_dir: &Path) -> Result<UnixListener> {
    let path = socket_path(data_dir);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(e).with_context(|| format!("suppression de {}", path.display())),
    }
    let listener =
        UnixListener::bind(&path).with_context(|| format!("écoute sur {}", path.display()))?;
    // Réservé à l'utilisateur, comme les bases à côté.
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
        .with_context(|| format!("droits de {}", path.display()))?;
    Ok(listener)
}

/// Côté principal : lit la présentation d'un relais. Rend les deux moitiés du
/// flux, prêtes pour le protocole MCP, et le mode demandé par l'hôte du relais.
pub async fn accept(
    stream: UnixStream,
) -> Result<(BufReader<OwnedReadHalf>, OwnedWriteHalf, bool)> {
    let (read, write) = stream.into_split();
    let mut read = BufReader::new(read);
    let mut line = String::new();
    tokio::time::timeout(HELLO_TIMEOUT, read.read_line(&mut line))
        .await
        .context("relais muet")??;
    let read_only = match line.trim_end().split_once(' ') {
        Some((HELLO, "read-only")) => true,
        Some((HELLO, "read-write")) => false,
        _ => bail!("présentation de relais inattendue : {:?}", line.trim_end()),
    };
    Ok((read, write, read_only))
}

/// Côté relais : se branche sur l'instance principale et recopie stdio dans
/// les deux sens, jusqu'à ce que l'hôte ou l'instance principale s'en aille.
pub async fn relay(data_dir: &Path, read_only: bool) -> Result<()> {
    let path = socket_path(data_dir);
    let stream = connect(&path).await?;
    tracing::info!(socket = %path.display(), read_only, "compte déjà servi : relais vers l'instance en cours");
    let (mut from_main, mut to_main) = stream.into_split();
    let mode = if read_only { "read-only" } else { "read-write" };
    to_main
        .write_all(format!("{HELLO} {mode}\n").as_bytes())
        .await?;

    let mut stdin = tokio::io::stdin();
    let mut stdout = tokio::io::stdout();
    let up = async {
        tokio::io::copy(&mut stdin, &mut to_main).await?;
        to_main.shutdown().await
    };
    let down = async {
        tokio::io::copy(&mut from_main, &mut stdout).await?;
        stdout.flush().await
    };
    tokio::select! {
        r = up => { r.context("hôte MCP → instance principale")?; tracing::info!("hôte MCP parti, arrêt du relais"); }
        r = down => { r.context("instance principale → hôte MCP")?; tracing::info!("instance principale arrêtée, arrêt du relais"); }
    }
    Ok(())
}

async fn connect(path: &Path) -> Result<UnixStream> {
    let deadline = tokio::time::Instant::now() + CONNECT_PATIENCE;
    loop {
        match UnixStream::connect(path).await {
            Ok(s) => return Ok(s),
            Err(e) if tokio::time::Instant::now() >= deadline => {
                return Err(e).with_context(|| {
                    format!(
                        "aucune instance `serve` à rejoindre sur {} (le compte est peut-être tenu par `pair` ou `run` dans un terminal)",
                        path.display()
                    )
                });
            }
            Err(_) => tokio::time::sleep(CONNECT_RETRY).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn presentation_du_relais() {
        let dir = std::env::temp_dir().join(format!("wa-share-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap_or_default();
        let listener = bind(&dir).unwrap_or_else(|e| panic!("bind : {e}"));
        let mode = std::fs::metadata(socket_path(&dir))
            .map(|m| m.permissions().mode() & 0o777)
            .unwrap_or_default();
        assert_eq!(mode, 0o600);

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

        // Un socket orphelin (process tué) est remplacé sans erreur.
        drop(listener);
        assert!(bind(&dir).is_ok());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
