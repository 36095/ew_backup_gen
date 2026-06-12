// src/main.rs
use chrono::{DateTime, Local, NaiveDateTime};
use config::{Config, File};
use dirs::data_local_dir;
use fs_extra::dir::{CopyOptions, copy};
use serde::{Deserialize, Serialize};
// use std::cmp::Ordering;
// use std::fs;
use std::path::{Path, PathBuf};
use tokio_cron_scheduler::{Job, JobScheduler};

const COMPANY_NAME: &str = "The Streamer Company SpA.";
const SERVICE_NAME: &str = "EW Backup Service";
const CONFIG_FILE_NAME: &str = "config.ini";

#[derive(Debug, Deserialize, Serialize)]
struct PathsConfig {
    source: String,
    destination_base: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct IntervalsConfig {
    min_interval_seconds: u64,
}

#[derive(Debug, Deserialize, Serialize)]
struct BehaviorConfig {
    on_startup: bool,
}

#[derive(Debug, Deserialize, Serialize)]
struct LimitsConfig {
    max_backups: Option<u32>,
}

#[derive(Debug, Deserialize, Serialize)]
struct AppConfig {
    paths: PathsConfig,
    intervals: IntervalsConfig,
    behavior: BehaviorConfig,
    #[serde(default = "default_limits")]
    limits: LimitsConfig,
}

fn default_limits() -> LimitsConfig {
    LimitsConfig {
        max_backups: Some(5),
    }
}

fn create_default_config_file<P: AsRef<Path>>(
    config_path: P,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent_dir) = config_path.as_ref().parent() {
        std::fs::create_dir_all(parent_dir)?;
    }

    let ini_content = r#"[paths]
source = "C:\\Users\\Public\\Documents\\Softouch"
destination_base = "C:\\Users\\Public\\Documents\backups\\"

[intervals]
min_interval_seconds = 3600

[behavior]
on_startup = true

[limits]
max_backups = 5
"#;

    std::fs::write(config_path.as_ref(), ini_content)?;
    println!(
        "Archivo de configuración predeterminado '{}' creado.",
        config_path.as_ref().display()
    );
    Ok(())
}

fn get_config_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let base_dir = data_local_dir()
        .ok_or("No se pudo determinar el directorio local de datos del usuario.")?;

    let config_dir = base_dir
        .join(COMPANY_NAME)
        .join(SERVICE_NAME)
        .join("Configs");
    std::fs::create_dir_all(&config_dir)?;

    Ok(config_dir.join(CONFIG_FILE_NAME))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Iniciando el servicio de copia de seguridad periódica (ew_backup_gen)...");

    let config_path = get_config_path()?;
    println!("Usando archivo de configuración: {}", config_path.display());

    if !config_path.exists() {
        println!(
            "No se encontró '{}'. Creando archivo de configuración predeterminado...",
            config_path.display()
        );
        create_default_config_file(&config_path)?;
    }

    let settings = Config::builder()
        .add_source(
            File::with_name(config_path.to_str().ok_or("Ruta de config inválida")?).required(true),
        )
        .build()?;

    let app_config: AppConfig = settings.try_deserialize()?;

    let source_path = PathBuf::from(&app_config.paths.source);
    let dest_base_path = PathBuf::from(&app_config.paths.destination_base);
    let min_interval_seconds = app_config.intervals.min_interval_seconds;
    let run_on_startup = app_config.behavior.on_startup;
    let max_backups_opt = app_config.limits.max_backups;

    if !source_path.exists() {
        eprintln!(
            "ERROR: La ruta de origen '{}' no existe.",
            source_path.display()
        );
        std::process::exit(1);
    }

    if !dest_base_path.exists() {
        std::fs::create_dir_all(&dest_base_path)?;
        println!(
            "Directorio de destino base creado: {}",
            dest_base_path.display()
        );
    }

    let last_run_file_path = dest_base_path.join(".ew_backup_gen_last_run");
    let should_run_initially =
        should_run_initially(&last_run_file_path, min_interval_seconds, run_on_startup)?;

    if should_run_initially {
        println!("Ejecutando copia de seguridad inicial...");
        if let Err(e) = execute_backup_and_update_timestamp(
            &source_path,
            &dest_base_path,
            &last_run_file_path,
            max_backups_opt,
        )
        .await
        {
            eprintln!("Error durante la copia de seguridad inicial: {}", e);
        }
    } else {
        println!(
            "Copia de seguridad inicial omitida (no ha pasado suficiente tiempo o no es inicio)."
        );
    }

    if !run_on_startup {
        let scheduler = JobScheduler::new().await?;
        let cron_expr_str = seconds_to_cron(min_interval_seconds);
        println!(
            "Configurando copia de seguridad periódica cada {} segundos ({}).",
            min_interval_seconds, cron_expr_str
        );

        let src_clone = source_path.clone();
        let dst_base_clone = dest_base_path.clone();
        let last_run_clone = last_run_file_path.clone();
        let max_backups_clone = max_backups_opt;

        let job = Job::new_cron_job_async(cron_expr_str, move |uuid, _l| {
            let src = src_clone.clone();
            let dst_base = dst_base_clone.clone();
            let last_run_file = last_run_clone.clone();
            let max_backups_job = max_backups_clone;
            Box::pin(async move {
                println!("Tarea de copia periódica (ID: {}) iniciada.", uuid);
                if should_run_check_file(&last_run_file, min_interval_seconds)
                    .await
                    .unwrap_or(false)
                {
                    if let Err(e) = execute_backup_and_update_timestamp(
                        &src,
                        &dst_base,
                        &last_run_file,
                        max_backups_job,
                    )
                    .await
                    {
                        eprintln!("Error en tarea de copia periódica (Tarea {}): {}", uuid, e);
                    }
                } else {
                    println!(
                        "Tarea de copia periódica (ID: {}) omitida, no ha pasado suficiente tiempo.",
                        uuid
                    );
                }
            })
        })?;

        scheduler.add(job).await?;
        scheduler.start().await?;
    } else {
        println!("Modo 'on_startup' activo. El programa terminará después de la copia inicial.");
    }

    if !run_on_startup {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
        }
    }

    Ok(())
}

fn should_run_initially(
    last_run_file: &Path,
    min_interval: u64,
    is_on_startup: bool,
) -> Result<bool, Box<dyn std::error::Error>> {
    if is_on_startup {
        return Ok(true);
    }

    if !last_run_file.exists() {
        return Ok(true);
    }

    if let Ok(metadata) = std::fs::metadata(last_run_file) {
        if let Ok(modified_time) = metadata.modified() {
            let modified_datetime: DateTime<Local> = modified_time.into();
            let now = Local::now();
            let elapsed_duration = now.signed_duration_since(modified_datetime);

            if elapsed_duration.num_seconds() >= min_interval as i64 {
                return Ok(true);
            }
        }
    } else {
        eprintln!(
            "Advertencia: No se pudo leer el archivo de último backup '{}'. Procediendo con la copia.",
            last_run_file.display()
        );
        return Ok(true);
    }

    Ok(false)
}

async fn should_run_check_file(
    last_run_file: &Path,
    min_interval: u64,
) -> Result<bool, Box<dyn std::error::Error>> {
    if !last_run_file.exists() {
        return Ok(true);
    }

    if let Ok(metadata) = tokio::fs::metadata(last_run_file).await
        && let Ok(modified_time) = metadata.modified() {
            let modified_datetime: DateTime<Local> = modified_time.into();
            let now = Local::now();
            let elapsed_duration = now.signed_duration_since(modified_datetime);

            if elapsed_duration.num_seconds() >= min_interval as i64 {
                return Ok(true);
            }
        }

    Ok(false)
}

async fn execute_backup_and_update_timestamp(
    source: &Path,
    dest_base: &Path,
    last_run_file: &Path,
    max_backups_opt: Option<u32>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(max_backups) = max_backups_opt
        && max_backups > 0 {
            cleanup_old_backups(dest_base, max_backups).await?;
        }

    let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
    let dest_path = dest_base.join(format!("backup_{}", timestamp));

    let options = CopyOptions::new().overwrite(true).copy_inside(false);

    println!(
        "Copiando de '{}' a '{}'",
        source.display(),
        dest_path.display()
    );

    let source_buf = source.to_path_buf();
    let dest_path_buf = dest_path.clone();

    let result = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&dest_path_buf)?;
        let copy_result = copy(&source_buf, &dest_path_buf, &options);

        if copy_result.is_err()
            && let Err(remove_err) = std::fs::remove_dir_all(&dest_path_buf) {
                eprintln!(
                    "Advertencia: Error al eliminar directorio de backup incompleto '{:?}': {}",
                    dest_path_buf, remove_err
                );
            }
        copy_result
    })
    .await;

    match result {
        Ok(copy_operation_result) => {
            copy_operation_result?;
        }
        Err(join_error) => {
            return Err(Box::new(join_error));
        }
    }

    tokio::fs::write(last_run_file, Local::now().to_rfc3339()).await?;
    println!(
        "Copia de seguridad completada y archivo de último backup actualizado: {:?}",
        last_run_file
    );

    Ok(())
}

async fn cleanup_old_backups(
    dest_base: &Path,
    max_backups: u32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut backup_entries: Vec<(PathBuf, NaiveDateTime)> = Vec::new();

    for entry in std::fs::read_dir(dest_base)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir()
            && let Some(file_name) = path.file_name().and_then(|n| n.to_str())
                && file_name.starts_with("backup_") {
                    // El formato es backup_YYYYMMDD_HHMMSS
                    if let Some(timestamp_part) = file_name.strip_prefix("backup_")
                        && let Ok(parsed_time) =
                            NaiveDateTime::parse_from_str(timestamp_part, "%Y%m%d_%H%M%S")
                        {
                            backup_entries.push((path, parsed_time));
                        }
                }
    }

    // Ordenar por fecha de backup ascendente (el más antiguo primero)
    backup_entries.sort_by(|a, b| a.1.cmp(&b.1));

    // Calcular cuántos exceden el límite DESPUÉS de crear el nuevo backup
    let num_existing = backup_entries.len();
    let num_to_remove = (num_existing + 1).saturating_sub(max_backups as usize);

    // Eliminar los más antiguos
    for (path_to_delete, _) in backup_entries.iter().take(num_to_remove) {
        println!("Eliminando backup antiguo: {:?}", path_to_delete);
        tokio::task::spawn_blocking({
            let path_clone = path_to_delete.clone();
            move || std::fs::remove_dir_all(&path_clone)
        })
        .await??;
    }

    Ok(())
}

fn seconds_to_cron(seconds: u64) -> String {
    if seconds < 60 {
        println!(
            "Advertencia: Intervalo de {} segundos es menor a 1 minuto. Programando para cada minuto (podría fallar en algunas implementaciones de cron).",
            seconds
        );
        return "*/1 * * * * *".to_string();
    }
    let minutes = seconds / 60;
    if minutes < 60 {
        format!("0 */{} * * * *", minutes)
    } else {
        let hours = minutes / 60;
        if hours > 23 {
            let effective_hours = hours % 24;
            if effective_hours == 0 {
                "0 0 */1 * * *".to_string()
            } else {
                format!("0 0 */{} * * *", effective_hours)
            }
        } else {
            format!("0 0 */{} * * *", hours)
        }
    }
}
