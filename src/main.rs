use chrono::{DateTime, Duration, Local, NaiveDateTime};
use colored::*; // Importar la crate colored
use config::{Config, File};
use dirs::data_local_dir;
use fs_extra::dir::{CopyOptions, copy};
use serde::{Deserialize, Serialize};
use std::collections::BinaryHeap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use tokio_cron_scheduler::{Job, JobScheduler};

const COMPANY_NAME: &str = "The Streamer Company SpA.";
const SERVICE_NAME: &str = "EW Backup Service";
const CONFIG_FILE_NAME: &str = "config.ini";

static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

#[derive(Debug, Deserialize, Serialize)]
struct PathsConfig {
    source: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct IntervalsConfig {
    min_interval_seconds: Option<u64>,
    days: Option<u64>,
    hours: Option<u64>,
    minutes: Option<u64>,
    seconds: Option<u64>,
    cron: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct BehaviorConfig {
    on_startup: bool,
    use_cron: Option<bool>,
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

// --- Nueva estructura para el resultado de should_run_check_file ---
#[derive(Debug)]
struct ShouldRunResult {
    should_run: bool,
    time_remaining: String, // Representación legible del tiempo restante
}

fn default_limits() -> LimitsConfig {
    LimitsConfig {
        max_backups: Some(5),
    }
}

fn get_service_base_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let base_dir = data_local_dir()
        .ok_or("No se pudo determinar el directorio local de datos del usuario.")?;

    Ok(base_dir.join(COMPANY_NAME).join(SERVICE_NAME))
}

fn get_config_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let service_base = get_service_base_path()?;
    let config_dir = service_base.join("Configs");
    std::fs::create_dir_all(&config_dir)?;
    Ok(config_dir.join(CONFIG_FILE_NAME))
}

fn get_backups_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    let service_base = get_service_base_path()?;
    let backups_dir = service_base.join("Backups");
    std::fs::create_dir_all(&backups_dir)?;
    Ok(backups_dir)
}

fn create_default_config_file<P: AsRef<Path>>(
    config_path: P,
) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent_dir) = config_path.as_ref().parent() {
        std::fs::create_dir_all(parent_dir)?;
    }

    let ini_content = r#"[paths]
source = "C:/Users/Public/Documents/Softouch"

[intervals]
# Opción 1: Usar valores legibles (días, horas, minutos, segundos)
# Si se definen estos, se calcula el intervalo total en segundos.
# days = 0
# hours = 0
# minutes = 30
# seconds = 0

# Opción 2: Usar una expresión cron explícita (tiene precedencia si use_cron = true)
# Formato: segundos minutos horas dia_mes mes dia_semana
# cron = "0 */30 * * * *" # Cada 30 minutos (segundo 0)

# Opción 3: (Legacy) Intervalo en segundos (menos precisa, se convierte a minutos/horas/días si es posible)
# min_interval_seconds = 1800 # Equivale a 30 minutos

[behavior]
on_startup = true
# Si use_cron es true, se ignora el cálculo de intervalo legible y se usa el campo 'cron'.
use_cron = false

[limits]
max_backups = 5
"#;

    std::fs::write(config_path.as_ref(), ini_content)?;
    println!(
        "{} \"{}\" {}",
        "Archivo de configuración predeterminado".green().bold(),
        config_path.as_ref().display(),
        "creado.".green().bold()
    );
    Ok(())
}

fn setup_ctrlc_handler() {
    ctrlc::set_handler(move || {
        println!("{}", "Ctrl+C recibido. Solicitud de apagado...".yellow());
        SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
    })
    .expect("Error al establecer el manejador de Ctrl+C");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{} (ew_backup_gen)...",
        "Iniciando el servicio de copia de seguridad periódica"
            .blue()
            .bold()
    );

    setup_ctrlc_handler();

    let config_path = get_config_path()?;
    println!(
        "{} \"{}\"",
        "Usando archivo de configuración:".cyan(),
        config_path.display()
    );

    if !config_path.exists() {
        println!(
            "{} \"{}\". {}",
            "No se encontró".red().bold(),
            config_path.display(),
            "Creando archivo de configuración predeterminado...".yellow()
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
    let dest_base_path = get_backups_path()?;
    let (execution_interval_seconds, cron_expression) = calculate_execution_interval(
        &app_config.intervals,
        app_config.behavior.use_cron.unwrap_or(false),
    )?;
    let run_on_startup = app_config.behavior.on_startup;
    let max_backups_opt = app_config.limits.max_backups;

    if !source_path.exists() {
        eprintln!(
            "{} \"{}\" {}",
            "ERROR: La ruta de origen".red().bold(),
            source_path.display(),
            "no existe.".red().bold()
        );
        std::process::exit(1);
    }

    let last_run_file_path = dest_base_path.join("last_run.ebg");

    let should_run_initially = should_run_initially(
        &last_run_file_path,
        execution_interval_seconds,
        run_on_startup,
    )?;

    if should_run_initially {
        println!("{}", "Ejecutando copia de seguridad inicial...".green());
        if let Err(e) = execute_backup_and_update_timestamp(
            &source_path,
            &dest_base_path,
            &last_run_file_path,
            max_backups_opt,
        )
        .await
        {
            eprintln!(
                "{} \"{}\"",
                "Error durante la copia de seguridad inicial:".red().bold(),
                e
            );
        }
    } else {
        println!(
            "{}",
            "Copia de seguridad inicial omitida (no ha pasado suficiente tiempo o no es inicio)."
                .yellow()
        );
    }

    if !run_on_startup {
        if app_config.behavior.use_cron.unwrap_or(false) {
            if let Some(cron_expr_str) = cron_expression {
                println!(
                    "{} \"{}\"",
                    "Configurando copia de seguridad periódica con expresión cron:".cyan(),
                    cron_expr_str
                );
                let mut scheduler = JobScheduler::new().await?;

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
                        if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
                            println!(
                                "{} (ID: {}) {}",
                                "Tarea de copia periódica".red(),
                                uuid,
                                "cancelada debido a solicitud de apagado.".red()
                            );
                            return; // <-- Salir silenciosamente si se apaga
                        }
                        println!(
                            "{} (ID: {})",
                            "Tarea de copia periódica iniciada.".green(),
                            uuid
                        );
                        // Manejar el Result de should_run_check_file dentro del job
                        match should_run_check_file(&last_run_file, execution_interval_seconds)
                            .await
                        {
                            Ok(should_run_result) => {
                                if should_run_result.should_run {
                                    if let Err(e) = execute_backup_and_update_timestamp(
                                        &src,
                                        &dst_base,
                                        &last_run_file,
                                        max_backups_job,
                                    )
                                    .await
                                    {
                                        eprintln!(
                                            "{} (Tarea {}): \"{}\"",
                                            "Error en tarea de copia periódica".red().bold(),
                                            uuid,
                                            e
                                        );
                                    }
                                } else {
                                    println!(
                                        "{} (ID: {})",
                                        format!("Tarea de copia periódica omitida, no ha pasado suficiente tiempo. Tiempo restante estimado: {}.", should_run_result.time_remaining).yellow(),
                                        uuid
                                    );
                                }
                            }
                            Err(e) => {
                                // Si should_run_check_file falla, registramos el error y posiblemente procedemos con la copia
                                eprintln!(
                                    "{} (Tarea {}): \"{}\"",
                                    "Error al verificar si se debe ejecutar la tarea de copia periódica (cron)".red().bold(),
                                    uuid,
                                    e
                                );
                                // Opcional: Intentar la copia de todos modos si no se puede leer el archivo de control
                                // if let Err(e_copy) = execute_backup_and_update_timestamp(...) { ... }
                            }
                        }
                        // El job no retorna un Result, por lo que no usamos Ok(())
                    })
                })?;

                scheduler.add(job).await?;
                scheduler.start().await?;

                loop {
                    if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
                        println!(
                            "{}",
                            "Bucle principal (cron) detectó solicitud de apagado.".yellow()
                        );
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }

                println!("{}", "Iniciando limpieza y apagado (cron)...".yellow());
                scheduler.shutdown().await?;
                println!("{}", "Scheduler detenido (cron).".yellow());
            } else {
                eprintln!(
                    "{}",
                    "Advertencia: 'use_cron' es true, pero no se proporcionó un campo 'cron'. Usando temporizador interno.".yellow()
                );
            }
        } else {
            println!(
                "{} {} {}",
                "Configurando copia de seguridad periódica cada".cyan(),
                execution_interval_seconds,
                "segundos (temporizador interno).".cyan()
            );

            // Nuevo mensaje para indicar el tiempo del temporizador
            let formatted_duration = format_duration_for_display(execution_interval_seconds);
            println!(
                "{} {}",
                "Tiempo configurado para el temporizador interno:"
                    .yellow()
                    .bold(),
                formatted_duration.yellow().bold()
            );

            let src_clone = source_path.clone();
            let dst_base_clone = dest_base_path.clone();
            let last_run_clone = last_run_file_path.clone();
            let max_backups_clone = max_backups_opt;

            let duration = std::time::Duration::from_secs(execution_interval_seconds);
            let mut interval_timer = tokio::time::interval(duration);

            loop {
                tokio::select! {
                    _ = interval_timer.tick() => {
                         if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
                             println!("{}", "Temporizador interno detectó solicitud de apagado.".yellow());
                             break; // Salir del bucle
                         }
                         println!("{}", "Temporizador interno: Ejecutando copia de seguridad periódica...".green());
                         // Manejar el Result de should_run_check_file dentro del loop del timer
                         match should_run_check_file(&last_run_clone, execution_interval_seconds).await {
                            Ok(should_run_result) => {
                                if should_run_result.should_run {
                                    if let Err(e) = execute_backup_and_update_timestamp(
                                       &src_clone,
                                       &dst_base_clone,
                                       &last_run_clone,
                                       max_backups_clone,
                                    )
                                    .await
                                    {
                                       eprintln!(
                                           "{} \"{}\"",
                                           "Error en tarea de copia periódica (temporizador interno):".red().bold(),
                                           e
                                       );
                                    }
                                } else {
                                    println!(
                                       "{}",
                                       format!("Tarea de copia periódica omitida (temporizador interno), no ha pasado suficiente tiempo. Tiempo restante estimado: {}.", should_run_result.time_remaining).yellow()
                                    );
                                }
                            }
                            Err(e) => {
                                // Manejar el error aquí también
                                eprintln!(
                                   "{} \"{}\"",
                                   "Error al verificar si se debe ejecutar la tarea de copia periódica (temporizador interno):".red().bold(),
                                   e
                                );
                            }
                         }
                    },
                    _ = tokio::signal::ctrl_c() => {
                        if SHUTDOWN_REQUESTED.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst).is_ok() {
                            println!("{}", "Ctrl+C recibido (desde tokio::signal::ctrl_c). Solicitud de apagado...".yellow());
                        }
                    }
                }

                if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
                    println!(
                        "{}",
                        "Bucle principal (temporizador) detectó solicitud de apagado.".yellow()
                    );
                    break;
                }
            }
        }
    } else {
        println!(
            "{}",
            "Modo 'on_startup' activo. El programa terminará después de la copia inicial.".yellow()
        );
    }

    if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
        println!("{}", "Programa finalizado por señal de apagado.".yellow());
    } else {
        println!("{}", "Programa finalizado normalmente.".green());
    }

    Ok(())
}

fn calculate_execution_interval(
    intervals_cfg: &IntervalsConfig,
    use_cron_setting: bool,
) -> Result<(u64, Option<String>), Box<dyn std::error::Error>> {
    if use_cron_setting {
        if let Some(ref cron_expr) = intervals_cfg.cron {
            println!(
                "{} \"{}\"",
                "Usando expresión cron explícita:".cyan(),
                cron_expr
            );
            return Ok((0, Some(cron_expr.clone())));
        } else {
            eprintln!(
                "{}",
                "Advertencia: 'use_cron' es true, pero no se proporcionó un campo 'cron'. Usando cálculo legible o fallback.".yellow()
            );
        }
    }

    let days = intervals_cfg.days.unwrap_or(0);
    let hours = intervals_cfg.hours.unwrap_or(0);
    let minutes = intervals_cfg.minutes.unwrap_or(0);
    let seconds = intervals_cfg.seconds.unwrap_or(0);

    let total_seconds = days * 24 * 60 * 60 + hours * 60 * 60 + minutes * 60 + seconds;

    if total_seconds == 0 {
        if let Some(legacy_seconds) = intervals_cfg.min_interval_seconds {
            println!(
                "{} \"{}\"",
                "Usando intervalo legacy 'min_interval_seconds':".cyan(),
                legacy_seconds
            );
            return Ok((legacy_seconds, None));
        } else {
            eprintln!(
                "{}",
                "Advertencia: No se encontró un intervalo válido (legible ni legacy). Usando valor por defecto de 3600 segundos (1 hora).".yellow()
            );
            return Ok((3600, None));
        }
    }

    println!(
        "{} {} {}, {} {}, {} {}, {} {} ({}: {} {})",
        "Usando intervalo calculado:".cyan(),
        days,
        "días,".cyan(),
        hours,
        "horas,".cyan(),
        minutes,
        "minutos,".cyan(),
        seconds,
        "segundos".cyan(),
        "total".cyan(),
        total_seconds,
        "segundos".cyan()
    );
    Ok((total_seconds, None))
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
            "{} \"{}\". {}",
            "Advertencia: No se pudo leer el archivo de último backup".yellow(),
            last_run_file.display(),
            "Procediendo con la copia.".yellow()
        );
        return Ok(true);
    }

    Ok(false)
}

async fn should_run_check_file(
    last_run_file: &Path,
    min_interval: u64,
) -> Result<ShouldRunResult, Box<dyn std::error::Error + Send + Sync>> {
    let now = Local::now();

    if !last_run_file.exists() {
        return Ok(ShouldRunResult {
            should_run: true,
            time_remaining: "".to_string(),
        }); // No hay tiempo restante si no existe
    }

    if let Ok(metadata) = tokio::fs::metadata(last_run_file).await
        && let Ok(modified_time) = metadata.modified()
    {
        let modified_datetime: DateTime<Local> = modified_time.into();
        let elapsed_duration = now.signed_duration_since(modified_datetime);

        if elapsed_duration.num_seconds() >= min_interval as i64 {
            return Ok(ShouldRunResult {
                should_run: true,
                time_remaining: "".to_string(),
            });
        } else {
            // Calcular el tiempo restante
            let remaining_seconds = min_interval as i64 - elapsed_duration.num_seconds();
            // let remaining_duration = Duration::seconds(remaining_seconds); // No es necesario crear un Duration para segundos
            let readable_remaining = format_duration_for_display(remaining_seconds as u64);
            // Opcional: Mostrar también la fecha/hora estimada
            let estimated_next_run = now + Duration::seconds(remaining_seconds);
            let readable_estimated_time =
                estimated_next_run.format("%Y-%m-%d %H:%M:%S").to_string();
            let detailed_info = format!(
                "{} (estimado para {}) ",
                readable_remaining, readable_estimated_time
            );

            return Ok(ShouldRunResult {
                should_run: false,
                time_remaining: detailed_info,
            });
        }
    }

    // Si hay un error al leer el archivo, asumimos que se debe ejecutar
    eprintln!(
        "{} \"{}\". {}",
        "Advertencia: No se pudo leer el archivo de último backup".yellow(),
        last_run_file.display(),
        "Procediendo con la copia.".yellow()
    );
    Ok(ShouldRunResult {
        should_run: true,
        time_remaining: "".to_string(),
    })
}

async fn execute_backup_and_update_timestamp(
    source: &Path,
    dest_base: &Path,
    last_run_file: &Path,
    max_backups_opt: Option<u32>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if let Some(max_backups) = max_backups_opt
        && max_backups > 0
    {
        cleanup_old_backups(dest_base, max_backups).await?;
    }

    let timestamp = Local::now().format("%Y%m%d_%H%M%S").to_string();
    let dest_path = dest_base.join(format!("backup_{}", timestamp));

    let options = CopyOptions::new().overwrite(true).copy_inside(false);

    println!(
        "{} \"{}\" {} \"{}\"",
        "Copiando de".green(),
        source.display(),
        "a".green(),
        dest_path.display()
    );

    let source_buf = source.to_path_buf();
    let dest_path_buf = dest_path.clone();

    let result = tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&dest_path_buf)?;
        let copy_result = copy(&source_buf, &dest_path_buf, &options);

        if copy_result.is_err()
            && let Err(remove_err) = std::fs::remove_dir_all(&dest_path_buf)
        {
            eprintln!(
                "{} \"{}\": \"{}\"",
                "Advertencia: Error al eliminar directorio de backup incompleto".yellow(),
                dest_path_buf.display(),
                remove_err
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
        "{} \"{}\"",
        "Copia de seguridad completada y archivo de último backup actualizado:"
            .green()
            .bold(),
        last_run_file.display()
    );

    Ok(())
}

async fn cleanup_old_backups(
    dest_base: &Path,
    max_backups: u32,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut backup_entries: BinaryHeap<(std::cmp::Reverse<NaiveDateTime>, PathBuf)> =
        BinaryHeap::new();

    for entry in std::fs::read_dir(dest_base)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir()
            && let Some(file_name) = path.file_name().and_then(|n| n.to_str())
            && file_name.starts_with("backup_")
            && let Some(timestamp_part) = file_name.strip_prefix("backup_")
            && let Ok(parsed_time) = NaiveDateTime::parse_from_str(timestamp_part, "%Y%m%d_%H%M%S")
        {
            backup_entries.push((std::cmp::Reverse(parsed_time), path));
        }
    }

    while backup_entries.len() > max_backups as usize {
        if let Some((_, path_to_delete)) = backup_entries.pop() {
            println!(
                "{} \"{}\"",
                "Eliminando backup antiguo:".red(),
                path_to_delete.display()
            );
            tokio::task::spawn_blocking({
                let path_clone = path_to_delete.clone();
                move || std::fs::remove_dir_all(&path_clone)
            })
            .await??;
        }
    }

    Ok(())
}

fn format_duration_for_display(seconds: u64) -> String {
    let days = seconds / (24 * 60 * 60);
    let hours = (seconds % (24 * 60 * 60)) / (60 * 60);
    let minutes = (seconds % (60 * 60)) / 60;
    let secs = seconds % 60;

    let mut parts = Vec::new();
    if days > 0 {
        parts.push(format!("{}d", days));
    }
    if hours > 0 {
        parts.push(format!("{}h", hours));
    }
    if minutes > 0 {
        parts.push(format!("{}m", minutes));
    }
    if secs > 0 || parts.is_empty() {
        // Mostrar 0s si es 0 y no hay otras partes
        parts.push(format!("{}s", secs));
    }

    parts.join(" ")
}
