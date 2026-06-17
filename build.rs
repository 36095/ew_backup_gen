use std::fs;
use std::io::Result;
use std::path::Path;

fn main() -> Result<()> {
    // Leer la versión desde Cargo.toml
    let cargo_toml = fs::read_to_string("Cargo.toml")?;
    let version = cargo_toml
        .lines()
        .find(|line| line.starts_with("version = "))
        .and_then(|line| line.split('"').nth(1))
        .unwrap_or("0.0.0.0");

    // Convertir la versión semántica (ej: "0.1.1") a formato Windows (ej: "0,1,1,0")
    let version_parts: Vec<&str> = version.split('.').collect();
    let major = version_parts.get(0).unwrap_or(&"0");
    let minor = version_parts.get(1).unwrap_or(&"0");
    let patch = version_parts.get(2).unwrap_or(&"0");
    let build = version_parts.get(3).unwrap_or(&"0");
    
    let windows_version = format!("{},{},{},{}", major, minor, patch, build);

    // Generar el archivo .rc dinámicamente
    let rc_content = format!(r#"/**
 * ?: Tells the precompiler to use the UTF-8 code page to compile the resources
 * See: https://learn.microsoft.com/en-us/windows/win32/menurc/pragma-directives
 * See: https://learn.microsoft.com/en-us/windows/win32/intl/code-page-identifiers
 */
#pragma code_page(65001) // UTF-8
#include <winuser.h>

// Icono
IDI_ICON1 ICON "assets/icon.ico"

// Información de versión
1 VERSIONINFO
FILEVERSION {windows_version}
PRODUCTVERSION {windows_version}
FILEOS 0x4
FILETYPE 0x1
{{
    BLOCK "StringFileInfo"
    {{
        //?: Español (España)
        BLOCK "040A04B0"  // Español (España), UTF-8
        {{
            VALUE "CompanyName", "The Streamer Company SpA."
            VALUE "FileDescription", "Generador de Copias de Seguridad para EasyWorship 2009"
            VALUE "FileVersion", "{version}"
            VALUE "InternalName", "ew_backup_gen"
            VALUE "LegalCopyright", "Copyright © 2026 - The Streamer Company SpA. All rights reserved."
            VALUE "OriginalFilename", "ew_backup_gen.exe"
            VALUE "ProductName", "EW Backup Gen"
            VALUE "ProductVersion", "{version}"
            VALUE "Comments", "N/A"
        }}

        //?: Español (Latinoamérica / Internacional)
        BLOCK "040A04E4"  // Español (Latinoamérica), UTF-8
        {{
            VALUE "CompanyName", "The Streamer Company SpA."
            VALUE "FileDescription", "Generador de Copias de Seguridad para EasyWorship 2009"
            VALUE "FileVersion", "{version}"
            VALUE "InternalName", "ew_backup_gen"
            VALUE "LegalCopyright", "Copyright © 2026 - The Streamer Company SpA. All rights reserved."
            VALUE "OriginalFilename", "ew_backup_gen.exe"
            VALUE "ProductName", "EW Backup Gen"
            VALUE "ProductVersion", "{version}"
            VALUE "Comments", "N/A"
        }}

        //?: Inglés (Estados Unidos) - como respaldo
        BLOCK "040904B0"  // Inglés (EE.UU.), UTF-8
        {{
            VALUE "CompanyName", "The Streamer Company SpA."
            VALUE "FileDescription", "EasyWorship 2009 Backup Generator"
            VALUE "FileVersion", "{version}"
            VALUE "InternalName", "ew_backup_gen"
            VALUE "LegalCopyright", "Copyright © 2026 - The Streamer Company SpA. All rights reserved."
            VALUE "OriginalFilename", "ew_backup_gen.exe"
            VALUE "ProductName", "EW Backup Gen"
            VALUE "ProductVersion", "{version}"
            VALUE "Comments", "N/A"
        }}
    }}
    BLOCK "VarFileInfo"
    {{
        // Lista de traducciones disponibles
        VALUE "Translation", 0x40A, 1200, 0x409, 1200 //?: Español (España), Español (LatAm), Inglés (EE.UU.), UTF-8
    }}
}}
"#, version = version, windows_version = windows_version);

    let out_dir = std::env::var("OUT_DIR").unwrap_or_else(|_| ".".to_string());
    let rc_path = Path::new(&out_dir).join("ew_backup_gen.rc");
    fs::write(&rc_path, rc_content)?;

    // Compilar el recurso generado
    let _ = embed_resource::compile(&rc_path, embed_resource::NONE);
    
    Ok(())
}
