use std::io::Result;

fn main() -> Result<()> {
    let _ = embed_resource::compile("ew_backup_gen.rc", embed_resource::NONE);
    Ok(())
}
