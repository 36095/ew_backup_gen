use std::io::Result;

fn main() -> Result<()> {
    let _ = embed_resource::compile("resources.rc", embed_resource::NONE);
    Ok(())
}
