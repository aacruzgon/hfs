use std::fs;
use std::io::Write;

fn main() -> std::io::Result<()> {
    let content = fs::read_to_string("/home/slm/git/hfs/crates/fhir/src/r4.rs")?;
    let mut output = String::new();
    
    for line in content.lines() {
        // If the line has indentation but no comment markers, and it's not inside a string literal or other context
        if line.starts_with("    ") && 
           !line.trim_start().starts_with("//") && 
           !line.trim_start().starts_with("pub ") &&
           !line.trim_start().starts_with("#[") &&
           !line.trim_start().starts_with("}") &&
           !line.trim_start().starts_with("impl ") &&
           !line.trim().is_empty() {
            // This is likely a documentation line that's missing the /// prefix
            output.push_str("    /// ");
            output.push_str(line.trim_start());
            output.push_str("\n");
        } else {
            output.push_str(line);
            output.push_str("\n");
        }
    }
    
    fs::write("/home/slm/git/hfs/crates/fhir/src/r4.rs.fixed", output)?;
    println!("Fixed file written to r4.rs.fixed");
    
    Ok(())
}