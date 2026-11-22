use std::process::ExitCode;

#[expect(
    clippy::print_stderr,
    reason = "stub until the node uses Nautilus logging"
)]
fn main() -> ExitCode {
    eprintln!("The NautilusTrader catalog node has not been implemented yet.");
    ExitCode::FAILURE
}
