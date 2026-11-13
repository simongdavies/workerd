// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

fn main() -> std::process::ExitCode {
    match executor::run() {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Hyperlight executor: {error}");
            std::process::ExitCode::FAILURE
        }
    }
}
