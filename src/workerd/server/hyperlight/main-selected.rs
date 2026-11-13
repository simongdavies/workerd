// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

fn main() -> std::process::ExitCode {
    let result = executor::Executor::new(std::collections::HashMap::default())
        .and_then(|engine| {
            engine.with_worker_admission_factory(executor::selected_worker_from_socket)
        })
        .and_then(|engine| {
            let args: Vec<_> = std::env::args().skip(1).collect();
            if args.is_empty() {
                return executor::run_with_executor(engine);
            }
            let admission = executor::protocol::decode_serve_args(&args)?;
            engine.serve_artifact(
                admission.length,
                admission.budget,
                &admission.digest,
                &admission.socket,
            )
        });
    match result {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("Selected Worker entry failed: {error:#}");
            std::process::ExitCode::FAILURE
        }
    }
}
