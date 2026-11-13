use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use nix::unistd::mkfifo;
use workerd_capnp::config;

use super::*;

static NEXT_FIXTURE: AtomicU64 = AtomicU64::new(0);

struct Directory(PathBuf);

impl Directory {
    fn new() -> Self {
        let base = std::env::var_os("TEST_TMPDIR").map_or_else(std::env::temp_dir, PathBuf::from);
        let path = base.join(format!(
            "config-artifact-{}-{}",
            std::process::id(),
            NEXT_FIXTURE.fetch_add(1, Ordering::Relaxed),
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn file(&self) -> PathBuf {
        self.0.join(CONFIG_FILE)
    }
}

impl Drop for Directory {
    fn drop(&mut self) {
        match std::fs::symlink_metadata(self.file()) {
            Ok(metadata) if metadata.is_dir() => std::fs::remove_dir(self.file()).unwrap(),
            Ok(_) => std::fs::remove_file(self.file()).unwrap(),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => panic!("fixture metadata: {error}"),
        }
        std::fs::remove_dir(&self.0).unwrap();
    }
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    digest::digest(&digest::SHA256, bytes)
        .as_ref()
        .try_into()
        .unwrap()
}

#[test]
fn admitted_regular_file_has_exact_size_and_digest() {
    let directory = Directory::new();
    let bytes = b"12345678";
    std::fs::write(directory.file(), bytes).unwrap();
    assert_eq!(
        from_directory(&directory.0, 8, 16, &sha256(bytes)).unwrap(),
        bytes
    );
    assert!(from_directory(&directory.0, 8, 16, &[0; 32]).is_err());
    assert!(from_directory(&directory.0, 16, 16, &sha256(bytes)).is_err());
    assert!(from_directory(&directory.0, 8, 7, &sha256(bytes)).is_err());
}

#[test]
fn missing_symlink_directory_and_fifo_are_refused_without_fallback() {
    let directory = Directory::new();
    assert!(from_directory(&directory.0, 8, 8, &[0; 32]).is_err());
    std::os::unix::fs::symlink("missing", directory.file()).unwrap();
    assert!(from_directory(&directory.0, 8, 8, &[0; 32]).is_err());
    std::fs::remove_file(directory.file()).unwrap();
    std::fs::create_dir(directory.file()).unwrap();
    assert!(from_directory(&directory.0, 8, 8, &[0; 32]).is_err());
    std::fs::remove_dir(directory.file()).unwrap();
    mkfifo(&directory.file(), Mode::S_IRUSR | Mode::S_IWUSR).unwrap();
    assert!(from_directory(&directory.0, 8, 8, &[0; 32]).is_err());
}

#[test]
fn admitted_parent_directory_is_not_followed_through_a_symlink() {
    let directory = Directory::new();
    std::fs::write(directory.file(), b"12345678").unwrap();
    let link = directory.0.with_extension("link");
    std::os::unix::fs::symlink(&directory.0, &link).unwrap();
    let result = from_directory(&link, 8, 8, &sha256(b"12345678"));
    std::fs::remove_file(link).unwrap();
    assert!(result.is_err());
}

#[test]
fn exact_owned_read_refuses_short_extra_or_wrong_digest_bytes() {
    let expected = sha256(b"12345678");
    assert!(read_owned(&mut Cursor::new(b"1234567"), 8, 8, &expected).is_err());
    assert!(read_owned(&mut Cursor::new(b"123456789"), 8, 8, &expected).is_err());
    assert!(read_owned(&mut Cursor::new(b"12345678"), 8, 8, &[0; 32]).is_err());
    assert_eq!(
        read_owned(&mut Cursor::new(b"12345678"), 8, 8, &expected).unwrap(),
        b"12345678"
    );
}

#[test]
fn invalid_budget_is_refused_before_opening_or_allocating() {
    for (length, budget) in [(0, 8), (7, 8), (16, 8), (8, usize::MAX)] {
        assert!(from_directory(Path::new("does-not-exist"), length, budget, &[0; 32]).is_err());
    }
}

const TARGET_CONFIG_BYTES: usize = 8 * 1024 * 1024;
const FIXTURE_MODULE_LIMIT: usize = 1024 * 1024;
const PARTS: usize = 7;

fn padded_module(mut source: String) -> String {
    let padding = FIXTURE_MODULE_LIMIT.checked_sub(source.len() + 4).unwrap();
    source.push_str("/*");
    source.push_str(&"x".repeat(padding));
    source.push_str("*/");
    assert_eq!(source.len(), FIXTURE_MODULE_LIMIT);
    source
}

fn large_config() -> (Vec<u8>, Vec<usize>) {
    let mut source = String::new();
    for index in 1..=PARTS {
        source.push_str(&format!(
            "import {{ value as v{index} }} from './part-{index:02}.js';\n"
        ));
    }
    let sum = (1..=PARTS)
        .map(|index| format!("v{index}"))
        .collect::<Vec<_>>()
        .join("+");
    source.push_str(&format!(
        "export default {{ fetch() {{ return new Response('artifact:' + ({sum})); }} }};"
    ));
    let mut sources = vec![("main.js".to_owned(), padded_module(source))];
    for index in 1..=PARTS {
        sources.push((
            format!("part-{index:02}.js"),
            padded_module(format!("export const value = {index};\n")),
        ));
    }
    let sizes: Vec<_> = sources.iter().map(|(_, source)| source.len()).collect();
    let mut message = capnp::message::Builder::new_default();
    {
        let mut root = message.init_root::<config::Builder>();
        let mut service = root.reborrow().init_services(1).get(0);
        service.set_name("artifact-app");
        let mut worker = service.init_worker();
        worker.set_compatibility_date("2026-10-19");
        let mut modules = worker.init_modules(u32::try_from(sources.len()).unwrap());
        for (index, (name, source)) in sources.iter().enumerate() {
            let mut module = modules.reborrow().get(u32::try_from(index).unwrap());
            module.set_name(name);
            module.set_es_module(source);
        }
        let mut socket = root.init_sockets(1).get(0);
        socket.set_name("http");
        socket.reborrow().init_http();
        socket.init_service().set_name("artifact-app");
    }
    (capnp::serialize::write_message_to_words(&message), sizes)
}

#[test]
fn large_artifact_loads_and_executes_standard_modules() {
    // V8 cannot be disposed and initialized again in the finite fixture's process.
    if std::env::var_os("WORKERD_ARTIFACT_TEST_CHILD").is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "artifact::tests::large_artifact_loads_and_executes_standard_modules",
                "--nocapture",
            ])
            .env("WORKERD_ARTIFACT_TEST_CHILD", "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        print!("{stdout}");
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success());
        assert!(stdout.contains("test result: ok. 1 passed;"));
        return;
    }
    let (bytes, sizes) = large_config();
    assert!(bytes.len() >= TARGET_CONFIG_BYTES);
    assert_eq!(sizes.len(), PARTS + 1);
    assert!(sizes.len() <= 32);
    assert!(sizes.iter().all(|&size| size <= FIXTURE_MODULE_LIMIT));
    let total_sources: usize = sizes.iter().sum();
    assert_eq!(total_sources, TARGET_CONFIG_BYTES);
    let expected = sha256(&bytes);
    let directory = Directory::new();
    std::fs::write(directory.file(), &bytes).unwrap();
    let loaded = from_directory(&directory.0, bytes.len(), 16 * 1024 * 1024, &expected).unwrap();
    let mut executor = crate::Executor::new(HashMap::new()).unwrap();
    assert!(executor.configure(b"12345678", "http", 4096).is_err());
    executor.configure(&loaded, "http", 4096).unwrap();
    let response = executor
        .fetch(b"GET / HTTP/1.1\r\nHost: example.test\r\n\r\n".to_vec())
        .unwrap();
    assert!(response.ends_with(b"artifact:28"));
    executor.close().unwrap();
    if let Some(output) = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR") {
        let output = PathBuf::from(output);
        std::fs::write(output.join("large.config.bin"), &bytes).unwrap();
        let hex = expected.iter().fold(String::new(), |mut hex, byte| {
            use std::fmt::Write;
            write!(hex, "{byte:02x}").unwrap();
            hex
        });
        std::fs::write(output.join("large.config.json"), format!(
            "{{\"config_bytes\":{},\"sha256\":\"{hex}\",\"modules\":{},\"module_bytes_max\":{},\"module_bytes_total\":{},\"socket\":\"http\",\"response\":\"artifact:28\"}}\n",
            bytes.len(), sizes.len(), sizes.iter().max().unwrap(), total_sources,
        )).unwrap();
    }
}
