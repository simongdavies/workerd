#pragma once

#include <workerd/io/worker-fs.h>

namespace workerd::server::sandbox_executor {

// Opens the fixed guest path /mnt/workerd-storage/<name>. The caller must validate `name` as a
// single logical storage identifier before calling this function.
kj::Rc<Directory> newHostStorageDirectory(kj::StringPtr name, bool writable);

}  // namespace workerd::server::sandbox_executor
