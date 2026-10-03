#include "host-fs.h"

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <kj/map.h>

namespace workerd::server::sandbox_executor {
namespace {

FsError fromErrno(int error) {
  switch (error) {
    case EROFS:
      return FsError::READ_ONLY;
    case EDQUOT:
      return FsError::QUOTA_EXCEEDED;
    case EACCES:
    case EPERM:
      return FsError::NOT_PERMITTED;
    case ENOENT:
      return FsError::NOT_FOUND;
    case ENOTDIR:
      return FsError::NOT_DIRECTORY;
    case EEXIST:
      return FsError::ALREADY_EXISTS;
    case ENOTEMPTY:
      return FsError::NOT_EMPTY;
    case EMFILE:
    case ENFILE:
      return FsError::TOO_MANY_OPEN_FILES;
    case EFBIG:
      return FsError::FILE_SIZE_LIMIT_EXCEEDED;
    case EINVAL:
    case ENAMETOOLONG:
      return FsError::INVALID_PATH;
    default:
      return FsError::FAILED;
  }
}

kj::Date toDate(const timespec& value) {
  return kj::UNIX_EPOCH + value.tv_sec * kj::SECONDS + value.tv_nsec * kj::NANOSECONDS;
}

bool validEntryName(kj::StringPtr name) {
  return name.size() > 0 && name != "."_kj && name != ".."_kj && name.findFirst('/') == kj::none;
}

class HostFile final: public File {
 public:
  HostFile(int fd, bool writable): fd(fd), writable(writable) {
    struct stat info;
    KJ_REQUIRE(fstat(fd, &info) == 0 && S_ISREG(info.st_mode), "invalid host storage file");
    uniqueId = kj::str("hostfs:", info.st_dev, ":", info.st_ino);
  }

  ~HostFile() noexcept(false) {
    close(fd);
  }

  kj::Maybe<FsError> setLastModified(jsg::Lock&, kj::Date date) override {
    if (!writable) return FsError::READ_ONLY;
    auto nanos = date - kj::UNIX_EPOCH;
    timespec times[2] = {
      {.tv_sec = 0, .tv_nsec = UTIME_OMIT},
      {
        .tv_sec = nanos / kj::SECONDS,
        .tv_nsec = (nanos % kj::SECONDS) / kj::NANOSECONDS,
      },
    };
    if (futimens(fd, times) < 0) return fromErrno(errno);
    return kj::none;
  }

  Stat stat(jsg::Lock&) override {
    auto result = tryStatImpl();
    KJ_IF_SOME(info, result.tryGet<Stat>()) {
      return info;
    }
    KJ_FAIL_REQUIRE("host storage stat failed");
  }

  kj::OneOf<FsError, Stat> tryStat(jsg::Lock&) override {
    return tryStatImpl();
  }

  uint32_t read(jsg::Lock&, uint32_t offset, kj::ArrayPtr<kj::byte> buffer) const override {
    auto result = tryReadImpl(offset, buffer);
    KJ_IF_SOME(amount, result.tryGet<uint32_t>()) {
      return amount;
    }
    KJ_FAIL_REQUIRE("host storage read failed");
  }

  kj::OneOf<FsError, uint32_t> tryRead(
      jsg::Lock&, uint32_t offset, kj::ArrayPtr<kj::byte> buffer) const override {
    return tryReadImpl(offset, buffer);
  }

  kj::OneOf<FsError, uint32_t> write(
      jsg::Lock&, uint32_t offset, kj::ArrayPtr<const kj::byte> data) override {
    if (!writable) return FsError::READ_ONLY;
    size_t total = 0;
    while (total < data.size()) {
      auto amount = pwrite(fd, data.begin() + total, data.size() - total, offset + total);
      if (amount < 0 && errno == EINTR) continue;
      if (amount < 0) return fromErrno(errno);
      if (amount == 0) return FsError::FAILED;
      total += static_cast<size_t>(amount);
    }
    return static_cast<uint32_t>(total);
  }

  kj::Maybe<FsError> fill(
      jsg::Lock& js, kj::byte value, kj::Maybe<uint32_t> offset = kj::none) override {
    if (!writable) return FsError::READ_ONLY;
    auto statResult = tryStat(js);
    KJ_IF_SOME(error, statResult.tryGet<FsError>()) {
      return error;
    }
    auto info = KJ_ASSERT_NONNULL(statResult.tryGet<Stat>());
    auto position = offset.orDefault(0);
    if (position >= info.size) return kj::none;
    byte buffer[4096];
    memset(buffer, value, sizeof(buffer));
    while (position < info.size) {
      auto amount = kj::min<size_t>(sizeof(buffer), info.size - position);
      auto result = write(js, position, kj::arrayPtr(buffer, amount));
      KJ_SWITCH_ONEOF(result) {
        KJ_CASE_ONEOF(error, FsError) {
          return error;
        }
        KJ_CASE_ONEOF(written, uint32_t) {
          position += written;
        }
      }
    }
    return kj::none;
  }

  kj::Maybe<FsError> resize(jsg::Lock&, uint32_t size) override {
    if (!writable) return FsError::READ_ONLY;
    if (ftruncate(fd, size) < 0) return fromErrno(errno);
    return kj::none;
  }

  kj::StringPtr jsgGetMemoryName() const override {
    return "HostStorageFile"_kj;
  }

  size_t jsgGetMemorySelfSize() const override {
    return sizeof(HostFile);
  }

  void jsgGetMemoryInfo(jsg::MemoryTracker&) const override {}

  kj::OneOf<FsError, kj::Rc<File>> clone(jsg::Lock& js) override {
    auto statResult = tryStat(js);
    KJ_IF_SOME(error, statResult.tryGet<FsError>()) {
      return error;
    }
    auto info = KJ_ASSERT_NONNULL(statResult.tryGet<Stat>());
    auto copy = File::newWritable(js, info.size);
    byte buffer[4096];
    uint32_t offset = 0;
    while (offset < info.size) {
      auto readResult = tryRead(
          js, offset, kj::arrayPtr(buffer, kj::min<size_t>(sizeof(buffer), info.size - offset)));
      KJ_IF_SOME(error, readResult.tryGet<FsError>()) {
        return error;
      }
      auto amount = KJ_ASSERT_NONNULL(readResult.tryGet<uint32_t>());
      if (amount == 0) return FsError::FAILED;
      auto result = copy->write(js, offset, kj::arrayPtr(buffer, amount));
      KJ_IF_SOME(error, result.tryGet<FsError>()) {
        return error;
      }
      offset += amount;
    }
    return kj::mv(copy);
  }

  kj::Maybe<FsError> replace(jsg::Lock& js, kj::Rc<File> file) override {
    if (!writable) return FsError::READ_ONLY;
    auto statResult = file->tryStat(js);
    KJ_IF_SOME(error, statResult.tryGet<FsError>()) {
      return error;
    }
    auto info = KJ_ASSERT_NONNULL(statResult.tryGet<Stat>());
    KJ_IF_SOME(error, resize(js, info.size)) {
      return error;
    }
    byte buffer[4096];
    uint32_t offset = 0;
    while (offset < info.size) {
      auto readResult = file->tryRead(
          js, offset, kj::arrayPtr(buffer, kj::min<size_t>(sizeof(buffer), info.size - offset)));
      KJ_IF_SOME(error, readResult.tryGet<FsError>()) {
        return error;
      }
      auto amount = KJ_ASSERT_NONNULL(readResult.tryGet<uint32_t>());
      if (amount == 0) return FsError::FAILED;
      auto result = write(js, offset, kj::arrayPtr(buffer, amount));
      KJ_IF_SOME(error, result.tryGet<FsError>()) {
        return error;
      }
      offset += amount;
    }
    return kj::none;
  }

  kj::StringPtr getUniqueId(jsg::Lock&) const override {
    return uniqueId;
  }

 private:
  int fd;
  bool writable;
  kj::String uniqueId;

  kj::OneOf<FsError, Stat> tryStatImpl() const {
    struct stat info;
    if (fstat(fd, &info) < 0) return fromErrno(errno);
    if (info.st_size < 0) return FsError::FAILED;
    return Stat{
      .type = FsType::FILE,
      .size = static_cast<uint32_t>(
          kj::min<uint64_t>(static_cast<uint64_t>(info.st_size), 0xffffffffu)),
      .lastModified = toDate(info.st_mtim),
      .created = toDate(info.st_ctim),
      .writable = writable,
    };
  }

  kj::OneOf<FsError, uint32_t> tryReadImpl(uint32_t offset, kj::ArrayPtr<kj::byte> buffer) const {
    size_t total = 0;
    while (total < buffer.size()) {
      auto amount = pread(fd, buffer.begin() + total, buffer.size() - total, offset + total);
      if (amount < 0 && errno == EINTR) continue;
      if (amount < 0) return fromErrno(errno);
      if (amount == 0) break;
      total += static_cast<size_t>(amount);
    }
    return static_cast<uint32_t>(total);
  }
};

class HostDirectory final: public Directory {
 public:
  HostDirectory(int fd, bool writable): fd(fd), writable(writable) {
    struct stat info;
    KJ_REQUIRE(fstat(fd, &info) == 0 && S_ISDIR(info.st_mode), "invalid host storage directory");
    uniqueId = kj::str("hostfs:", info.st_dev, ":", info.st_ino);
    loadEntries();
  }

  ~HostDirectory() noexcept(false) {
    close(fd);
  }

  kj::Maybe<kj::OneOf<FsError, Stat>> stat(jsg::Lock& js, kj::PathPtr path) override {
    if (path.size() == 0) {
      struct stat info;
      if (fstat(fd, &info) < 0) {
        return kj::OneOf<FsError, Stat>(fromErrno(errno));
      }
      return kj::OneOf<FsError, Stat>(Stat{
        .type = FsType::DIRECTORY,
        .size = 0,
        .lastModified = toDate(info.st_mtim),
        .created = toDate(info.st_ctim),
        .writable = writable,
      });
    }
    KJ_IF_SOME(node, tryOpen(js, path)) {
      KJ_SWITCH_ONEOF(node) {
        KJ_CASE_ONEOF(error, FsError) {
          return kj::OneOf<FsError, Stat>(error);
        }
        KJ_CASE_ONEOF(file, kj::Rc<File>) {
          return file->tryStat(js);
        }
        KJ_CASE_ONEOF(directory, kj::Rc<Directory>) {
          return KJ_ASSERT_NONNULL(directory->stat(js, nullptr));
        }
        KJ_CASE_ONEOF(link, kj::Rc<SymbolicLink>) {
          return kj::OneOf<FsError, Stat>(FsError::NOT_PERMITTED);
        }
      }
    }
    return kj::none;
  }

  size_t count(jsg::Lock&, kj::Maybe<FsType> typeFilter = kj::none) override {
    KJ_IF_SOME(type, typeFilter) {
      size_t result = 0;
      for (const auto& entry: entries) {
        KJ_SWITCH_ONEOF(entry.value) {
          KJ_CASE_ONEOF(file, kj::Rc<File>) {
            result += type == FsType::FILE;
          }
          KJ_CASE_ONEOF(directory, kj::Rc<Directory>) {
            result += type == FsType::DIRECTORY;
          }
          KJ_CASE_ONEOF(link, kj::Rc<SymbolicLink>) {
            result += type == FsType::SYMLINK;
          }
        }
      }
      return result;
    }
    return entries.size();
  }

  Entry* begin() override {
    return entries.begin();
  }
  Entry* end() override {
    return entries.end();
  }
  const Entry* begin() const override {
    return entries.begin();
  }
  const Entry* end() const override {
    return entries.end();
  }

  kj::Maybe<FsNodeWithError> tryOpen(
      jsg::Lock& js, kj::PathPtr path, OpenOptions options = {}) override {
    if (path.size() == 0) return FsNodeWithError(kj::Rc<Directory>(addRefToThis()));
    KJ_IF_SOME(entry, entries.find(path[0])) {
      if (path.size() == 1) {
        KJ_SWITCH_ONEOF(entry) {
          KJ_CASE_ONEOF(file, kj::Rc<File>) {
            return FsNodeWithError(file.addRef());
          }
          KJ_CASE_ONEOF(directory, kj::Rc<Directory>) {
            return FsNodeWithError(directory.addRef());
          }
          KJ_CASE_ONEOF(link, kj::Rc<SymbolicLink>) {
            return FsNodeWithError(FsError::NOT_PERMITTED);
          }
        }
      }
      KJ_SWITCH_ONEOF(entry) {
        KJ_CASE_ONEOF(file, kj::Rc<File>) {
          return FsNodeWithError(FsError::NOT_DIRECTORY);
        }
        KJ_CASE_ONEOF(directory, kj::Rc<Directory>) {
          return directory->tryOpen(js, path.slice(1, path.size()), kj::mv(options));
        }
        KJ_CASE_ONEOF(link, kj::Rc<SymbolicLink>) {
          return FsNodeWithError(FsError::NOT_PERMITTED);
        }
      }
    }

    KJ_IF_SOME(type, options.createAs) {
      if (!writable) return FsNodeWithError(FsError::READ_ONLY);
      if (path.size() == 1) return create(path[0], type);
      auto child = create(path[0], FsType::DIRECTORY);
      KJ_SWITCH_ONEOF(child) {
        KJ_CASE_ONEOF(error, FsError) {
          return FsNodeWithError(error);
        }
        KJ_CASE_ONEOF(file, kj::Rc<File>) {
          return FsNodeWithError(FsError::NOT_DIRECTORY);
        }
        KJ_CASE_ONEOF(directory, kj::Rc<Directory>) {
          return directory->tryOpen(js, path.slice(1, path.size()), kj::mv(options));
        }
        KJ_CASE_ONEOF(link, kj::Rc<SymbolicLink>) {
          return FsNodeWithError(FsError::NOT_PERMITTED);
        }
      }
    }
    return kj::none;
  }

  kj::Maybe<FsError> add(jsg::Lock& js, kj::StringPtr name, Item item) override {
    if (!writable) return FsError::READ_ONLY;
    if (!validEntryName(name)) return FsError::INVALID_PATH;
    if (entries.find(name) != kj::none) return FsError::ALREADY_EXISTS;

    KJ_SWITCH_ONEOF(item) {
      KJ_CASE_ONEOF(file, kj::Rc<File>) {
        auto created = create(name, FsType::FILE);
        KJ_IF_SOME(error, created.tryGet<FsError>()) {
          return error;
        }
        auto target = KJ_ASSERT_NONNULL(created.tryGet<kj::Rc<File>>()).addRef();
        KJ_IF_SOME(error, target->replace(js, file.addRef())) {
          auto path = kj::Path({name});
          remove(js, path);
          return error;
        }
        return kj::none;
      }
      KJ_CASE_ONEOF(directory, kj::Rc<Directory>) {
        auto created = create(name, FsType::DIRECTORY);
        KJ_IF_SOME(error, created.tryGet<FsError>()) {
          return error;
        }
        auto target = KJ_ASSERT_NONNULL(created.tryGet<kj::Rc<Directory>>()).addRef();
        for (auto& entry: *directory) {
          auto child = ([&]() -> Item {
            KJ_SWITCH_ONEOF(entry.value) {
              KJ_CASE_ONEOF(file, kj::Rc<File>) {
                return file.addRef();
              }
              KJ_CASE_ONEOF(directory, kj::Rc<Directory>) {
                return directory.addRef();
              }
              KJ_CASE_ONEOF(link, kj::Rc<SymbolicLink>) {
                return link.addRef();
              }
            }
            KJ_UNREACHABLE;
          })();
          KJ_IF_SOME(error, target->add(js, entry.key, kj::mv(child))) {
            auto path = kj::Path({name});
            remove(js, path, {.recursive = true});
            return error;
          }
        }
        return kj::none;
      }
      KJ_CASE_ONEOF(link, kj::Rc<SymbolicLink>) {
        return FsError::NOT_PERMITTED;
      }
    }
    KJ_UNREACHABLE;
  }

  kj::OneOf<FsError, bool> remove(
      jsg::Lock& js, kj::PathPtr path, RemoveOptions options = {}) override {
    if (!writable) return FsError::READ_ONLY;
    if (path.size() == 0) return false;
    KJ_IF_SOME(entry, entries.find(path[0])) {
      if (path.size() > 1) {
        KJ_IF_SOME(directory, entry.tryGet<kj::Rc<Directory>>()) {
          return directory->remove(js, path.slice(1, path.size()), options);
        }
        return FsError::NOT_DIRECTORY;
      }

      int flags = 0;
      KJ_IF_SOME(directory, entry.tryGet<kj::Rc<Directory>>()) {
        if (directory->count(js) > 0) {
          if (!options.recursive) return FsError::NOT_EMPTY;
          KJ_IF_SOME(error, removeContents(js, *directory)) {
            return error;
          }
        }
        flags = AT_REMOVEDIR;
      }
      auto name = kj::str(path[0]);
      if (unlinkat(fd, name.cStr(), flags) < 0) return fromErrno(errno);
      entries.erase(path[0]);
      return true;
    }
    return false;
  }

  kj::StringPtr jsgGetMemoryName() const override {
    return "HostStorageDirectory"_kj;
  }
  size_t jsgGetMemorySelfSize() const override {
    return sizeof(HostDirectory);
  }
  void jsgGetMemoryInfo(jsg::MemoryTracker& tracker) const override {
    for (const auto& entry: entries) {
      KJ_SWITCH_ONEOF(entry.value) {
        KJ_CASE_ONEOF(file, kj::Rc<File>) {
          tracker.trackField("file", *file);
        }
        KJ_CASE_ONEOF(directory, kj::Rc<Directory>) {
          tracker.trackField("directory", *directory);
        }
        KJ_CASE_ONEOF(link, kj::Rc<SymbolicLink>) {}
      }
    }
  }
  kj::StringPtr getUniqueId(jsg::Lock&) const override {
    return uniqueId;
  }

 private:
  int fd;
  bool writable;
  kj::HashMap<kj::String, Item> entries;
  kj::String uniqueId;

  void loadEntries() {
    auto duplicate = dup(fd);
    KJ_REQUIRE(duplicate >= 0, "failed to duplicate host storage directory");
    auto stream = fdopendir(duplicate);
    if (stream == nullptr) {
      close(duplicate);
      KJ_FAIL_REQUIRE("failed to enumerate host storage directory", strerror(errno));
    }
    KJ_DEFER(closedir(stream));
    errno = 0;
    while (auto entry = readdir(stream)) {
      auto name = kj::StringPtr(entry->d_name);
      if (!validEntryName(name)) continue;
      struct stat info;
      KJ_REQUIRE(fstatat(fd, entry->d_name, &info, AT_SYMLINK_NOFOLLOW) == 0,
          "failed to inspect host storage entry", entry->d_name, strerror(errno));
      if (S_ISREG(info.st_mode)) {
        auto child =
            openat(fd, entry->d_name, (writable ? O_RDWR : O_RDONLY) | O_CLOEXEC | O_NOFOLLOW);
        KJ_REQUIRE(child >= 0, "failed to open host storage file", entry->d_name, strerror(errno));
        kj::Rc<File> file = kj::rc<HostFile>(child, writable);
        entries.insert(kj::str(name), kj::mv(file));
      } else if (S_ISDIR(info.st_mode)) {
        auto child = openat(fd, entry->d_name, O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        KJ_REQUIRE(
            child >= 0, "failed to open host storage directory", entry->d_name, strerror(errno));
        kj::Rc<Directory> directory = kj::rc<HostDirectory>(child, writable);
        entries.insert(kj::str(name), kj::mv(directory));
      }
      errno = 0;
    }
    KJ_REQUIRE(errno == 0, "failed to enumerate host storage directory", strerror(errno));
  }

  FsNodeWithError create(kj::StringPtr rawName, FsType type) {
    if (!validEntryName(rawName)) return FsError::INVALID_PATH;
    auto name = kj::str(rawName);
    switch (type) {
      case FsType::FILE: {
        auto child =
            openat(fd, name.cStr(), O_RDWR | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0600);
        if (child < 0) return fromErrno(errno);
        kj::Rc<File> file = kj::rc<HostFile>(child, true);
        auto result = file.addRef();
        entries.insert(kj::mv(name), kj::mv(file));
        return kj::mv(result);
      }
      case FsType::DIRECTORY: {
        if (mkdirat(fd, name.cStr(), 0700) < 0) return fromErrno(errno);
        auto child = openat(fd, name.cStr(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
        if (child < 0) {
          auto error = errno;
          unlinkat(fd, name.cStr(), AT_REMOVEDIR);
          return fromErrno(error);
        }
        kj::Rc<Directory> directory = kj::rc<HostDirectory>(child, true);
        auto result = directory.addRef();
        entries.insert(kj::mv(name), kj::mv(directory));
        return kj::mv(result);
      }
      case FsType::SYMLINK:
        return FsError::NOT_PERMITTED;
    }
    KJ_UNREACHABLE;
  }

  kj::Maybe<FsError> removeContents(jsg::Lock& js, Directory& directory) {
    kj::Vector<kj::String> names;
    for (const auto& entry: directory) {
      names.add(kj::str(entry.key));
    }
    for (const auto& name: names) {
      auto result = directory.remove(js, kj::Path({name}), {.recursive = true});
      KJ_IF_SOME(error, result.tryGet<FsError>()) {
        return error;
      }
    }
    return kj::none;
  }
};

}  // namespace

kj::Rc<Directory> newHostStorageDirectory(kj::StringPtr name, bool writable) {
  auto path = kj::str("/mnt/workerd-storage/", name);
  auto fd = open(path.cStr(), O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  KJ_REQUIRE(fd >= 0, "failed to open configured host storage", name, strerror(errno));
  return kj::rc<HostDirectory>(fd, writable);
}

}  // namespace workerd::server::sandbox_executor
