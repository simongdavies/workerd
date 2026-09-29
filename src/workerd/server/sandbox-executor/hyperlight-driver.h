// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

#pragma once

#include <errno.h>
#include <fcntl.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <kj/array.h>
#include <kj/common.h>
#include <kj/debug.h>
#include <kj/string.h>

namespace workerd::server::sandbox_executor {

using DispatchFunction = int (*)(const uint8_t*, size_t);

static constexpr auto CALL_DEVICE = "/dev/hlcall";
static constexpr unsigned long CALL_MAX_LENGTH = _IOR('H', 1, uint64_t);

inline int callFd = -1;
inline int protocolOutputFd = STDOUT_FILENO;
inline uint8_t* callBuffer = nullptr;
inline size_t callBufferCapacity = 0;
inline constexpr size_t MAX_PROTOCOL_WRITE_BYTES = 1024;

inline bool inBuffer(size_t length, size_t offset, size_t amount) {
  return offset <= length && amount <= length - offset;
}

inline bool readU16(const uint8_t* buffer, size_t length, size_t offset, uint16_t& result) {
  if (!inBuffer(length, offset, 2)) return false;
  result = buffer[offset] | static_cast<uint16_t>(buffer[offset + 1]) << 8;
  return true;
}

inline bool readU32(const uint8_t* buffer, size_t length, size_t offset, uint32_t& result) {
  if (!inBuffer(length, offset, 4)) return false;
  result = buffer[offset] | static_cast<uint32_t>(buffer[offset + 1]) << 8 |
      static_cast<uint32_t>(buffer[offset + 2]) << 16 |
      static_cast<uint32_t>(buffer[offset + 3]) << 24;
  return true;
}

inline size_t rootTable(const uint8_t* buffer, size_t length) {
  uint32_t offset;
  if (!readU32(buffer, length, 4, offset)) return kj::maxValue;
  auto result = 4 + static_cast<size_t>(offset);
  return inBuffer(length, result, 4) ? result : kj::maxValue;
}

inline size_t vtable(const uint8_t* buffer, size_t length, size_t table) {
  uint32_t raw;
  if (!readU32(buffer, length, table, raw)) return kj::maxValue;
  auto signedOffset = static_cast<int32_t>(raw);
  if (signedOffset < 0 || static_cast<size_t>(signedOffset) > table) return kj::maxValue;
  auto result = table - static_cast<size_t>(signedOffset);
  return inBuffer(length, result, 4) ? result : kj::maxValue;
}

inline size_t field(const uint8_t* buffer, size_t length, size_t table, uint16_t vtableOffset) {
  auto tableVtable = vtable(buffer, length, table);
  uint16_t vtableSize;
  uint16_t fieldOffset;
  if (tableVtable == kj::maxValue || !readU16(buffer, length, tableVtable, vtableSize)) {
    return kj::maxValue;
  }
  if (vtableOffset >= vtableSize) return 0;
  if (!readU16(buffer, length, tableVtable + vtableOffset, fieldOffset)) {
    return kj::maxValue;
  }
  return fieldOffset;
}

inline size_t follow(const uint8_t* buffer, size_t length, size_t table, uint16_t vtableOffset) {
  auto fieldOffset = field(buffer, length, table, vtableOffset);
  if (fieldOffset == 0 || fieldOffset == kj::maxValue) return fieldOffset;
  auto pointer = table + fieldOffset;
  uint32_t relative;
  if (!readU32(buffer, length, pointer, relative)) return kj::maxValue;
  auto result = pointer + static_cast<size_t>(relative);
  return inBuffer(length, result, 4) ? result : kj::maxValue;
}

inline kj::Maybe<kj::ArrayPtr<const char>> functionName(const uint8_t* buffer, size_t length) {
  auto root = rootTable(buffer, length);
  auto string = root == kj::maxValue ? kj::maxValue : follow(buffer, length, root, 4);
  uint32_t stringLength;
  if (string == 0 || string == kj::maxValue || !readU32(buffer, length, string, stringLength) ||
      !inBuffer(length, string + 4, stringLength)) {
    return kj::none;
  }
  return kj::arrayPtr(
      reinterpret_cast<const char*>(buffer + string + 4), static_cast<size_t>(stringLength));
}

inline kj::Maybe<kj::ArrayPtr<const char>> firstStringArgument(
    const uint8_t* buffer, size_t length) {
  auto root = rootTable(buffer, length);
  auto parameters = root == kj::maxValue ? kj::maxValue : follow(buffer, length, root, 6);
  uint32_t count;
  if (parameters == 0 || parameters == kj::maxValue ||
      !readU32(buffer, length, parameters, count) || count == 0) {
    return kj::none;
  }

  auto firstPosition = parameters + 4;
  uint32_t firstOffset;
  if (!readU32(buffer, length, firstPosition, firstOffset)) return kj::none;
  auto parameter = firstPosition + static_cast<size_t>(firstOffset);

  auto typeField = field(buffer, length, parameter, 4);
  if (typeField == 0 || typeField == kj::maxValue || !inBuffer(length, parameter + typeField, 1) ||
      buffer[parameter + typeField] != 7) {
    return kj::none;
  }

  auto stringHolder = follow(buffer, length, parameter, 6);
  auto string = stringHolder == 0 || stringHolder == kj::maxValue
      ? kj::maxValue
      : follow(buffer, length, stringHolder, 4);
  uint32_t stringLength;
  if (string == 0 || string == kj::maxValue || !readU32(buffer, length, string, stringLength) ||
      !inBuffer(length, string + 4, stringLength)) {
    return kj::none;
  }
  return kj::arrayPtr(
      reinterpret_cast<const char*>(buffer + string + 4), static_cast<size_t>(stringLength));
}

inline bool nameEquals(const uint8_t* buffer, size_t length, kj::StringPtr expected) {
  KJ_IF_SOME(name, functionName(buffer, length)) {
    return name == expected;
  }
  return false;
}

inline int initializeDriver() {
  uint64_t capacity = 0;
  callFd = open(CALL_DEVICE, O_RDWR | O_CLOEXEC);
  if (callFd < 0) return 1;
  if (ioctl(callFd, CALL_MAX_LENGTH, &capacity) < 0 || capacity == 0 || capacity > SIZE_MAX) {
    return 1;
  }
  callBuffer = static_cast<uint8_t*>(malloc(static_cast<size_t>(capacity)));
  if (callBuffer == nullptr) return 1;
  callBufferCapacity = static_cast<size_t>(capacity);
  return 0;
}

inline int isolateProtocolOutput() {
  protocolOutputFd = dup(STDOUT_FILENO);
  if (protocolOutputFd < 0) return 1;

  auto nullFd = open("/dev/null", O_WRONLY | O_CLOEXEC);
  if (nullFd >= 0) {
    if (dup2(nullFd, STDOUT_FILENO) < 0 || dup2(nullFd, STDERR_FILENO) < 0) {
      close(nullFd);
      return 1;
    }
    close(nullFd);
  } else {
    close(STDOUT_FILENO);
    close(STDERR_FILENO);
  }
  return 0;
}

template <typename Write>
inline int writeAllWith(int fd, kj::ArrayPtr<const char> data, Write&& writeFunction) {
  while (data.size() > 0) {
    auto writeSize = kj::min(data.size(), MAX_PROTOCOL_WRITE_BYTES);
    auto amount = writeFunction(fd, data.begin(), writeSize);
    if (amount < 0 && errno == EINTR) continue;
    if (amount <= 0 || static_cast<size_t>(amount) > writeSize) return -1;
    data = data.slice(static_cast<size_t>(amount));
  }
  return 0;
}

inline int writeAll(int fd, kj::ArrayPtr<const char> data) {
  return writeAllWith(
      fd, data, [](int fd, const void* buffer, size_t size) { return write(fd, buffer, size); });
}

[[noreturn]] inline void runDriver(DispatchFunction dispatch) {
  for (;;) {
    auto amount = read(callFd, callBuffer, callBufferCapacity);
    if (amount < 0) {
      if (errno == EINTR) continue;
      _exit(1);
    }
    if (amount == 0) continue;
    int32_t status = dispatch(callBuffer, static_cast<size_t>(amount));
    if (status != 0) {
      if (write(callFd, &status, sizeof(status)) < 0) _exit(1);
    }
  }
}

}  // namespace workerd::server::sandbox_executor
