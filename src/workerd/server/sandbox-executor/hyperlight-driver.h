// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Hyperlight Authors.

#pragma once

#include <errno.h>
#include <fcntl.h>
#include <stddef.h>
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
static constexpr uint32_t HLCALL_HOSTCALL_ABI_VERSION = 1;
static constexpr uint32_t HLCALL_HOSTCALL_MAX_ARGS = 4;
static constexpr uint64_t HLCALL_IOC_MAXLEN = 64 * 1024;

enum HlHostCallType : uint32_t {
  HLCALL_TYPE_I32 = 1,
  HLCALL_TYPE_U64 = 2,
  HLCALL_TYPE_STRING = 3,
  HLCALL_TYPE_VECBYTES = 4,
};

struct hlcall_host_arg {
  uint32_t type;
  uint32_t reserved;
  uint64_t value;
  const void* data;
  uint64_t len;
};

struct hlcall_host_call {
  uint32_t version;
  uint32_t return_type;
  const char* function;
  uint64_t function_len;
  uint32_t arg_count;
  uint32_t reserved;
  hlcall_host_arg args[HLCALL_HOSTCALL_MAX_ARGS];
  void* output;
  uint64_t output_cap;
  uint64_t output_len;
  int32_t output_i32;
  uint32_t reserved2;
  uint64_t output_u64;
};

static constexpr unsigned long HLCALL_IOC_HOSTCALL = _IOWR('H', 3, struct hlcall_host_call);

static_assert(sizeof(hlcall_host_arg) == 32);
static_assert(offsetof(hlcall_host_arg, type) == 0);
static_assert(offsetof(hlcall_host_arg, value) == 8);
static_assert(offsetof(hlcall_host_arg, data) == 16);
static_assert(offsetof(hlcall_host_arg, len) == 24);
static_assert(sizeof(hlcall_host_call) == 200);
static_assert(offsetof(hlcall_host_call, function) == 8);
static_assert(offsetof(hlcall_host_call, args) == 32);
static_assert(offsetof(hlcall_host_call, output) == 160);
static_assert(offsetof(hlcall_host_call, output_i32) == 184);
static_assert(offsetof(hlcall_host_call, output_u64) == 192);

inline int callFd = -1;
inline int protocolOutputFd = STDOUT_FILENO;
inline uint8_t* callBuffer = nullptr;
inline size_t callBufferCapacity = 0;
inline constexpr size_t MAX_PROTOCOL_WRITE_BYTES = 1024;

inline hlcall_host_arg hostCallU64(uint64_t value) {
  return {
    .type = HLCALL_TYPE_U64,
    .reserved = 0,
    .value = value,
    .data = nullptr,
    .len = 0,
  };
}

inline hlcall_host_arg hostCallStringArg(kj::StringPtr value) {
  return {
    .type = HLCALL_TYPE_STRING,
    .reserved = 0,
    .value = 0,
    .data = value.begin(),
    .len = value.size(),
  };
}

inline hlcall_host_arg hostCallBytesArg(kj::ArrayPtr<const kj::byte> value) {
  return {
    .type = HLCALL_TYPE_VECBYTES,
    .reserved = 0,
    .value = 0,
    .data = value.begin(),
    .len = value.size(),
  };
}

inline void invokeHostCall(hlcall_host_call& call) {
  KJ_REQUIRE(callFd >= 0, "Hyperlight call device is not initialized");
  KJ_REQUIRE(call.version == HLCALL_HOSTCALL_ABI_VERSION, "invalid Hyperlight host-call version");
  KJ_REQUIRE(call.function_len > 0 && call.function_len <= 128,
      "invalid Hyperlight host-call function name");
  KJ_REQUIRE(call.arg_count <= HLCALL_HOSTCALL_MAX_ARGS, "too many Hyperlight host-call arguments");
  for (size_t i = 0; i < call.arg_count; ++i) {
    KJ_REQUIRE(call.args[i].len <= HLCALL_IOC_MAXLEN, "Hyperlight host-call argument is too large");
  }
  KJ_REQUIRE(call.output_cap <= HLCALL_IOC_MAXLEN, "Hyperlight host-call output is too large");

  if (ioctl(callFd, HLCALL_IOC_HOSTCALL, &call) < 0) {
    if (errno == ENOTTY) {
      KJ_FAIL_REQUIRE(
          "Hyperlight kernel does not support the guest userspace host-call ABI", strerror(errno));
    }
    KJ_FAIL_REQUIRE("Hyperlight host call failed", strerror(errno));
  }
  KJ_REQUIRE(call.output_len <= call.output_cap, "Hyperlight host call returned oversized output");
}

inline hlcall_host_call makeHostCall(
    kj::StringPtr function, uint32_t returnType, kj::ArrayPtr<const hlcall_host_arg> args) {
  hlcall_host_call call = {};
  call.version = HLCALL_HOSTCALL_ABI_VERSION;
  call.return_type = returnType;
  call.function = function.begin();
  call.function_len = function.size();
  call.arg_count = args.size();
  for (auto i: kj::indices(args)) {
    call.args[i] = args[i];
  }
  return call;
}

inline kj::String hostCallString(kj::StringPtr function, kj::ArrayPtr<const hlcall_host_arg> args) {
  auto output = kj::heapArray<char>(HLCALL_IOC_MAXLEN + 1);
  auto call = makeHostCall(function, HLCALL_TYPE_STRING, args);
  call.output = output.begin();
  call.output_cap = HLCALL_IOC_MAXLEN;
  invokeHostCall(call);
  auto result = kj::heapArray<char>(call.output_len + 1);
  memcpy(result.begin(), output.begin(), call.output_len);
  result[call.output_len] = '\0';
  return kj::String(kj::mv(result));
}

inline kj::Array<kj::byte> hostCallBytes(
    kj::StringPtr function, kj::ArrayPtr<const hlcall_host_arg> args, size_t outputCapacity) {
  KJ_REQUIRE(outputCapacity <= HLCALL_IOC_MAXLEN, "Hyperlight byte result capacity is too large");
  auto output = kj::heapArray<kj::byte>(outputCapacity);
  auto call = makeHostCall(function, HLCALL_TYPE_VECBYTES, args);
  call.output = output.begin();
  call.output_cap = output.size();
  invokeHostCall(call);
  auto result = kj::heapArray<kj::byte>(call.output_len);
  memcpy(result.begin(), output.begin(), call.output_len);
  return result;
}

inline int32_t hostCallI32(kj::StringPtr function, kj::ArrayPtr<const hlcall_host_arg> args) {
  auto call = makeHostCall(function, HLCALL_TYPE_I32, args);
  invokeHostCall(call);
  return call.output_i32;
}

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
