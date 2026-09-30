FROM mcr.microsoft.com/vscode/devcontainers/javascript-node:26

# Install dependencies, including clang through the LLVM APT repository. Note that this
# will also install lldb and clangd alongside dependencies.
ARG LLVM_VERSION=22
RUN export DEBIAN_FRONTEND=noninteractive \
    && apt-get update \
    && apt-get -y install --no-install-recommends tcl \
    && curl -fSsL -o /tmp/llvm.sh https://apt.llvm.org/llvm.sh \
    && bash /tmp/llvm.sh ${LLVM_VERSION} \
    && apt-get -y install --no-install-recommends libunwind-${LLVM_VERSION}-dev libc++-${LLVM_VERSION}-dev libc++abi-${LLVM_VERSION}-dev libclang-rt-${LLVM_VERSION}-dev -o DPkg::options::="--force-overwrite" \
    && rm -f /tmp/llvm.sh \
    && rm -rf /var/lib/apt/lists/*
ENV PATH="/usr/lib/llvm-${LLVM_VERSION}/bin:${PATH}"

# Install Bazel (via Bazelisk)
RUN npm install -g @bazel/bazelisk

# Install Just
RUN npm install -g rust-just
