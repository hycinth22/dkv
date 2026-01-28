# 第一阶段：构建阶段
FROM ubuntu:22.04 AS builder

# 设置环境变量
ENV DEBIAN_FRONTEND=noninteractive

# 安装构建依赖
RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    git \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 复制项目文件
COPY . .

# 创建构建目录并构建项目
RUN mkdir -p build/release && \
    cd build/release && \
    cmake -DCMAKE_BUILD_TYPE=Release ../../ && \
    cmake --build . -- -j$(nproc)

# 第二阶段：运行阶段
FROM ubuntu:22.04 AS runtime

# 设置环境变量
ENV DEBIAN_FRONTEND=noninteractive

# 安装运行时依赖
RUN apt-get update && apt-get install -y \
    libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

# 设置工作目录
WORKDIR /app

# 从构建阶段复制构建产物
COPY --from=builder /app/build/release/bin/dkv_server /app/
COPY --from=builder /app/config.conf /app/
COPY --from=builder /app/lib/release/libdkv_script.so /usr/lib/

# 设置动态库路径
ENV LD_LIBRARY_PATH=/usr/lib

# 暴露端口
EXPOSE 6379

# 启动服务器
CMD ["./dkv_server"]