FROM ubuntu:22.04

# Avoid interactive prompts during package installation
ENV DEBIAN_FRONTEND=noninteractive \
    ZPOOL_IMPORT_PATH=/var/lib/zfs

# Install required packages for ZFS and testing
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    zfsutils-linux \
    gzip \
    pigz \
    pv \
    util-linux \
    kmod \
    udev \
    && rm -rf /var/lib/apt/lists/*

# Set up ZFS test environment
RUN mkdir -p /etc/zfs \
    && mkdir -p /etc/modules-load.d \
    && echo 'zfs' > /etc/modules-load.d/zfs.conf \
    && echo 'zfs' >> /etc/modules \
    && mkdir -p ${ZPOOL_IMPORT_PATH} \
    && touch /etc/zfs/zpool.cache

# Create a working directory
WORKDIR /app

# Copy the main script and test files
COPY zfs_simple_backup_restore.py .
COPY tests/suites/ ./tests/suites/
COPY tests/test_entrypoint.sh ./entrypoint.sh
COPY README.md .

# Make scripts executable
RUN chmod +x tests/suites/*.py entrypoint.sh

# Create a volume for ZFS data
VOLUME ["${ZPOOL_IMPORT_PATH}"]

# Health check to verify ZFS is working
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD zpool status || exit 1

ENTRYPOINT ["/bin/bash", "./entrypoint.sh"]
