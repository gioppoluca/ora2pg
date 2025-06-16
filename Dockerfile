# -------------------------------
# Builder Stage: Compile oracle_fdw
# -------------------------------
FROM ghcr.io/cloudnative-pg/postgresql:17.4 AS builder

LABEL maintainer="your_name@domain.com" \
      description="Builder image for oracle_fdw with Oracle Instant Client" \
      version="1.0"

ENV PG_MAJOR=17
ENV ORACLE_VERSION=19_26
ENV INSTANT_CLIENT_DIR=/opt/oracle/instantclient

USER root

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl lsb-release build-essential libaio1 libaio-dev git unzip \
      postgresql-server-dev-${PG_MAJOR}

# Add Dalibo Labs repository and install anonymizer (if needed for build)
RUN echo "deb http://apt.dalibo.org/labs $(lsb_release -cs)-dalibo main" > /etc/apt/sources.list.d/dalibo-labs.list && \
    curl -fsSL -o /etc/apt/trusted.gpg.d/dalibo-labs.gpg https://apt.dalibo.org/labs/debian-dalibo.gpg && \
    apt-get update && \
    apt-get install -y --no-install-recommends locales-all \
      postgresql_anonymizer_${PG_MAJOR}

# Install Oracle Instant Client Basic + SDK
RUN curl -L -H "Cookie: oraclelicense=accept-securebackup-cookie" -o /tmp/instantclient-basic.zip \
      https://download.oracle.com/otn_software/linux/instantclient/1926000/instantclient-basic-linux.x64-19.26.0.0.0dbru.zip && \
    curl -L -H "Cookie: oraclelicense=accept-securebackup-cookie" -o /tmp/instantclient-sdk.zip \
      https://download.oracle.com/otn_software/linux/instantclient/1926000/instantclient-sdk-linux.x64-19.26.0.0.0dbru.zip && \
    mkdir -p /opt/oracle && \
    unzip -o /tmp/instantclient-basic.zip -d /opt/oracle && \
    unzip -o /tmp/instantclient-sdk.zip -d /opt/oracle && \
    ln -sf /opt/oracle/instantclient_${ORACLE_VERSION} ${INSTANT_CLIENT_DIR} && \
    echo "${INSTANT_CLIENT_DIR}" > /etc/ld.so.conf.d/oracle-instantclient.conf && \
    ldconfig && \
    rm /tmp/instantclient-*.zip

# Environment for oracle_fdw compilation
ENV OCI_LIB_DIR=${INSTANT_CLIENT_DIR}
ENV OCI_INCLUDE_DIR=${INSTANT_CLIENT_DIR}/sdk/include
ENV ORACLE_HOME=${INSTANT_CLIENT_DIR}
ENV LD_LIBRARY_PATH=${INSTANT_CLIENT_DIR}

# Clone and compile oracle_fdw
RUN git clone https://github.com/laurenz/oracle_fdw.git /tmp/oracle_fdw && \
    cd /tmp/oracle_fdw && \
    make && make install && \
    rm -rf /tmp/oracle_fdw

# Clean build dependencies
RUN apt-get purge -y --auto-remove build-essential git unzip && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/*

# -------------------------------
# Final Stage: Runtime Image
# -------------------------------
FROM ghcr.io/cloudnative-pg/postgresql:17.4

LABEL maintainer="your_name@domain.com" \
      description="PostgreSQL runtime with oracle_fdw, anonymizer, Python 3.8+, and data tools" \
      version="1.0"

ENV PG_MAJOR=17
ENV ORACLE_VERSION=19_26
ENV INSTANT_CLIENT_DIR=/opt/oracle/instantclient

USER root

# Install runtime dependencies (minimal set)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      curl lsb-release libaio1 libaio-dev python3 python3-pip python3-distutils python3-venv && \
    ln -sf python3 /usr/bin/python && \
    echo "deb http://apt.dalibo.org/labs $(lsb_release -cs)-dalibo main" > /etc/apt/sources.list.d/dalibo-labs.list && \
    curl -fsSL -o /etc/apt/trusted.gpg.d/dalibo-labs.gpg https://apt.dalibo.org/labs/debian-dalibo.gpg && \
    apt-get update && \
    apt-get install -y --no-install-recommends locales-all \
      postgresql_anonymizer_${PG_MAJOR} && \
    apt-get purge -y --auto-remove && \
    rm -rf /var/lib/apt/lists/* /var/cache/apt/*

# Install Python libraries
RUN pip3 install --no-cache-dir \
    oracledb psycopg2-binary openpyxl beautifulsoup4

# Copy Oracle Instant Client from builder
COPY --from=builder /opt/oracle /opt/oracle
RUN ln -sf /opt/oracle/instantclient_${ORACLE_VERSION} ${INSTANT_CLIENT_DIR} && \
    echo "${INSTANT_CLIENT_DIR}" > /etc/ld.so.conf.d/oracle-instantclient.conf && ldconfig

# Copy oracle_fdw extension
COPY --from=builder /usr/lib/postgresql /usr/lib/postgresql
COPY --from=builder /usr/share/postgresql/17/extension /usr/share/postgresql/17/extension
COPY --from=builder /usr/share/doc/postgresql-doc-17/extension /usr/share/doc/postgresql-doc-17/extension
COPY oracle_analyzer.py /usr/local/bin/oracle_analyzer.py
RUN chmod +x /usr/local/bin/oracle_analyzer.py

USER 26
