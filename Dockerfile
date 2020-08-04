FROM python:3.7-slim

# libxml2-dev libxslt-dev 
COPY requirements.txt /
RUN buildDeps="build-essential gcc gfortran python3-dev" \
    && apt-get update \
    && apt-get install -y $buildDeps --no-install-recommends \
    && apt-get install -y libpq-dev  --no-install-recommends \
    && CFLAGS="-g0 -Wl,--strip-all -I/usr/include:/usr/local/include -L/usr/lib:/usr/local/lib" \
        pip install \
        --no-cache-dir \
        --compile \
        --global-option=build_ext \
        -r /requirements.txt \
    && apt-get purge -y --auto-remove $buildDeps \
    && rm -rf /var/lib/apt/lists/*

COPY . /app
WORKDIR /app