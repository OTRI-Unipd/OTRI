FROM python:3.7-slim

COPY requirements.txt /
RUN buildDeps='build-essential gcc gfortran python3-dev libpq-dev' \
    && apt-get update \
    && apt-get install -y $buildDeps --no-install-recommends\
    && CFLAGS="-g0 -Wl,--strip-all -I/usr/include:/usr/local/include -L/usr/lib:/usr/local/lib" \
        pip install \
        --no-cache-dir \
        --compile \
        --global-option=build_ext \
        -r /requirements.txt \
    && apt-get purge -y --auto-remove $buildDeps \
    && rm -rf /var/lib/apt/lists/*
    
#RUN pip install -r /requirements.txt --no-cache-dir --compile

COPY . /app
WORKDIR /app