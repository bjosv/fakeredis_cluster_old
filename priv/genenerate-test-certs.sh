#!/usr/bin/env bash
set -e

BASEDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )/configs"
cd "${BASEDIR}"

mkdir -p tls

# CA
openssl genrsa -out tls/ca.key 4096
openssl req \
    -x509 -new -nodes -sha256 \
    -key tls/ca.key \
    -days 3650 \
    -subj '/O=Eredis Test/CN=Certificate Authority' \
    -out tls/ca.crt

# Redis server
openssl genrsa -out tls/redis.key 2048
openssl req \
    -new -sha256 \
    -key tls/redis.key \
    -subj '/O=Eredis Test/CN=Server' | \
    openssl x509 \
        -req -sha256 \
        -CA tls/ca.crt \
        -CAkey tls/ca.key \
        -CAserial tls/ca.txt \
        -CAcreateserial \
        -days 365 \
        -out tls/redis.crt

# eredis client
openssl genrsa -out tls/client.key 2048
openssl req \
    -new -sha256 \
    -key tls/client.key \
    -subj '/O=Eredis Test/CN=Client' | \
    openssl x509 \
        -req -sha256 \
        -CA tls/ca.crt \
        -CAkey tls/ca.key \
        -CAserial tls/ca.txt \
        -CAcreateserial \
        -days 365 \
        -out tls/client.crt

# Make sure files are readable from a container
chmod 664 tls/*
