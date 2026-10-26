FROM mcr.microsoft.com/devcontainers/python:1-3.12-bookworm

# The base image ships a Yarn package source whose signing key has expired.
# It makes every "apt-get update" fail, so remove it before anything else runs.
RUN rm -f /etc/apt/sources.list.d/yarn.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
 && rm -rf /var/lib/apt/lists/*
