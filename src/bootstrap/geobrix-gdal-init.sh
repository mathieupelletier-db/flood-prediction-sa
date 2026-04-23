#!/bin/bash
# Flood demo - self-contained GeoBrix v0.2.0 init script
#
# Installs GDAL native libraries, downloads the GeoBrix JAR + GDAL JNI shared
# object from the public GitHub release, and stages them for the Spark driver
# and executors. Does NOT require a Databricks Volume (it fetches artifacts
# at cluster startup), which keeps this demo deployable on workspaces whose
# default storage binding can't create managed volumes.
set -euxo pipefail

GEOBRIX_VERSION="0.2.0"
RELEASE="https://github.com/databrickslabs/geobrix/releases/download/v${GEOBRIX_VERSION}"

# ---- apt sources + ubuntugis PPA ----
sudo add-apt-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-backports main universe multiverse restricted"
sudo add-apt-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-updates    main universe multiverse restricted"
sudo add-apt-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)-security   main multiverse restricted universe"
sudo add-apt-repository -y "deb http://archive.ubuntu.com/ubuntu $(lsb_release -sc)            main multiverse restricted universe"

sudo apt-get install -y software-properties-common
sudo add-apt-repository -y ppa:ubuntugis/ubuntugis-unstable
sudo apt-get update -y

# ---- GDAL natives ----
sudo apt-get -o DPkg::Lock::Timeout=-1 install -y \
    unixodbc libcurl3-gnutls libsnappy-dev libopenjp2-7
sudo apt-get -o DPkg::Lock::Timeout=-1 install -y \
    libgdal-dev gdal-bin python3-gdal

# ---- Python bindings for GDAL ----
pip install --upgrade pip setuptools wheel cython
pip install wheel "setuptools==74.0.0" "numpy==2.1.3"
export GDAL_CONFIG=/usr/bin/gdal-config
pip install --no-cache-dir --force-reinstall "GDAL[numpy]==$(gdal-config --version).*"

# ---- GeoBrix JAR + GDAL JNI shared object (from GitHub releases) ----
TMP=$(mktemp -d)
cd "$TMP"
curl -fsSL -o geobrix.jar  "${RELEASE}/geobrix-${GEOBRIX_VERSION}-jar-with-dependencies.jar"
curl -fsSL -o libgdalalljni.so "${RELEASE}/libgdalalljni.so"

sudo cp libgdalalljni.so /usr/lib/libgdalalljni.so
sudo cp geobrix.jar       /databricks/jars/geobrix-${GEOBRIX_VERSION}.jar

cd / && rm -rf "$TMP"
echo "GeoBrix ${GEOBRIX_VERSION} init script finished."
