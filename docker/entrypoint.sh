#!/usr/bin/env bash
# ==============================================================================
# Production Entrypoint: Flat File Decompression & Decryption Pod Executor
# Offloads CPU & Disk Intensive File Processing from Cloud Composer Workers to GKE
# ==============================================================================
set -e

echo "================================================================"
echo "  Flat File Decompression & Decryption Pod Executor"
echo "  Timestamp: $(date -u +'%Y-%m-%dT%H:%M:%SZ')"
echo "================================================================"

# Validate required environment variables
if [[ -z "${GCS_LANDING_URI}" || -z "${GCS_STAGE_URI}" || -z "${PROJECT_ID}" ]]; then
    echo "ERROR: Missing required environment variables (GCS_LANDING_URI, GCS_STAGE_URI, PROJECT_ID)."
    exit 1
fi

# Define local working directory inside dedicated /tmp volume
TMP_DIR="/tmp/flatfile_processor"
INBOUND_DIR="${TMP_DIR}/inbound"
DECRYPTED_DIR="${TMP_DIR}/decrypted"
UNCOMPRESSED_DIR="${TMP_DIR}/uncompressed"
KEYRING_DIR="${TMP_DIR}/.gnupg"

cleanup() {
    local exit_code=$?
    echo "----------------------------------------------------------------"
    echo "Executing cleanup trap (Exit code: ${exit_code})..."
    rm -rf "${TMP_DIR}"
    echo "Ephemeral workspace purged successfully."
    echo "----------------------------------------------------------------"
    exit ${exit_code}
}
trap cleanup EXIT

# Create required directories
mkdir -p "${INBOUND_DIR}" "${DECRYPTED_DIR}" "${UNCOMPRESSED_DIR}" "${KEYRING_DIR}"
chmod 700 "${KEYRING_DIR}"
export GNUPGHOME="${KEYRING_DIR}"

echo "[Step 1/5] Checking Secret Manager and importing GPG private key..."
if [[ -n "${SECRET_GPG_KEY_NAME}" ]]; then
    echo "Fetching secret '${SECRET_GPG_KEY_NAME}' from project '${PROJECT_ID}'..."
    if gcloud secrets versions access latest --secret="${SECRET_GPG_KEY_NAME}" --project="${PROJECT_ID}" > "${TMP_DIR}/private.key" 2>/dev/null; then
        gpg --batch --yes --import "${TMP_DIR}/private.key"
        rm -f "${TMP_DIR}/private.key"
        echo "GPG private key imported successfully into temporary keyring."
    else
        echo "WARNING: Secret '${SECRET_GPG_KEY_NAME}' could not be fetched or does not exist. Proceeding without GPG key import..."
    fi
else
    echo "No SECRET_GPG_KEY_NAME specified. Skipping GPG key import."
fi

echo "[Step 2/5] Downloading archives from Landing Bucket: ${GCS_LANDING_URI}..."
DOWNLOAD_SUCCESS=false
for attempt in $(seq 1 10); do
    if gcloud storage cp -r "${GCS_LANDING_URI}" "${INBOUND_DIR}/"; then
        DOWNLOAD_SUCCESS=true
        break
    fi
    echo "  Download attempt ${attempt}/10 failed (metadata/network warmup), retrying in 5s..."
    sleep 5
done

if [[ "${DOWNLOAD_SUCCESS}" != "true" ]]; then
    echo "ERROR: Failed to download from ${GCS_LANDING_URI} after 10 attempts!"
    exit 1
fi

FILE_COUNT=$(find "${INBOUND_DIR}" -type f | wc -l)
echo "Downloaded ${FILE_COUNT} file(s) into ${INBOUND_DIR}."
if [[ "${FILE_COUNT}" -eq 0 ]]; then
    echo "ERROR: No files found in ${GCS_LANDING_URI}!"
    exit 1
fi

echo "[Step 3/5] Decrypting and decompressing files..."
for file in $(find "${INBOUND_DIR}" -type f); do
    filename=$(basename "${file}")
    echo "--> Processing: ${filename} ($(du -h "${file}" | cut -f1))"
    
    current_file="${file}"
    
    # 1. Decrypt if .gpg or .pgp
    if [[ "${filename}" == *.gpg || "${filename}" == *.pgp ]]; then
        decrypted_name="${filename%.*}"
        echo "    Decrypting GPG payload -> ${decrypted_name}..."
        gpg --batch --yes --decrypt --output "${DECRYPTED_DIR}/${decrypted_name}" "${file}"
        current_file="${DECRYPTED_DIR}/${decrypted_name}"
        filename="${decrypted_name}"
    fi

    # 2. Decompress based on file extension
    case "${filename}" in
        *.zip)
            echo "    Extracting Zip archive -> ${UNCOMPRESSED_DIR}..."
            unzip -q -o "${current_file}" -d "${UNCOMPRESSED_DIR}"
            ;;
        *.tar.gz|*.tgz)
            echo "    Extracting Tarball -> ${UNCOMPRESSED_DIR}..."
            tar -xzf "${current_file}" -C "${UNCOMPRESSED_DIR}"
            ;;
        *.tar)
            echo "    Extracting Tar archive -> ${UNCOMPRESSED_DIR}..."
            tar -xf "${current_file}" -C "${UNCOMPRESSED_DIR}"
            ;;
        *.gz)
            outname="${filename%.gz}"
            echo "    Decompressing Gzip .gz archive -> ${outname}..."
            gzip -dc "${current_file}" > "${UNCOMPRESSED_DIR}/${outname}"
            ;;
        *.z|*.Z)
            outname="${filename%.[zZ]}"
            echo "    Decompressing Unix .z archive -> ${outname}..."
            if command -v uncompress >/dev/null 2>&1; then
                cp "${current_file}" "${UNCOMPRESSED_DIR}/${filename}"
                uncompress -f "${UNCOMPRESSED_DIR}/${filename}"
            else
                gzip -dc "${current_file}" > "${UNCOMPRESSED_DIR}/${outname}"
            fi
            ;;
        *)
            echo "    File is not a known archive format. Copying verbatim to output..."
            cp "${current_file}" "${UNCOMPRESSED_DIR}/${filename}"
            ;;
    esac
done

echo "[Step 4/5] Auditing uncompressed output files..."
OUTPUT_COUNT=0
for outfile in $(find "${UNCOMPRESSED_DIR}" -type f); do
    OUTPUT_COUNT=$((OUTPUT_COUNT + 1))
    outname=$(basename "${outfile}")
    size_h=$(du -h "${outfile}" | cut -f1)
    echo "    Output [${OUTPUT_COUNT}]: ${outname} | Size: ${size_h}"
done

if [[ "${OUTPUT_COUNT}" -eq 0 ]]; then
    echo "ERROR: Zero uncompressed files produced in output stage!"
    exit 1
fi

echo "[Step 5/5] Uploading uncompressed files to Staging Bucket: ${GCS_STAGE_URI}..."
UPLOAD_SUCCESS=false
for attempt in $(seq 1 10); do
    if gcloud storage cp -r "${UNCOMPRESSED_DIR}"/* "${GCS_STAGE_URI}"; then
        UPLOAD_SUCCESS=true
        break
    fi
    echo "  Upload attempt ${attempt}/10 failed, retrying in 5s..."
    sleep 5
done

if [[ "${UPLOAD_SUCCESS}" != "true" ]]; then
    echo "ERROR: Failed to upload output files to ${GCS_STAGE_URI}!"
    exit 1
fi

echo "================================================================"
echo "  Decompression & Decryption completed successfully!"
echo "  Total output files uploaded: ${OUTPUT_COUNT}"
echo "================================================================"
exit 0
