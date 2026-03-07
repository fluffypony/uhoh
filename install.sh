#!/usr/bin/env bash
#
# uhoh installer — https://uhoh.it/install.sh
# Usage: curl -fsSL https://uhoh.it/install.sh | bash
#
# Environment overrides:
#   UHOH_INSTALL_DIR  — override install directory
#   GITHUB_TOKEN      — authenticate GitHub API requests (optional, avoids rate limits)
#
set -euo pipefail

main() {
  local BOLD='\033[1m'
  local GREEN='\033[0;32m'
  local YELLOW='\033[1;33m'
  local RED='\033[0;31m'
  local NC='\033[0m'

  echo -e "${BOLD}uhoh installer${NC}\n"

  if command -v uhoh >/dev/null 2>&1; then
    local CURRENT_VERSION
    CURRENT_VERSION="$(uhoh --version 2>/dev/null || echo 'unknown')"
    echo -e "${GREEN}uhoh is already installed${NC} at $(command -v uhoh)"
    echo "  Current version: ${CURRENT_VERSION}"
    echo ""
    echo "To update, run: uhoh update"
    echo "To reinstall, remove the existing binary first and re-run this script."
    exit 0
  fi

  # Detect OS/arch
  local RAW_OS RAW_ARCH OS ARCH
  RAW_OS="$(uname -s)"
  RAW_ARCH="$(uname -m)"
  case "${RAW_OS}" in
    Linux|linux) OS="linux" ;;
    Darwin|darwin) OS="macos" ;;
    MINGW*|MSYS*|CYGWIN*)
      echo -e "${RED}Error:${NC} This script does not support Windows."
      echo "Use PowerShell:  irm https://uhoh.it/install.ps1 | iex"
      exit 1
      ;;
    *) echo -e "${RED}Error:${NC} Unsupported operating system: ${RAW_OS}"; exit 1 ;;
  esac
  case "${RAW_ARCH}" in
    x86_64|amd64) ARCH="x86_64" ;;
    aarch64|arm64) ARCH="aarch64" ;;
    *) echo -e "${RED}Error:${NC} Unsupported architecture: ${RAW_ARCH}"; exit 1 ;;
  esac
  local ASSET_NAME="uhoh-${OS}-${ARCH}"
  echo "Detected platform: ${OS}/${ARCH} (asset: ${ASSET_NAME})"

  # Choose install dir
  local INSTALL_DIR
  if [ -n "${UHOH_INSTALL_DIR:-}" ]; then
    INSTALL_DIR="${UHOH_INSTALL_DIR}"
  elif [ -d "/usr/local/bin" ] && [ -w "/usr/local/bin" ]; then
    INSTALL_DIR="/usr/local/bin"
  elif [ -d "${HOME}/.local/bin" ] || mkdir -p "${HOME}/.local/bin" 2>/dev/null; then
    INSTALL_DIR="${HOME}/.local/bin"
  else
    INSTALL_DIR="${HOME}/.local/bin"; mkdir -p "${INSTALL_DIR}"
  fi
  echo "Install directory: ${INSTALL_DIR}"

  # Fetch latest release
  echo ""
  echo "Fetching latest release information..."
  local GITHUB_API_URL="https://api.github.com/repos/fluffypony/uhoh/releases/latest"
  local AUTH_HEADER=""
  if [ -n "${GITHUB_TOKEN:-}" ]; then AUTH_HEADER="Authorization: token ${GITHUB_TOKEN}"; fi
  local RELEASE_JSON
  if command -v curl >/dev/null 2>&1; then
    if [ -n "${AUTH_HEADER}" ]; then
      RELEASE_JSON="$(curl -sS -H "${AUTH_HEADER}" "${GITHUB_API_URL}")"
    else
      RELEASE_JSON="$(curl -sS "${GITHUB_API_URL}")"
    fi
  elif command -v wget >/dev/null 2>&1; then
    if [ -n "${AUTH_HEADER}" ]; then
      RELEASE_JSON="$(wget -qO- --header="${AUTH_HEADER}" "${GITHUB_API_URL}")"
    else
      RELEASE_JSON="$(wget -qO- "${GITHUB_API_URL}")"
    fi
  else
    echo -e "${RED}Error:${NC} Neither curl nor wget found. Please install one."; exit 1
  fi

  local DOWNLOAD_URL
  if command -v jq >/dev/null 2>&1; then
    DOWNLOAD_URL="$(echo "${RELEASE_JSON}" | jq -r ".assets[] | select(.name | contains(\"${ASSET_NAME}\")) | .browser_download_url" | head -n 1)"
  else
    DOWNLOAD_URL="$(echo "${RELEASE_JSON}" | grep -o "\"browser_download_url\"[[:space:]]*:[[:space:]]*\"[^"]*${ASSET_NAME}[^"]*\"" | head -n 1 | sed 's/.*"browser_download_url"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/')"
  fi
  if [ -z "${DOWNLOAD_URL}" ] || [ "${DOWNLOAD_URL}" = "null" ]; then
    echo -e "${RED}Error:${NC} Could not find a release asset matching '${ASSET_NAME}'"
    echo "Please check: https://github.com/fluffypony/uhoh/releases/latest"; exit 1
  fi
  echo "Download URL: ${DOWNLOAD_URL}"

  # Download
  local TMP_DIR TMP_BIN
  TMP_DIR="$(mktemp -d)"; trap 'rm -rf "${TMP_DIR}"' EXIT
  TMP_BIN="${TMP_DIR}/uhoh"
  echo "Downloading..."
  if command -v curl >/dev/null 2>&1; then
    curl -fSL --progress-bar "${DOWNLOAD_URL}" -o "${TMP_BIN}"
  else
    wget -q --show-progress "${DOWNLOAD_URL}" -O "${TMP_BIN}"
  fi
  chmod +x "${TMP_BIN}"

  # Pre-install verification: verify the binary BEFORE installing it
  echo ""
  echo "Running pre-install verification..."
  if "${TMP_BIN}" doctor --verify-install 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Binary integrity verified before install."
  else
    code=$?
    if [ $code -eq 2 ]; then
      echo -e "${YELLOW}WARNING:${NC} DNS hash verification could not be completed."
    else
      echo -e "${YELLOW}Pre-install verification returned code ${code}.${NC}"
    fi
    # Continue with install despite verification issues
  fi

  # Install
  echo ""
  if [ ! -w "${INSTALL_DIR}" ]; then
    echo "Elevated permissions required to install to ${INSTALL_DIR}"
    sudo mv "${TMP_BIN}" "${INSTALL_DIR}/uhoh"
    sudo chmod +x "${INSTALL_DIR}/uhoh"
  else
    mv "${TMP_BIN}" "${INSTALL_DIR}/uhoh"
  fi
  echo -e "${GREEN}✓${NC} Binary installed to ${INSTALL_DIR}/uhoh"

  # Ensure PATH
  case ":${PATH}:" in *":${INSTALL_DIR}:"*) : ;; *)
    echo ""
    echo -e "${YELLOW}Warning:${NC} ${INSTALL_DIR} is not in your PATH."
    local EXPORT_LINE='export PATH="'"${INSTALL_DIR}"':$PATH"'
    for RC in "${HOME}/.bashrc" "${HOME}/.zshrc" "${HOME}/.profile"; do
      if [ -f "$RC" ] && ! grep -qF "${INSTALL_DIR}" "$RC"; then
        echo "" >> "$RC"; echo "# Added by uhoh installer" >> "$RC"; echo "$EXPORT_LINE" >> "$RC"
        echo "  Added to $RC"
      fi
    done
    export PATH="${INSTALL_DIR}:${PATH}"
    echo ""
    echo -e "${YELLOW}Please restart your terminal or run:${NC}"
    echo "  ${EXPORT_LINE}"
  esac

  # Post-install verification
  echo ""
  echo "Running post-install verification..."
  if "${INSTALL_DIR}/uhoh" doctor --verify-install 2>/dev/null; then
    echo -e "${GREEN}✓${NC} Binary integrity verified against DNS records."
  else
    code=$?
    if [ $code -eq 2 ]; then
      echo -e "${YELLOW}WARNING:${NC} DNS hash verification failed. Binary installed but be cautious."
    else
      echo -e "${YELLOW}Could not complete DNS verification.${NC} The binary has been installed."
    fi
  fi

  echo ""
  echo -e "${GREEN}${BOLD}uhoh installed successfully!${NC}"
  echo ""
  echo "Get started:"
  echo "  uhoh --help"
}

main "$@"
