#!/bin/bash
#################################################################################
# SharePoint Re-Authentication Helper Script
#
# This script helps you configure or re-authenticate rclone with any
# SharePoint/OneDrive site using the device code flow. This provides a
# 90-day refresh token that rclone automatically renews on each use.
#
# When to run this script:
#   - Initial setup (first time configuring SharePoint for an instance)
#   - After 90+ days of inactivity (refresh token expired)
#   - If you see "401 Unauthorized" or "token expired" errors
#   - Setting up a new host with a different SharePoint destination
#
# Usage:
#   sudo ./sharepoint_auth.sh [OPTIONS]
#
# Options:
#   --remote NAME       Rclone remote name (default: "sharepoint")
#   --folder PATH       Folder to verify/create in document library
#                       (default: reads from vmbackup config instance)
#   --instance NAME     vmbackup config instance to read settings from
#                       (default: auto-detect from CLOUD_DEST_1_PATH)
#   --config FILE       Rclone config file path
#                       (default: /root/.config/rclone/rclone.conf)
#   --test-only         Test existing connection without re-authenticating
#   --help              Show this help message
#
# Examples:
#   sudo ./sharepoint_auth.sh
#   sudo ./sharepoint_auth.sh --remote sharepoint --folder DEVBackups
#   sudo ./sharepoint_auth.sh --instance dev
#   sudo ./sharepoint_auth.sh --test-only
#
# Troubleshooting (if this script stops working):
#
#   1. Test connection:
#        sudo ./sharepoint_auth.sh --test-only
#
#   2. Token expired (90+ days idle):
#        Re-run this script to re-authenticate.
#
#   3. rclone default Client ID rate-limited or blocked:
#        Create your own Azure AD app registration and set client_id
#        and client_secret in rclone config. See:
#        https://rclone.org/onedrive/#getting-your-own-client-id-and-key
#
#   4. Bypass this script — use rclone directly:
#        rclone config delete sharepoint
#        rclone config
#        # Choose: onedrive → leave client_id/secret blank → region: global
#        # → advanced: n → web browser: n (triggers device code flow)
#        rclone lsd sharepoint:   # verify
#
#   5. Microsoft changes OAuth flow — update rclone:
#        sudo apt update && sudo apt install rclone
#        # Or latest from rclone.org:
#        curl https://rclone.org/install.sh | sudo bash
#
#   6. Device code flow broken — use SSH tunnel instead:
#        # From desktop with browser:
#        ssh -L localhost:53682:localhost:53682 root@<host>
#        # Then on host: rclone config → answer Y to web browser question
#
#   7. Nuclear option — delete config and start fresh:
#        rm /root/.config/rclone/rclone.conf
#        rclone config
#
# Version: 2.0
# Created: 2026-01-26
# Updated: 2026-02-27 — Generalized for any SharePoint site/folder/instance
#################################################################################

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

#################################################################################
# DEFAULTS
#################################################################################

RCLONE_REMOTE_NAME="sharepoint"
RCLONE_CONFIG_FILE="/root/.config/rclone/rclone.conf"
FOLDER_PATH=""
CONFIG_INSTANCE=""
TEST_ONLY=false

# Resolve script directory for finding vmbackup configs
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

#################################################################################
# ARGUMENT PARSING
#################################################################################

show_help() {
    echo ""
    echo "Usage: sudo $0 [OPTIONS]"
    echo ""
    echo "Configure or re-authenticate rclone with a SharePoint document library."
    echo ""
    echo "Options:"
    echo "  --remote NAME       Rclone remote name (default: \"sharepoint\")"
    echo "  --folder PATH       Folder path in document library to verify/create"
    echo "  --instance NAME     vmbackup config instance (e.g., dev, test, default)"
    echo "                      Reads CLOUD_DEST_1_REMOTE and CLOUD_DEST_1_PATH"
    echo "  --config FILE       Rclone config file path"
    echo "                      (default: /root/.config/rclone/rclone.conf)"
    echo "  --test-only         Test existing connection, don't re-authenticate"
    echo "  --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  sudo $0                                    # Interactive, defaults"
    echo "  sudo $0 --instance dev                     # Use dev instance settings"
    echo "  sudo $0 --remote sharepoint --folder DEVBackups"
    echo "  sudo $0 --test-only                        # Just verify connection"
    echo ""
    echo "The script uses device code flow authentication (works on headless servers)."
    echo "You will need a web browser on any device to complete authentication."
    echo ""
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --remote)
            RCLONE_REMOTE_NAME="$2"
            shift 2
            ;;
        --remote=*)
            RCLONE_REMOTE_NAME="${1#*=}"
            shift
            ;;
        --folder)
            FOLDER_PATH="$2"
            shift 2
            ;;
        --folder=*)
            FOLDER_PATH="${1#*=}"
            shift
            ;;
        --instance)
            CONFIG_INSTANCE="$2"
            shift 2
            ;;
        --instance=*)
            CONFIG_INSTANCE="${1#*=}"
            shift
            ;;
        --config)
            RCLONE_CONFIG_FILE="$2"
            shift 2
            ;;
        --config=*)
            RCLONE_CONFIG_FILE="${1#*=}"
            shift
            ;;
        --test-only)
            TEST_ONLY=true
            shift
            ;;
        --help|-h)
            show_help
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            echo "Use --help for usage information."
            exit 1
            ;;
    esac
done

#################################################################################
# LOAD SETTINGS FROM VMBACKUP CONFIG INSTANCE
#################################################################################

load_instance_settings() {
    local instance="$1"
    local config_dir="${SCRIPT_DIR}/config/${instance}"
    local cloud_conf="${config_dir}/replication_cloud.conf"

    if [[ ! -f "$cloud_conf" ]]; then
        echo -e "${YELLOW}WARNING: Config not found: ${cloud_conf}${NC}"
        return 1
    fi

    echo -e "${CYAN}Reading settings from config instance: ${instance}${NC}"
    echo -e "${CYAN}  Config: ${cloud_conf}${NC}"

    # Extract specific variables safely (no sourcing)
    local remote_val folder_val
    remote_val=$(grep -E '^\s*CLOUD_DEST_1_REMOTE=' "$cloud_conf" 2>/dev/null | tail -1 | cut -d'"' -f2)
    folder_val=$(grep -E '^\s*CLOUD_DEST_1_PATH=' "$cloud_conf" 2>/dev/null | tail -1 | cut -d'"' -f2)

    # Only override if not already set via CLI args
    if [[ -n "$remote_val" ]]; then
        # Strip trailing colon if present (remote_val might be "sharepoint:")
        RCLONE_REMOTE_NAME="${remote_val%:}"
        echo -e "  Remote name: ${GREEN}${RCLONE_REMOTE_NAME}${NC}"
    fi

    if [[ -n "$folder_val" ]] && [[ -z "$FOLDER_PATH" ]]; then
        FOLDER_PATH="$folder_val"
        echo -e "  Folder path: ${GREEN}${FOLDER_PATH}${NC}"
    fi

    echo ""
    return 0
}

# Auto-discover instance settings
if [[ -n "$CONFIG_INSTANCE" ]]; then
    load_instance_settings "$CONFIG_INSTANCE"
elif [[ -z "$FOLDER_PATH" ]]; then
    # Try to auto-detect: list available instances
    available_instances=()
    for dir in "${SCRIPT_DIR}/config/"*/; do
        instance_name="$(basename "$dir")"
        if [[ "$instance_name" != "template" ]] && [[ -f "${dir}/replication_cloud.conf" ]]; then
            available_instances+=("$instance_name")
        fi
    done

    if [[ ${#available_instances[@]} -gt 0 ]]; then
        echo ""
        echo -e "${BLUE}Available vmbackup config instances:${NC}"
        echo ""
        for i in "${!available_instances[@]}"; do
            local_instance="${available_instances[$i]}"
            local_folder=$(grep -E '^\s*CLOUD_DEST_1_PATH=' "${SCRIPT_DIR}/config/${local_instance}/replication_cloud.conf" 2>/dev/null | tail -1 | cut -d'"' -f2)
            local_remote=$(grep -E '^\s*CLOUD_DEST_1_REMOTE=' "${SCRIPT_DIR}/config/${local_instance}/replication_cloud.conf" 2>/dev/null | tail -1 | cut -d'"' -f2)
            echo -e "  $((i+1)). ${GREEN}${local_instance}${NC}  →  ${local_remote}${local_folder}"
        done
        echo ""
        read -p "Select instance [1-${#available_instances[@]}] or Enter for manual: " instance_choice
        if [[ -n "$instance_choice" ]] && [[ "$instance_choice" =~ ^[0-9]+$ ]] && [[ "$instance_choice" -ge 1 ]] && [[ "$instance_choice" -le ${#available_instances[@]} ]]; then
            CONFIG_INSTANCE="${available_instances[$((instance_choice-1))]}"
            load_instance_settings "$CONFIG_INSTANCE"
        fi
    fi
fi

#################################################################################
# HEADER
#################################################################################

echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  SharePoint Re-Authentication Script${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo -e "  Remote name:   ${GREEN}${RCLONE_REMOTE_NAME}${NC}"
echo -e "  Config file:   ${GREEN}${RCLONE_CONFIG_FILE}${NC}"
[[ -n "$FOLDER_PATH" ]] && echo -e "  Folder path:   ${GREEN}${FOLDER_PATH}${NC}"
[[ -n "$CONFIG_INSTANCE" ]] && echo -e "  Instance:      ${GREEN}${CONFIG_INSTANCE}${NC}"
echo ""

# Check if running as root (needed for /root/.config)
if [[ $EUID -ne 0 ]]; then
    echo -e "${RED}ERROR: This script must be run as root (sudo)${NC}"
    echo ""
    echo "Usage: sudo $0"
    exit 1
fi

# Check if rclone is installed
if ! command -v rclone &>/dev/null; then
    echo -e "${RED}ERROR: rclone is not installed${NC}"
    echo ""
    echo "Install with: sudo apt install rclone"
    exit 1
fi

#################################################################################
# TEST-ONLY MODE
#################################################################################

test_connection() {
    local remote="$1"
    local folder="$2"

    echo "Testing connection to ${remote}:..."
    if rclone lsd "${remote}:" --config "$RCLONE_CONFIG_FILE" &>/dev/null; then
        echo -e "${GREEN}✓ Connection to ${remote}: successful${NC}"
    else
        echo -e "${RED}✗ Connection to ${remote}: failed${NC}"
        return 1
    fi

    if [[ -n "$folder" ]]; then
        echo "Checking for folder: ${folder}..."
        if rclone lsd "${remote}:${folder}" --config "$RCLONE_CONFIG_FILE" &>/dev/null; then
            echo -e "${GREEN}✓ Folder '${folder}' exists${NC}"
        else
            echo -e "${YELLOW}✗ Folder '${folder}' not found${NC}"
        fi
    fi

    # Check quota
    local quota_info
    quota_info=$(rclone about "${remote}:" --config "$RCLONE_CONFIG_FILE" 2>/dev/null) && {
        echo ""
        echo -e "${CYAN}Storage quota:${NC}"
        echo "$quota_info" | sed 's/^/  /'
    }

    # Check refresh token
    echo ""
    if grep -A20 "^\[${remote}\]" "$RCLONE_CONFIG_FILE" 2>/dev/null | grep -q "refresh_token"; then
        echo -e "${GREEN}✓ Refresh token present - automatic renewal enabled${NC}"
    else
        echo -e "${YELLOW}⚠ No refresh token found - may expire after 1 hour${NC}"
    fi

    return 0
}

if [[ "$TEST_ONLY" == true ]]; then
    echo -e "${BLUE}Running connection test only...${NC}"
    echo ""
    test_connection "$RCLONE_REMOTE_NAME" "$FOLDER_PATH"
    exit $?
fi

#################################################################################
# CHECK EXISTING REMOTE
#################################################################################

# Check if remote already exists
if rclone listremotes 2>/dev/null | grep -q "^${RCLONE_REMOTE_NAME}:$"; then
    echo -e "${YELLOW}Existing '${RCLONE_REMOTE_NAME}' remote found.${NC}"
    echo ""
    
    # Test current connection
    echo "Testing current connection..."
    if rclone lsd "${RCLONE_REMOTE_NAME}:" &>/dev/null; then
        echo -e "${GREEN}✓ Current authentication is still valid!${NC}"
        echo ""
        echo "No re-authentication needed. If you're still having issues,"
        echo "the problem may be elsewhere (network, permissions, etc.)."
        echo ""
        read -p "Do you want to re-authenticate anyway? [y/N] " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Yy]$ ]]; then
            echo "Exiting without changes."
            exit 0
        fi
    else
        echo -e "${YELLOW}✗ Current authentication has expired or is invalid.${NC}"
        echo ""
    fi
    
    # Backup existing config
    if [[ -f "$RCLONE_CONFIG_FILE" ]]; then
        BACKUP_FILE="${RCLONE_CONFIG_FILE}.backup.$(date +%Y%m%d-%H%M%S)"
        cp "$RCLONE_CONFIG_FILE" "$BACKUP_FILE"
        echo -e "${GREEN}Backed up existing config to: ${BACKUP_FILE}${NC}"
    fi
    
    # Delete existing remote
    echo "Removing existing '${RCLONE_REMOTE_NAME}' remote..."
    rclone config delete "$RCLONE_REMOTE_NAME" 2>/dev/null || true
fi

echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  Step 1: Configure rclone remote${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo "This process will:"
echo "  1. Guide you through rclone's interactive setup"
echo "  2. Use device code authentication (works on headless servers)"
echo "  3. Set up automatic token refresh (no more 1-hour expiration!)"
echo ""
echo -e "${YELLOW}IMPORTANT: You will need access to a web browser (can be on another device)${NC}"
echo ""
read -p "Press Enter to continue..."

echo ""
echo "When prompted, enter the following:"
echo ""
echo -e "  Name:                    ${GREEN}${RCLONE_REMOTE_NAME}${NC}"
echo -e "  Storage type:            ${GREEN}onedrive${NC} (or number for Microsoft OneDrive)"
echo -e "  client_id:               ${GREEN}(leave blank - use rclone's default)${NC}"
echo -e "  client_secret:           ${GREEN}(leave blank - use rclone's default)${NC}"
echo -e "  region:                  ${GREEN}global${NC} (unless you're in a special region)"
echo -e "  Edit advanced config:    ${GREEN}n${NC}"
echo -e "  Use web browser:         ${GREEN}n${NC} (this triggers device code flow)"
echo ""
echo "  Then follow the device code instructions shown..."
echo ""
echo -e "  When asked for drive type, choose: ${GREEN}sharepoint${NC} (SharePoint site documentLibrary)"
echo -e "  When asked for site URL, enter your SharePoint site URL"
echo ""
echo -e "${YELLOW}Starting rclone config in 5 seconds...${NC}"
sleep 5

# Capture remotes before and after to detect what was created
REMOTES_BEFORE=$(rclone listremotes --config "$RCLONE_CONFIG_FILE" 2>/dev/null || true)

# Run rclone config with explicit config file path
rclone config --config "$RCLONE_CONFIG_FILE"

# Detect newly created remotes
REMOTES_AFTER=$(rclone listremotes --config "$RCLONE_CONFIG_FILE" 2>/dev/null || true)

echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  Step 2: Verify configuration${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Check if expected remote exists
if ! echo "$REMOTES_AFTER" | grep -q "^${RCLONE_REMOTE_NAME}:$"; then
    # Find any NEW remotes that weren't there before
    NEW_REMOTES=$(comm -13 <(echo "$REMOTES_BEFORE" | sort) <(echo "$REMOTES_AFTER" | sort) | sed 's/:$//')

    if [[ -n "$NEW_REMOTES" ]]; then
        # User created a remote with a different name
        NEW_REMOTE_NAME=$(echo "$NEW_REMOTES" | head -1)
        echo -e "${YELLOW}Remote '${RCLONE_REMOTE_NAME}' was not found, but '${NEW_REMOTE_NAME}' was created.${NC}"
        echo ""
        echo "vmbackup expects the remote to be named '${RCLONE_REMOTE_NAME}' (from CLOUD_DEST_N_REMOTE)."
        echo ""
        read -p "Rename '${NEW_REMOTE_NAME}' → '${RCLONE_REMOTE_NAME}'? [Y/n] " -n 1 -r
        echo
        if [[ ! $REPLY =~ ^[Nn]$ ]]; then
            # Use sed to rename (rclone config rename not available in all versions)
            sed -i "s/^\[${NEW_REMOTE_NAME}\]\$/[${RCLONE_REMOTE_NAME}]/" "$RCLONE_CONFIG_FILE"
            if rclone listremotes --config "$RCLONE_CONFIG_FILE" 2>/dev/null | grep -q "^${RCLONE_REMOTE_NAME}:$"; then
                echo -e "${GREEN}✓ Remote renamed to '${RCLONE_REMOTE_NAME}'${NC}"
            else
                echo -e "${RED}✗ Rename failed. Please manually edit ${RCLONE_CONFIG_FILE}${NC}"
                echo "  Change [${NEW_REMOTE_NAME}] to [${RCLONE_REMOTE_NAME}]"
                exit 1
            fi
        else
            echo -e "${YELLOW}Keeping remote as '${NEW_REMOTE_NAME}'.${NC}"
            echo "You will need to update CLOUD_DEST_N_REMOTE in your vmbackup config to '${NEW_REMOTE_NAME}:'."
            RCLONE_REMOTE_NAME="$NEW_REMOTE_NAME"
        fi
        echo ""
    else
        echo -e "${RED}ERROR: Remote '${RCLONE_REMOTE_NAME}' was not created.${NC}"
        echo ""
        echo "Please try again or check rclone documentation."
        exit 1
    fi
fi

echo "Testing connection to SharePoint..."
if rclone lsd "${RCLONE_REMOTE_NAME}:" --config "$RCLONE_CONFIG_FILE" &>/dev/null; then
    echo -e "${GREEN}✓ Connection successful!${NC}"
else
    echo -e "${RED}✗ Connection failed${NC}"
    echo ""
    echo "Try running: rclone lsd ${RCLONE_REMOTE_NAME}: --config ${RCLONE_CONFIG_FILE} -v"
    echo "to see detailed error messages."
    exit 1
fi

echo ""
if [[ -n "$FOLDER_PATH" ]]; then
    echo "Checking for folder: ${FOLDER_PATH}..."
    if rclone lsd "${RCLONE_REMOTE_NAME}:${FOLDER_PATH}" --config "$RCLONE_CONFIG_FILE" &>/dev/null; then
        echo -e "${GREEN}✓ Folder '${FOLDER_PATH}' exists${NC}"
    else
        echo -e "${YELLOW}Folder '${FOLDER_PATH}' not found. Creating it...${NC}"
        rclone mkdir "${RCLONE_REMOTE_NAME}:${FOLDER_PATH}" --config "$RCLONE_CONFIG_FILE"
        echo -e "${GREEN}✓ Folder '${FOLDER_PATH}' created${NC}"
    fi
else
    echo "Listing top-level folders in document library:"
    rclone lsd "${RCLONE_REMOTE_NAME}:" --config "$RCLONE_CONFIG_FILE" 2>/dev/null | head -20
fi

echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  Step 3: Verify token includes refresh_token${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""

# Check for refresh_token in config (look in the specific remote section)
if grep -A20 "^\[${RCLONE_REMOTE_NAME}\]" "$RCLONE_CONFIG_FILE" 2>/dev/null | grep -q "refresh_token"; then
    echo -e "${GREEN}✓ Refresh token present - automatic token renewal enabled!${NC}"
    echo ""
    echo "  Your token will automatically refresh when needed."
    echo "  No manual intervention required for long uploads."
else
    echo -e "${YELLOW}⚠ No refresh token found in configuration.${NC}"
    echo ""
    echo "  This may indicate client_credentials flow was used instead of"
    echo "  delegated auth. You may experience 1-hour token expiration issues."
    echo ""
    echo "  To fix: Run this script again and ensure you:"
    echo "    1. Leave client_id and client_secret BLANK"
    echo "    2. Answer 'n' to 'Use web browser to authenticate'"
fi

echo ""
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo -e "${GREEN}  Configuration Complete!${NC}"
echo -e "${BLUE}═══════════════════════════════════════════════════════════════${NC}"
echo ""
echo "Your SharePoint remote '${RCLONE_REMOTE_NAME}' is now configured."
echo ""
echo "Key points:"
echo "  • Token automatically refreshes (no 1-hour expiration limit)"
echo "  • Refresh token valid for 90 days of inactivity"
echo "  • Re-run this script if you see auth errors after 90+ days idle"
echo ""
DISPLAY_FOLDER="${FOLDER_PATH:-<YourFolder>}"
echo "Test commands:"
echo "  rclone lsd ${RCLONE_REMOTE_NAME}:${DISPLAY_FOLDER}     # List contents"
echo "  rclone about ${RCLONE_REMOTE_NAME}:                     # Check quota"
echo "  rclone copy /tmp/test.txt ${RCLONE_REMOTE_NAME}:${DISPLAY_FOLDER}/  # Test upload"
echo ""
if [[ -n "$CONFIG_INSTANCE" ]]; then
    echo "vmbackup usage:"
    echo "  sudo ./vmbackup.sh --config-instance ${CONFIG_INSTANCE}"
    echo ""
fi
