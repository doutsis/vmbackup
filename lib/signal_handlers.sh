#!/usr/bin/env bash
#
# lib/signal_handlers.sh — Shared signal/exit trap registration helpers.
#
# UNI-010: vmbackup.sh registers traps for EXIT/SIGINT/SIGTERM/SIGTSTP via
# four inline `trap '...' SIG` lines (vmbackup.sh L6351-6354). vmrestore.sh
# previously had no traps registered, so SIGINT during a restore left the
# per-VM lock and the PIT staging dir behind. This lib gives both binaries
# a single consistent registration surface; the cleanup *bodies* remain
# per-binary (each tool has different state to release).
#
# Contract:
# - Each setup_* takes one arg: the name of a shell function (or a quoted
#   command string) to invoke when the signal fires.
# - Callbacks are invoked in their own subshell context per bash trap rules.
# - EXIT-trap callbacks should inspect $? to distinguish success from error
#   (cleanup typically only needs to run on error or on signal-induced exit).
# - Idempotent: a later setup_*_handler call replaces the prior trap.
#
# UNI-321: idempotency guard — re-source is a no-op once setup_exit_cleanup is defined.
#
# Usage example:
#     source "$SCRIPT_DIR/lib/signal_handlers.sh"
#     setup_exit_cleanup    'my_cleanup_on_exit'
#     setup_sigint_handler  'my_cleanup_on_sigint'
#     setup_sigterm_handler 'my_cleanup_on_sigterm'

declare -F setup_exit_cleanup >/dev/null 2>&1 && return 0

setup_exit_cleanup() {
  local callback="${1:?setup_exit_cleanup: callback name required}"
  trap "$callback" EXIT
}

setup_sigint_handler() {
  local callback="${1:?setup_sigint_handler: callback name required}"
  trap "$callback" SIGINT
}

setup_sigterm_handler() {
  local callback="${1:?setup_sigterm_handler: callback name required}"
  trap "$callback" SIGTERM
}

setup_sigtstp_handler() {
  local callback="${1:?setup_sigtstp_handler: callback name required}"
  trap "$callback" SIGTSTP
}
