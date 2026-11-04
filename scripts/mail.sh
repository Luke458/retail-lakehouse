#!/usr/bin/env bash
# Local SMTP catcher on :1025. Each alert lands as a file in data/mail/new/.
set -euo pipefail
. "$(dirname "$0")/env.sh"
mkdir -p "$LAKE_ROOT"/mail/{tmp,new,cur}
exec python -m aiosmtpd -n -l localhost:1025 -c aiosmtpd.handlers.Mailbox "$LAKE_ROOT/mail"
