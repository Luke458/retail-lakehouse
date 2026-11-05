#!/usr/bin/env bash
# Local SMTP catcher on :$MAIL_PORT (default 1025). Each alert lands as a file in data/mail/new/.
set -euo pipefail
. "$(dirname "$0")/env.sh"
mkdir -p "$LAKE_ROOT"/mail/{tmp,new,cur}
exec python -m aiosmtpd -n -l "localhost:$MAIL_PORT" -c aiosmtpd.handlers.Mailbox "$LAKE_ROOT/mail"
