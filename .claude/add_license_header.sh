#!/usr/bin/env bash
# Auto-prepends the BUSL-1.1 header to newly written .rs files.
# Invoked as a PostToolUse hook by Claude Code.
set -euo pipefail

input=$(cat)
file_path=$(printf '%s' "$input" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('tool_input', {}).get('file_path', ''))
" 2>/dev/null || true)

[[ -z "$file_path" || "$file_path" != *.rs ]] && exit 0
[[ -f "$file_path" ]] || exit 0
grep -q "SPDX-License-Identifier" "$file_path" && exit 0

python3 - "$file_path" <<'EOF'
import sys, pathlib
f = pathlib.Path(sys.argv[1])
lines = [
    "// Copyright (c) 2026 Austin Han <austinhan1024@gmail.com>",
    "//",
    "// This file is part of MultiGraph.",
    "//",
    "// Use of this software is governed by the Business Source License 1.1",
    "// included in the LICENSE file at the root of this repository.",
    "//",
    "// As of the Change Date (2030-01-01), in accordance with the Business Source",
    "// License, use of this software will be governed by the Apache License 2.0.",
    "//",
    "// SPDX-License-Identifier: BUSL-1.1",
    "",
]
f.write_text("\n".join(lines) + "\n" + f.read_text())
EOF
