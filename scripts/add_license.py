#!/usr/bin/env python3

import sys

LICENSE = "/* Copyright (C) Vast Data Ltd. */"

LICENSE2 = "/* *  Copyright (C) Vast Data Ltd. */"

for f in sys.argv[1:]:
    contents = open(f).read()
    replace = contents.replace('\n', '', 2)
    if not replace.startswith(LICENSE) and not replace.startswith(LICENSE2):
        with open(f, "w") as f:
            f.write(LICENSE)
            f.write("\n\n")
            f.write(contents)
