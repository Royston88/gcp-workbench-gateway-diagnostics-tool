# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module entry point.

Allows the diagnostic to be invoked as ``python -m dataproc_gateway_diagnostics``.

This matters inside Jupyter notebooks: the ``gateway-diag`` console script is
installed into the interpreter's ``bin/`` directory, which is frequently absent
from the kernel process ``PATH`` (notably on Vertex AI Workbench, where the
kernel runs from the micromamba base environment). Invoking the package as a
module via ``sys.executable`` sidesteps ``PATH`` resolution entirely.
"""

import sys

from .cli import main

if __name__ == "__main__":
    sys.exit(main())
