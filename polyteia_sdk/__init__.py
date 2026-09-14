"""Python SDK for the Polyteia API.

The platform-neutral import name. It re-exports ``polyteia_sdk_python_v2`` in
full, so ``import polyteia_sdk as api`` is the one line new code needs, and it
stays valid when older API versions are removed from this package.
"""

from polyteia_sdk_python_v2 import *          # noqa: F401,F403
from polyteia_sdk_python_v2 import __all__    # noqa: F401
