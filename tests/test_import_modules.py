# This file is part of ppdb-gcp.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import importlib
import pkgutil
import sys
import unittest

try:
    import google.auth
    import google.cloud  # noqa: F401

    GCP_MISSING = False
except ImportError:
    GCP_MISSING = True


def _test_package_imports(package_name: str) -> None:
    """Test importing all submodules under the given package.

    Parameters
    ----------
    package_name : str
        The name of the package to test, e.g., 'lsst.dax.ppdbx.gcp'.
    """
    try:
        pkg = importlib.import_module(package_name)
    except ImportError as e:
        print(f"Cannot import root package '{package_name}': {e}")
        sys.exit(1)

    # Get package path(s)
    if not hasattr(pkg, "__path__"):
        print(f"'{package_name}' is not a package.")
        sys.exit(1)

    failures = []
    for finder, mod_name, ispkg in pkgutil.walk_packages(pkg.__path__, package_name + "."):
        try:
            print(f"Importing {mod_name}...")
            importlib.import_module(mod_name)
            print(f"Successfully imported {mod_name}")
        except Exception as e:
            failures.append((mod_name, repr(e)))

    if failures:
        print("Some modules failed to import:")
        for name, err in failures:
            print(f"  {name}: {err}")
        raise ImportError(
            f"Failed to import some modules under '{package_name}': {', '.join(name for name, _ in failures)}"
        )
    else:
        print(f"All modules under '{package_name}' imported successfully.")


@unittest.skipIf(GCP_MISSING, "GCP libraries are not available; skipping module import tests.")
class TestPackageImports(unittest.TestCase):
    """Unit test case to check imports of all modules in the 'lsst.ppdb.gcp'
    package.
    """

    def test_imports(self):
        """Test importing all modules in the 'lsst.ppdb.gcp' package."""
        _test_package_imports("lsst.dax.ppdbx.gcp")


if __name__ == "__main__":
    unittest.main()
