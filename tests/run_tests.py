#!/usr/bin/env python3
"""
tests/run_tests.py — Standalone test runner (no pytest needed)

Usage:
    python3 tests/run_tests.py
    python3 tests/run_tests.py -v          # verbose
    python3 tests/run_tests.py -k security # filter by name
"""
import os
import sys
import re
import hmac
import time
import inspect
import argparse

# Ensure project root is importable
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, BASE_DIR)


def main():
    parser = argparse.ArgumentParser(description="Run security test suite")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    parser.add_argument("-k", "--filter", default="", help="Only run tests matching this string")
    args = parser.parse_args()

    # Load test module with correct __file__ for path resolution
    ns = {
        "__file__": os.path.join(BASE_DIR, "tests", "test_security.py"),
        "__name__": "__test__",
        "os": os, "re": re, "hmac": hmac, "time": time,
    }
    with open(os.path.join(BASE_DIR, "tests", "test_security.py")) as f:
        exec(f.read(), ns)

    test_classes = [
        ns["TestWebPanelSecurity"],
        ns["TestCredentialSecurity"],
        ns["TestDataIntegrity"],
        ns["TestInjectionPrevention"],
        ns["TestBruteForceProtection"],
        ns["TestBackupSecurity"],
        ns["TestMemorySafety"],
        ns["TestInfrastructure"],
    ]

    passed = failed = 0
    failures = []

    for cls in test_classes:
        obj = cls()
        printed_class = False
        for name, method in sorted(inspect.getmembers(obj, predicate=inspect.ismethod)):
            if not name.startswith("test_"):
                continue
            if args.filter and args.filter.lower() not in name.lower():
                continue
            if args.verbose and not printed_class:
                print(f"\n{cls.__name__}:")
                printed_class = True
            try:
                method()
                passed += 1
                if args.verbose:
                    print(f"  ✅ {name}")
            except AssertionError as e:
                failed += 1
                failures.append((cls.__name__, name, str(e)))
                if args.verbose:
                    print(f"  ❌ {name}: {e}")
            except Exception as e:
                failed += 1
                failures.append((cls.__name__, name, f"{type(e).__name__}: {e}"))
                if args.verbose:
                    print(f"  ❌ {name}: {type(e).__name__}: {e}")

    # Summary
    total = passed + failed
    print(f"\n{'='*60}")
    print(f"{'✅ ALL PASSED' if failed == 0 else '❌ SOME FAILED'}")
    print(f"Tests: {passed}/{total} passed")

    if failures:
        print("\nFailed tests:")
        for cls_name, test_name, msg in failures:
            print(f"  ❌ {cls_name}::{test_name}")
            if msg:
                print(f"     {msg[:120]}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
