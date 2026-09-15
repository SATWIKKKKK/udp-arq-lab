"""Pytest root conftest.

Placing this file at the repository root makes pytest add the root to
``sys.path``, so tests can ``import udp_arq`` without installing the package.
"""
