"""Tests for network error classification and retry handling."""

import errno
import socket
import unittest
import urllib.error
from dataproc_gateway_diagnostics.client import GatewayDiagnosticClient


class TestNetworkError(unittest.TestCase):
    def test_is_network_error_socket_gaierror(self):
        exc = urllib.error.URLError(socket.gaierror(-2, "Name or service not known"))
        self.assertTrue(GatewayDiagnosticClient._is_network_error(exc))

    def test_is_network_error_enetunreach(self):
        exc = urllib.error.URLError(OSError(errno.ENETUNREACH, "Network is unreachable"))
        self.assertTrue(GatewayDiagnosticClient._is_network_error(exc))

    def test_is_network_error_econnrefused(self):
        exc = urllib.error.URLError(OSError(errno.ECONNREFUSED, "Connection refused"))
        self.assertTrue(GatewayDiagnosticClient._is_network_error(exc))

    def test_is_network_error_http_502_is_false(self):
        """HTTP 502 Bad Gateway should NOT be classified as connection-level unreachable."""
        exc = urllib.error.HTTPError(
            url="https://gateway.example.com",
            code=502,
            msg="Bad Gateway",
            hdrs={},
            fp=None,
        )
        self.assertFalse(GatewayDiagnosticClient._is_network_error(exc))

    def test_is_network_error_generic_timeout_is_false(self):
        """A generic string containing 'timed out' is NOT a network unreachability error."""
        exc = RuntimeError("The query execution timed out on the backend server")
        self.assertFalse(GatewayDiagnosticClient._is_network_error(exc))


if __name__ == "__main__":
    unittest.main()
