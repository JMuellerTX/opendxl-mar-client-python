"""
Unit tests for the MAR client (no broker required): the DXL client is
replaced by a fake that records the requests and returns canned responses.
"""

import json
import unittest

from dxlclient.message import ErrorResponse, Request, Response
from dxlmarclient import MarClient, ResultConstants, SortConstants
from dxlmarclient.client import MAR_SEARCH_TOPIC


def _json_response(request, payload_dict, code=200):
    payload_dict = dict(payload_dict)
    if code is not None:
        payload_dict["code"] = code
    response = Response(request)
    response.payload = json.dumps(payload_dict).encode("utf-8")
    return response


class FakeDxlClient(object):
    """Fake DxlClient returning the queued responses in order"""

    def __init__(self, responders):
        self.responders = list(responders)
        self.requests = []

    def sync_request(self, request, timeout=None): # pylint: disable=unused-argument
        self.requests.append(request)
        responder = self.responders.pop(0)
        return responder(request)


class MarClientTest(unittest.TestCase):

    def test_poll_interval_validation(self):
        client = MarClient(FakeDxlClient([]))
        self.assertEqual(5, client.poll_interval)
        client.poll_interval = 10
        self.assertEqual(10, client.poll_interval)
        with self.assertRaises(Exception):
            client.poll_interval = 1

    def test_search_and_get_results(self):
        results_body = {
            "startIndex": 0, "totalItems": 1, "currentItemCount": 1,
            "itemsPerPage": 20,
            "items": [{"id": "1", "count": 1, "created_at": "x",
                       "output": {"HostInfo|ip_address": "10.0.0.1"}}]}
        dxl_client = FakeDxlClient([
            lambda req: _json_response(req, {"body": {"id": "search-1"}}),
            lambda req: _json_response(req, {"body": {}}),
            lambda req: _json_response(req, {"body": {
                "status": "FINISHED", "results": 1, "errors": 0,
                "hosts": 2, "subscribedHosts": 3}}),
            lambda req: _json_response(req, {"body": results_body})
        ])
        client = MarClient(dxl_client)
        context = client.search(
            projections=[{"name": "HostInfo", "outputs": ["ip_address"]}],
            conditions={"or": []},
            context={"maGuids": ["{guid}"]})

        self.assertTrue(context.has_results)
        self.assertEqual(1, context.result_count)
        self.assertEqual(0, context.error_count)
        self.assertEqual(2, context.host_count)
        self.assertEqual(3, context.subscribed_host_count)

        results = context.get_results(offset=1, limit=5, text_filter="f",
                                      sort_by="HostInfo|ip_address",
                                      sort_direction=SortConstants.ASC)
        self.assertEqual(results_body, results)
        self.assertEqual(
            "10.0.0.1",
            results[ResultConstants.ITEMS][0][ResultConstants.ITEM_OUTPUT]
            ["HostInfo|ip_address"])

        for request in dxl_client.requests:
            self.assertIsInstance(request, Request)
            self.assertEqual(MAR_SEARCH_TOPIC, request.destination_topic)
        create, start, status, get = [
            json.loads(request.payload.decode("utf-8"))
            for request in dxl_client.requests]
        self.assertEqual("/v1/simple", create["target"])
        self.assertEqual("POST", create["method"])
        self.assertEqual(
            [{"name": "HostInfo", "outputs": ["ip_address"]}],
            create["body"]["projections"])
        self.assertEqual({"or": []}, create["body"]["condition"])
        self.assertEqual({"maGuids": ["{guid}"]}, create["body"]["context"])
        self.assertEqual(("/v1/search-1/start", "PUT"),
                         (start["target"], start["method"]))
        self.assertEqual(("/v1/search-1/status", "GET"),
                         (status["target"], status["method"]))
        self.assertEqual("/v1/search-1/results", get["target"])
        self.assertEqual(
            {"$offset": 1, "$limit": 5, "filter": "f",
             "sortBy": "HostInfo|ip_address", "sortDirection": "asc"},
            get["parameters"])

    def test_dxl_error_response_raises(self):
        def error_response(request):
            return ErrorResponse(request, error_code=404,
                                 error_message="unable to locate service")
        client = MarClient(FakeDxlClient([error_response]))
        with self.assertRaises(Exception) as context:
            client.search(projections=[])
        self.assertIn("unable to locate service", str(context.exception))

    def test_application_error_list(self):
        client = MarClient(FakeDxlClient([
            lambda req: _json_response(req, {"body": {
                "applicationErrorList": [{"message": "Bad projection",
                                          "code": 4001}]}}, code=400)]))
        with self.assertRaises(Exception) as context:
            client.search(projections=[])
        self.assertEqual("Bad projection: 4001", str(context.exception))

    def test_error_with_non_string_body(self):
        # Regression: a dict body used to raise TypeError (str + dict)
        client = MarClient(FakeDxlClient([
            lambda req: _json_response(req, {"body": {"detail": "x"}},
                                       code=500)]))
        with self.assertRaises(Exception) as context:
            client.search(projections=[])
        self.assertNotIsInstance(context.exception, TypeError)
        self.assertIn("500", str(context.exception))
        self.assertIn("detail", str(context.exception))

    def test_missing_response_code(self):
        client = MarClient(FakeDxlClient([
            lambda req: _json_response(req, {"body": {}}, code=None)]))
        with self.assertRaises(Exception) as context:
            client.search(projections=[])
        self.assertIn("unable to find response code", str(context.exception))
