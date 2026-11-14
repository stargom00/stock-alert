import net_probe


class R:
    def __init__(self, code, content=b"xx"):
        self.status_code, self.content = code, content


def test_probe_reports_status_and_errors():
    def get(url, timeout):
        if "binance" in url:
            return R(451)
        if "okx" in url:
            raise TimeoutError("x")
        return R(200, b"abc")
    out = net_probe.probe(get)
    assert set(out) == set(net_probe.URLS)
    assert out["binance_fapi"] == {"status": 451, "bytes": 2}
    assert out["okx"] == {"status": 0, "error": "TimeoutError"}
    assert out["bitget"] == {"status": 200, "bytes": 3}
