from shared.analytics import alert_rate, risk_level


def test_alert_rate_uses_total_valid_readings():
    assert alert_rate(3, 10) == 30.0
    assert alert_rate(0, 0) == 0.0


def test_risk_level_boundaries_are_explicit():
    assert risk_level(9.99) == "BAJO"
    assert risk_level(10) == "MEDIO"
    assert risk_level(29.99) == "MEDIO"
    assert risk_level(30) == "ALTO"
