"""Cálculos de indicadores de riesgo para dashboard y agregaciones."""


def alert_rate(anomalous_readings: int, total_valid_readings: int) -> float:
    if total_valid_readings <= 0:
        return 0.0
    return round(anomalous_readings / total_valid_readings * 100, 2)


def risk_level(
    rate_percent: float, medium_threshold: float = 10.0, high_threshold: float = 30.0
) -> str:
    if rate_percent < medium_threshold:
        return "BAJO"
    if rate_percent < high_threshold:
        return "MEDIO"
    return "ALTO"
