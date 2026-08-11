from shared.generator import (
    GenerationRequest,
    Scenario,
    generate_batch,
    generate_malformed_payload,
)
from shared.models import AnomalyType


def test_generator_creates_requested_count_and_batch_id():
    result = generate_batch(GenerationRequest(count=100, seed=7, scenario=Scenario.STABLE))
    assert len(result.events) == 100
    assert result.batch_id is not None
    assert all(event.batch_id == result.batch_id for event in result.events)


def test_generator_uses_non_uniform_agricultural_values():
    result = generate_batch(GenerationRequest(count=200, seed=12, scenario=Scenario.MIXED))
    values = {round(event.value, 4) for event in result.events}
    parcels = {event.parcel_id for event in result.events}
    assert len(values) > 20
    assert len(parcels) > 5


def test_generator_can_create_anomalies_and_duplicates():
    result = generate_batch(
        GenerationRequest(
            count=100,
            anomaly_percent=30,
            duplicate_percent=20,
            seed=3,
            scenario=Scenario.HEAT_WAVE,
        )
    )
    assert result.stats.requested == 100
    assert result.stats.duplicates_requested > 0
    assert result.stats.anomalies_requested > 0
    event_ids = [event.event_id for event in result.events]
    assert len(event_ids) > len(set(event_ids))
    assert any(
        event.anomaly_type in {AnomalyType.ABOVE_MAXIMUM, AnomalyType.BELOW_MINIMUM}
        for event in result.events
    )


def test_malformed_payloads_are_controlled_and_varied():
    payloads = [
        generate_malformed_payload(kind)
        for kind in (
            "text_value",
            "missing_parcel",
            "bad_date",
            "inverted_range",
            "unknown_measurement",
        )
    ]
    assert all(isinstance(payload, dict) for payload in payloads)
    assert payloads[0]["value"] == "not-a-number"
    assert "parcel_id" not in payloads[1]
    assert payloads[3]["safe_min"] > payloads[3]["safe_max"]


def test_malformed_events_keep_the_batch_id():
    result = generate_batch(
        GenerationRequest(count=20, malformed_percent=25, seed=21, scenario=Scenario.STABLE)
    )

    assert len(result.events) == 20
    assert all(
        event["batch_id"] == str(result.batch_id)
        for event in result.events
        if isinstance(event, dict)
    )
