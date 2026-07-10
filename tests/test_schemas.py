"""Tests de validation des schemas Pydantic de l'API."""
import pytest
from pydantic import ValidationError

from src.api.schemas import IngestRequest, IngestResponse, StationIn


def test_station_defaults():
    s = StationIn(station_id="a")
    assert s.num_bikes_available == 0
    assert s.is_renting == 1
    assert s.capacity is None


def test_station_valid_values():
    s = StationIn(station_id="a", num_bikes_available=5, capacity=20)
    assert s.num_bikes_available == 5
    assert s.capacity == 20


def test_station_requires_id():
    with pytest.raises(ValidationError):
        StationIn()  # station_id obligatoire


def test_station_rejects_bad_type():
    with pytest.raises(ValidationError):
        StationIn(station_id="a", num_bikes_available="pas-un-entier")


def test_ingest_request_parses_data():
    r = IngestRequest(source="manual", data=[{"station_id": "a"}])
    assert r.source == "manual"
    assert len(r.data) == 1
    assert isinstance(r.data[0], StationIn)


def test_ingest_request_defaults():
    r = IngestRequest()
    assert r.source == "manual"
    assert r.data == []


def test_ingest_response():
    resp = IngestResponse(status="ok", records_processed=3,
                          duration_ms=12.5, mode="fast")
    assert resp.mode == "fast"
    assert resp.records_processed == 3