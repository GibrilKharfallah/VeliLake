"""Tests de l'API (endpoints ne necessitant pas de services actifs).

/health repond meme sans S3/MySQL/MongoDB : les ping echouent proprement et le
statut passe a 'degraded' sans planter. On teste donc la forme de la reponse,
pas la connectivite reelle.
"""
from fastapi.testclient import TestClient

from src.api.main import app

client = TestClient(app)


def test_health_shape():
    r = client.get("/health")
    assert r.status_code == 200
    body = r.json()
    assert body["status"] in ("ok", "degraded")
    assert "timestamp" in body
    assert set(body["services"]) == {"s3", "mysql", "mongodb"}


def test_root_lists_endpoints():
    r = client.get("/")
    assert r.status_code == 200
    endpoints = r.json()["endpoints"]
    for expected in ("/health", "/raw", "/staging", "/curated", "/stats",
                     "/ingest", "/ingest_fast"):
        assert expected in endpoints


def test_ingest_rejects_empty_data():
    r = client.post("/ingest", json={"source": "manual", "data": []})
    assert r.status_code == 400