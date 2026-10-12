"""Tests for udp_arq.rto (owner: Pratik)."""

import pytest

from udp_arq.rto import RTOEstimator


def make_est(**kw):
    defaults = dict(
        initial_rto=0.5, min_rto=0.1, max_rto=5.0, clock_granularity=0.05
    )
    defaults.update(kw)
    return RTOEstimator(**defaults)


def test_first_sample_sets_rfc_initial_values():
    est = make_est()
    rto = est.sample(0.8)
    assert est.srtt == pytest.approx(0.8)
    assert est.rttvar == pytest.approx(0.4)
    assert rto == pytest.approx(0.8 + max(0.05, 4 * 0.4))


def test_converges_toward_constant_rtt():
    est = make_est()
    for _ in range(100):
        est.sample(0.2)
    assert est.srtt == pytest.approx(0.2, abs=1e-6)
    assert est.rttvar == pytest.approx(0.0, abs=1e-6)
    assert est.rto == pytest.approx(0.2 + 0.05)  # SRTT + G


def test_timeout_doubles_rto():
    est = make_est(initial_rto=0.4)
    assert est.on_timeout() == pytest.approx(0.8)
    assert est.on_timeout() == pytest.approx(1.6)


def test_rto_clamped_to_max():
    est = make_est(initial_rto=3.0, max_rto=5.0)
    assert est.on_timeout() == pytest.approx(5.0)
    assert est.on_timeout() == pytest.approx(5.0)


def test_fresh_sample_recovers_after_backoff():
    est = make_est(initial_rto=0.4)
    est.on_timeout()          # 0.8
    est.sample(0.2)           # fresh (Karn) sample pulls it back down
    assert est.rto < 0.8
    assert est.rto >= est.min_rto


def test_invalid_rtt_rejected():
    with pytest.raises(ValueError):
        make_est().sample(0)


def test_invalid_bounds_rejected():
    with pytest.raises(ValueError):
        RTOEstimator(initial_rto=0.5, min_rto=1.0)