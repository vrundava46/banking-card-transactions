from streaming.geo import haversine_km, implied_speed_kmh


def test_haversine_known_distance():
    # London (51.5, -0.13) to Paris (48.85, 2.35) ~= 340 km
    d = haversine_km(51.5, -0.13, 48.85, 2.35)
    assert 300 < d < 380


def test_zero_distance():
    assert haversine_km(10.0, 20.0, 10.0, 20.0) == 0.0


def test_implied_speed():
    # 340 km in 0.5 h -> 680 km/h
    speed = implied_speed_kmh(340.0, 0.5)
    assert 670 < speed < 690


def test_implied_speed_zero_time_is_infinite():
    assert implied_speed_kmh(100.0, 0.0) == float("inf")
