"""Соответствие «город из Polymarket slug» → станция резолва и lat/lon для NWS API.

Polymarket резолвит temperature-рынки по конкретной станции Weather Underground.
Для US-городов станция ≡ ASOS/METAR той же локации, данные NWS и WU совпадают
(оба тянут из одного сенсора).

Пока покрываем только US-города (для NWS api.weather.gov). Остальные добавим,
когда будем подключать Open-Meteo.
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Station:
    slug_city: str       # как в Polymarket slug, напр. "nyc", "los-angeles"
    display: str         # человеческое имя
    resolution_station: str  # как пишется в описании рынка, для лога
    icao: str            # METAR код
    lat: float
    lon: float
    timezone: str        # IANA, напр. "America/New_York" — нужно для «day-of»


# US stations (NWS coverage). Координаты станции резолва, не центра города.
US_STATIONS: dict[str, Station] = {
    "nyc": Station(
        slug_city="nyc",
        display="New York City",
        resolution_station="LaGuardia Airport",
        icao="KLGA",
        lat=40.7769, lon=-73.8740,
        timezone="America/New_York",
    ),
    "los-angeles": Station(
        slug_city="los-angeles",
        display="Los Angeles",
        resolution_station="LAX",
        icao="KLAX",
        lat=33.9425, lon=-118.4081,
        timezone="America/Los_Angeles",
    ),
    "chicago": Station(
        slug_city="chicago",
        display="Chicago",
        resolution_station="O'Hare",
        icao="KORD",
        lat=41.9742, lon=-87.9073,
        timezone="America/Chicago",
    ),
    "miami": Station(
        slug_city="miami",
        display="Miami",
        resolution_station="Miami Intl",
        icao="KMIA",
        lat=25.7932, lon=-80.2906,
        timezone="America/New_York",
    ),
    "atlanta": Station(
        slug_city="atlanta",
        display="Atlanta",
        resolution_station="Hartsfield-Jackson",
        icao="KATL",
        lat=33.6407, lon=-84.4277,
        timezone="America/New_York",
    ),
    "dallas": Station(
        slug_city="dallas",
        display="Dallas",
        resolution_station="DFW",
        icao="KDFW",
        lat=32.8998, lon=-97.0403,
        timezone="America/Chicago",
    ),
    "houston": Station(
        slug_city="houston",
        display="Houston",
        resolution_station="George Bush IAH",
        icao="KIAH",
        lat=29.9902, lon=-95.3368,
        timezone="America/Chicago",
    ),
    "austin": Station(
        slug_city="austin",
        display="Austin",
        resolution_station="Austin-Bergstrom",
        icao="KAUS",
        lat=30.1945, lon=-97.6699,
        timezone="America/Chicago",
    ),
    "denver": Station(
        slug_city="denver",
        display="Denver",
        resolution_station="Denver Intl",
        icao="KDEN",
        lat=39.8561, lon=-104.6737,
        timezone="America/Denver",
    ),
    "seattle": Station(
        slug_city="seattle",
        display="Seattle",
        resolution_station="Seattle-Tacoma",
        icao="KSEA",
        lat=47.4502, lon=-122.3088,
        timezone="America/Los_Angeles",
    ),
    "san-francisco": Station(
        slug_city="san-francisco",
        display="San Francisco",
        resolution_station="SFO",
        icao="KSFO",
        lat=37.6213, lon=-122.3790,
        timezone="America/Los_Angeles",
    ),
}


def get_station(slug_city: str) -> Station | None:
    return US_STATIONS.get(slug_city)
