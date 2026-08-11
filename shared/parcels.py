"""Catálogo ficticio de parcelas agrícolas hondureñas."""

from dataclasses import dataclass

from shared.models import MeasurementType


@dataclass(frozen=True)
class Parcel:
    parcel_id: str
    parcel_name: str
    crop_type: str
    size_hectares: float
    location: str
    latitude: float
    longitude: float
    sensor_count: int
    safe_ranges: dict[MeasurementType, tuple[float, float]]


PARCELS = (
    Parcel(
        "HN-ATL-001",
        "Finca La Ceiba",
        "banano",
        48.5,
        "La Ceiba, Atlántida",
        15.763,
        -86.782,
        12,
        {
            MeasurementType.TEMPERATURE: (18, 32),
            MeasurementType.SOIL_MOISTURE: (28, 72),
            MeasurementType.AIR_HUMIDITY: (55, 92),
            MeasurementType.PH: (5.5, 7.0),
        },
    ),
    Parcel(
        "HN-ATL-002",
        "Hacienda El Porvenir",
        "cacao",
        16.2,
        "Jutiapa, Atlántida",
        15.665,
        -86.493,
        7,
        {
            MeasurementType.TEMPERATURE: (19, 31),
            MeasurementType.SOIL_MOISTURE: (35, 78),
            MeasurementType.AIR_HUMIDITY: (60, 95),
            MeasurementType.PH: (5.0, 6.8),
        },
    ),
    Parcel(
        "HN-COM-001",
        "Café Las Nubes",
        "cafe",
        23.8,
        "La Esperanza, Intibucá",
        14.310,
        -88.180,
        9,
        {
            MeasurementType.TEMPERATURE: (14, 27),
            MeasurementType.SOIL_MOISTURE: (30, 68),
            MeasurementType.AIR_HUMIDITY: (50, 88),
            MeasurementType.PH: (5.2, 6.5),
        },
    ),
    Parcel(
        "HN-COM-002",
        "Loma del Pino",
        "cafe",
        61.4,
        "Marcala, La Paz",
        14.151,
        -88.034,
        15,
        {
            MeasurementType.TEMPERATURE: (15, 28),
            MeasurementType.SOIL_MOISTURE: (32, 70),
            MeasurementType.AIR_HUMIDITY: (48, 86),
            MeasurementType.PH: (5.0, 6.6),
        },
    ),
    Parcel(
        "HN-COR-001",
        "Valle del Sol",
        "maiz",
        92.0,
        "Comayagua, Comayagua",
        14.456,
        -87.639,
        18,
        {
            MeasurementType.TEMPERATURE: (18, 35),
            MeasurementType.SOIL_MOISTURE: (24, 65),
            MeasurementType.AIR_HUMIDITY: (40, 82),
            MeasurementType.PH: (5.8, 7.2),
        },
    ),
    Parcel(
        "HN-COR-002",
        "Parcela La Milpa",
        "frijol",
        11.7,
        "Siguatepeque, Comayagua",
        14.595,
        -87.833,
        5,
        {
            MeasurementType.TEMPERATURE: (17, 31),
            MeasurementType.SOIL_MOISTURE: (28, 66),
            MeasurementType.AIR_HUMIDITY: (45, 84),
            MeasurementType.PH: (5.5, 7.0),
        },
    ),
    Parcel(
        "HN-COL-001",
        "Palmares del Aguán",
        "palma_africana",
        135.6,
        "Tocoa, Colón",
        15.650,
        -86.010,
        24,
        {
            MeasurementType.TEMPERATURE: (20, 34),
            MeasurementType.SOIL_MOISTURE: (30, 76),
            MeasurementType.AIR_HUMIDITY: (58, 94),
            MeasurementType.PH: (4.8, 6.5),
        },
    ),
    Parcel(
        "HN-COL-002",
        "Río Tinto",
        "banano",
        74.3,
        "Trujillo, Colón",
        15.916,
        -85.954,
        16,
        {
            MeasurementType.TEMPERATURE: (19, 33),
            MeasurementType.SOIL_MOISTURE: (30, 74),
            MeasurementType.AIR_HUMIDITY: (55, 93),
            MeasurementType.PH: (5.4, 7.0),
        },
    ),
    Parcel(
        "HN-COP-001",
        "Cacao Copán",
        "cacao",
        19.5,
        "Copán Ruinas, Copán",
        14.838,
        -89.156,
        8,
        {
            MeasurementType.TEMPERATURE: (18, 30),
            MeasurementType.SOIL_MOISTURE: (34, 75),
            MeasurementType.AIR_HUMIDITY: (55, 91),
            MeasurementType.PH: (5.0, 6.7),
        },
    ),
    Parcel(
        "HN-CHO-001",
        "Los Pinares",
        "cafe",
        37.9,
        "San José de Colinas, Santa Bárbara",
        14.833,
        -88.295,
        10,
        {
            MeasurementType.TEMPERATURE: (15, 29),
            MeasurementType.SOIL_MOISTURE: (30, 70),
            MeasurementType.AIR_HUMIDITY: (50, 89),
            MeasurementType.PH: (5.1, 6.6),
        },
    ),
    Parcel(
        "HN-CHO-002",
        "El Cedral",
        "frijol",
        8.4,
        "Quimistán, Santa Bárbara",
        15.350,
        -88.400,
        4,
        {
            MeasurementType.TEMPERATURE: (18, 32),
            MeasurementType.SOIL_MOISTURE: (26, 64),
            MeasurementType.AIR_HUMIDITY: (42, 82),
            MeasurementType.PH: (5.6, 7.1),
        },
    ),
    Parcel(
        "HN-OLN-001",
        "Finca El Jícaro",
        "maiz",
        52.6,
        "Juticalpa, Olancho",
        14.667,
        -86.220,
        13,
        {
            MeasurementType.TEMPERATURE: (19, 36),
            MeasurementType.SOIL_MOISTURE: (22, 62),
            MeasurementType.AIR_HUMIDITY: (38, 80),
            MeasurementType.PH: (5.7, 7.3),
        },
    ),
    Parcel(
        "HN-OLN-002",
        "Sabana Verde",
        "palma_africana",
        108.1,
        "Catacamas, Olancho",
        14.806,
        -85.895,
        21,
        {
            MeasurementType.TEMPERATURE: (20, 35),
            MeasurementType.SOIL_MOISTURE: (28, 72),
            MeasurementType.AIR_HUMIDITY: (45, 86),
            MeasurementType.PH: (5.0, 6.8),
        },
    ),
    Parcel(
        "HN-VAL-001",
        "El Guayacán",
        "maiz",
        28.1,
        "Nacaome, Valle",
        13.533,
        -87.487,
        7,
        {
            MeasurementType.TEMPERATURE: (22, 38),
            MeasurementType.SOIL_MOISTURE: (18, 56),
            MeasurementType.AIR_HUMIDITY: (35, 76),
            MeasurementType.PH: (5.8, 7.4),
        },
    ),
    Parcel(
        "HN-FCO-001",
        "Montaña Azul",
        "cafe",
        44.7,
        "Danlí, El Paraíso",
        14.033,
        -86.570,
        12,
        {
            MeasurementType.TEMPERATURE: (14, 28),
            MeasurementType.SOIL_MOISTURE: (31, 71),
            MeasurementType.AIR_HUMIDITY: (52, 90),
            MeasurementType.PH: (5.0, 6.5),
        },
    ),
    Parcel(
        "HN-FCO-002",
        "Cosecha del Oriente",
        "frijol",
        15.3,
        "Yuscarán, El Paraíso",
        13.944,
        -86.829,
        6,
        {
            MeasurementType.TEMPERATURE: (16, 31),
            MeasurementType.SOIL_MOISTURE: (27, 67),
            MeasurementType.AIR_HUMIDITY: (43, 83),
            MeasurementType.PH: (5.5, 7.0),
        },
    ),
)


def get_parcel(parcel_id: str) -> Parcel:
    for parcel in PARCELS:
        if parcel.parcel_id == parcel_id:
            return parcel
    raise KeyError(f"Parcela desconocida: {parcel_id}")
