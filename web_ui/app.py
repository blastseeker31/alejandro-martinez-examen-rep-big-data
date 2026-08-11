"""Dashboard académico de AgroStream IoT."""

from __future__ import annotations

from datetime import UTC, datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import streamlit as st

from web_ui.api_client import ApiClient

st.set_page_config(page_title="AgroStream IoT", page_icon="🌱", layout="wide")


@st.cache_resource
def get_client() -> ApiClient:
    return ApiClient()


@st.cache_data(ttl=5)
def get_parcels() -> list[dict]:
    return get_client().get("/parcels")


@st.cache_data(ttl=5)
def get_summary() -> dict:
    return get_client().get("/metrics/summary")


@st.cache_data(ttl=5)
def get_aggregates(parcel_id: str, measurement_type: str | None) -> list[dict]:
    params = {"parcel_id": parcel_id}
    if measurement_type:
        params["measurement_type"] = measurement_type
    return get_client().get("/dashboard/aggregates", **params)


@st.cache_data(ttl=5)
def get_readings(parcel_id: str, measurement_type: str | None) -> list[dict]:
    params = {"parcel_id": parcel_id, "limit": 500}
    if measurement_type:
        params["measurement_type"] = measurement_type
    return get_client().get("/dashboard/readings", **params)


def show_request_error(exc: Exception) -> None:
    st.error(f"No fue posible consultar la API: {exc}")


def render_generator(parcels: list[dict]) -> None:
    st.header("Generador de lecturas")
    mode = st.radio("Tipo de envío", ["Individual", "Masivo"], horizontal=True)
    client = get_client()
    if mode == "Individual":
        parcel = st.selectbox(
            "Parcela",
            parcels,
            format_func=lambda item: f"{item['parcel_id']} · {item['parcel_name']}",
        )
        measurement = st.selectbox(
            "Tipo de medición", ["temperature", "soil_moisture", "air_humidity", "ph"]
        )
        safe_min, safe_max = parcel["safe_ranges"][measurement]
        value = st.number_input("Valor", value=float((safe_min + safe_max) / 2), format="%.3f")
        sensor_id = st.text_input("Sensor", value=f"{parcel['parcel_id']}-S01")
        if st.button("Publicar lectura individual", type="primary"):
            try:
                response = client.post(
                    "/events/single",
                    {
                        "parcel_id": parcel["parcel_id"],
                        "sensor_id": sensor_id,
                        "measurement_type": measurement,
                        "value": value,
                        "event_timestamp": datetime.now(UTC).isoformat(),
                    },
                )
                st.success("Entrega confirmada por Kafka")
                st.json(response)
                get_summary.clear()
            except requests.RequestException as exc:
                show_request_error(exc)
    else:
        count = st.selectbox("Cantidad del lote", [100, 1000, 5000, 10000], index=1)
        scenario = st.selectbox(
            "Escenario",
            ["stable", "heat_wave", "irrigation_failure", "heavy_rain", "mixed"],
            format_func=lambda value: {
                "stable": "Estable",
                "heat_wave": "Ola de calor",
                "irrigation_failure": "Falla de riego",
                "heavy_rain": "Lluvia intensa",
                "mixed": "Mixto",
            }[value],
        )
        anomaly = st.slider("Porcentaje de anomalías", 0, 100, 15)
        duplicates = st.slider("Porcentaje de duplicados", 0, 50, 0)
        malformed = st.slider("Porcentaje de malformados", 0, 20, 0)
        seed_text = st.text_input("Semilla opcional", value="20260811")
        if st.button("Generar y publicar lote", type="primary"):
            try:
                seed = int(seed_text) if seed_text.strip() else None
                with st.spinner("Generando, publicando y esperando confirmaciones..."):
                    response = client.post(
                        "/events/batch",
                        {
                            "count": count,
                            "scenario": scenario,
                            "anomaly_percent": anomaly,
                            "duplicate_percent": duplicates,
                            "malformed_percent": malformed,
                            "seed": seed,
                        },
                    )
                st.success("Lote publicado")
                st.metric("Confirmados", response["deliveries_confirmed"])
                st.metric("Eventos por segundo", response["events_per_second"])
                st.json(response)
                get_summary.clear()
            except (ValueError, requests.RequestException) as exc:
                show_request_error(exc)


def render_dashboard(parcels: list[dict]) -> None:
    st.header("Dashboard de alertas")
    parcel = st.selectbox(
        "Seleccione una parcela",
        parcels,
        format_func=lambda item: (
            f"{item['parcel_id']} · {item['parcel_name']} ({item['crop_type']})"
        ),
        key="dashboard_parcel",
    )
    measurement_label = st.selectbox(
        "Filtro de medición",
        ["Todas", "temperature", "soil_moisture", "air_humidity", "ph"],
    )
    measurement = None if measurement_label == "Todas" else measurement_label
    try:
        aggregates = get_aggregates(parcel["parcel_id"], measurement)
        readings = get_readings(parcel["parcel_id"], measurement)
    except requests.RequestException as exc:
        show_request_error(exc)
        return
    aggregate = aggregates[0] if len(aggregates) == 1 else None
    if not aggregate:
        st.info("Todavía no existen lecturas procesadas para esta parcela y filtro.")
        return
    cards = st.columns(5)
    cards[0].metric("Lecturas válidas", aggregate.get("total_valid_readings", 0))
    cards[1].metric("Anomalías", aggregate.get("anomalous_readings", 0))
    cards[2].metric("Tasa de alerta", f"{aggregate.get('alert_rate', 0):.2f}%")
    cards[3].metric("Riesgo", aggregate.get("risk_level", "BAJO"))
    cards[4].metric("Último valor", aggregate.get("last_value", "—"))

    frame = pd.DataFrame(readings)
    if frame.empty:
        return
    frame["event_timestamp"] = pd.to_datetime(frame["event_timestamp"], utc=True)
    frame = frame.sort_values("event_timestamp")
    st.caption(f"Última actualización de la consulta: {datetime.now(UTC).isoformat()}")
    chart = go.Figure()
    for measurement_name, group in frame.groupby("measurement_type"):
        chart.add_trace(
            go.Scatter(
                x=group["event_timestamp"],
                y=group["value"],
                mode="lines+markers",
                name=measurement_name,
                text=group["event_id"],
            )
        )
    if measurement and measurement in frame["measurement_type"].unique():
        selected = frame[frame["measurement_type"] == measurement].iloc[0]
        chart.add_hline(
            y=selected["safe_min"],
            line_dash="dash",
            line_color="green",
            annotation_text="Mínimo seguro",
        )
        chart.add_hline(
            y=selected["safe_max"],
            line_dash="dash",
            line_color="orange",
            annotation_text="Máximo seguro",
        )
    chart.update_layout(title="Serie temporal de lecturas", xaxis_title="UTC", yaxis_title="Valor")
    st.plotly_chart(chart, use_container_width=True)

    left, right = st.columns(2)
    with left:
        status = (
            frame["is_anomaly"].map({True: "Anómala", False: "Segura"}).value_counts().reset_index()
        )
        status.columns = ["estado", "cantidad"]
        st.plotly_chart(
            px.bar(status, x="estado", y="cantidad", title="Lecturas seguras y anómalas"),
            use_container_width=True,
        )
    with right:
        anomaly_frame = frame[frame["anomaly_type"].notna()]
        if anomaly_frame.empty:
            st.info("No hay anomalías en la ventana consultada.")
        else:
            distribution = anomaly_frame["anomaly_type"].value_counts().reset_index()
            distribution.columns = ["tipo", "cantidad"]
            st.plotly_chart(
                px.pie(distribution, names="tipo", values="cantidad", title="Tipo de anomalía"),
                use_container_width=True,
            )
    st.subheader("Lecturas recientes")
    st.dataframe(
        frame.sort_values("event_timestamp", ascending=False).head(25),
        use_container_width=True,
        hide_index=True,
    )


def render_health() -> None:
    st.header("Arquitectura y salud")
    client = get_client()
    try:
        health = client.get("/health")
        summary = get_summary()
        checks = health.get("checks", {})
        cols = st.columns(3)
        cols[0].metric("FastAPI", "OK" if health.get("status") == "ok" else "DEGRADADO")
        cols[1].metric("Kafka", "OK" if checks.get("kafka") else "ERROR")
        cols[2].metric("MongoDB", "OK" if checks.get("mongodb") else "ERROR")
        st.subheader("Métricas de procesamiento")
        st.json(summary)
    except requests.RequestException as exc:
        show_request_error(exc)
    st.subheader("Flujo de arquitectura")
    st.code(
        "Streamlit → FastAPI → Kafka agro.sensor-readings\n"
        "                              ├→ consumer group agro-sensor-processors → MongoDB\n"
        "                              ├→ agro.alerts\n"
        "                              └→ agro.sensor-readings-dlq",
        language="text",
    )
    st.markdown(
        "**Semántica:** productor idempotente, procesamiento at-least-once, offsets manuales "
        "y deduplicación durable por `event_id`. Replication factor 1 en desarrollo local no es HA."
    )


def main() -> None:
    st.title("🌱 AgroStream IoT")
    st.caption("Plataforma local de sensores agrícolas en tiempo real")
    try:
        parcels = get_parcels()
    except requests.RequestException as exc:
        show_request_error(exc)
        st.stop()
    page = st.sidebar.radio("Navegación", ["Generador", "Dashboard", "Arquitectura y salud"])
    if page == "Generador":
        render_generator(parcels)
    elif page == "Dashboard":
        render_dashboard(parcels)
    else:
        render_health()


if __name__ == "__main__":
    main()
