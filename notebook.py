import pandas as pd
import numpy as np
import altair as alt
from utils import query_di
import config as cfg
from detentores import df_compra_venda


alt.data_transformers.enable("vegafusion")


import clickhouse_driver

client = clickhouse_driver.Client(
    host="localhost", database="AQDB", settings={"use_numpy": True}
)

cod_security = "DI1F33"


sql = f"""
SELECT
    CodigoInstrumento,
    DataReferencia,
    DataHoraFechamento,
    CodigoParticipanteComprador,
    CodigoParticipanteVendedor,
    toFloat64(PrecoNegocio) AS PrecoNegocio,
    QuantidadeNegociada
FROM TradeIntraday
WHERE
    CodigoInstrumento LIKE 'DI1F33'
    AND toDate(DataHoraFechamento) BETWEEN '{begin_date}' AND '{end_date}'
"""

df = client.query_dataframe(sql)
df.query(
    'DataHoraFechamento >= "2023-11-09 10:10:00" and DataHoraFechamento <= "2023-11-09 10:10:02"'
)

import pyarrow.ipc as ipc
import pyarrow as pa

file_path = "/home/felipe/AlgebraQuant/data/TradeIntraday/processed/2023-11-09_TradeIntraday.arrow"
with open(file_path, "rb") as f:
    reader = ipc.open_file(f)
    table = reader.read_all()

record_batch = pa.RecordBatch.from_pandas(df_)
df = table.to_pandas(strings_to_categorical=True)
new_schema = pa.schema(
    [
        (field.name, pa.int32() if field.type == pa.uint32() else field.type)
        for field in table.schema
    ]
)

table = table.cast(new_schema)
table
df_processed = table.to_pandas(strings_to_categorical=True)

begin_date = "2023-11-09"
end_date = "2023-11-09"
begin_time = "09:00:00"
end_time = "18:30:00"

# df = df_compra_venda(begin_date, begin_time, end_date, end_time)

df = query_di(begin_date, end_date, cod_security="DI1F33")

df.query(
    'CodigoInstrumento == "DI1F33" and DataHoraFechamento >= "2023-11-09 10:10:00" and DataHoraFechamento <= "2023-11-09 10:10:02"'
)

df.query(
    'DataHoraFechamento >= "2023-11-09 10:10:00" and DataHoraFechamento <= "2023-11-09 10:10:02"'
)


df.drop(
    columns=[
        "CodigoParticipanteVendedor",
        "CodigoParticipanteComprador",
        "DataReferencia",
        "PrecoNegocio",
    ],
    inplace=True,
)

participantes_venda = df["Vendedor"].unique().tolist()
participantes_compra = df["Comprador"].unique().tolist()
# Conjunto dos participantes
participantes = set(participantes_venda + participantes_compra)
participantes = list(participantes)

df_melted = pd.melt(
    df,
    id_vars=["DataHoraFechamento", "QuantidadeNegociada", "CodigoInstrumento"],
    value_vars=["Vendedor", "Comprador"],
    var_name="Transacao",
    value_name="Participante",
)

transaction_dict = {"Vendedor": -1, "Comprador": 1}
df_melted["QuantidadeNegociada"] = df_melted["QuantidadeNegociada"] * df_melted[
    "Transacao"
].map(transaction_dict)

df_melted.sort_values(by=["DataHoraFechamento"], inplace=True)
df_melted.reset_index(drop=True, inplace=True)
df_grouped = df_melted.groupby(
    ["DataHoraFechamento", "Participante", "CodigoInstrumento"], as_index=False
)["QuantidadeNegociada"].sum()
df_grouped["NetPosition"] = df_grouped.groupby(["Participante", "CodigoInstrumento"])[
    "QuantidadeNegociada"
].cumsum()
df_grouped.reset_index(drop=True, inplace=True)

contrato_ = "DI1F33"
df_grouped_f25 = df_grouped.query(f"CodigoInstrumento == '{contrato_}'")
df_grouped_f25.head(15)

df_grouped_f25
df_grouped_f25.query(
    'DataHoraFechamento >= "2023-11-09 10:10:00" and DataHoraFechamento <= "2023-11-09 10:10:02"'
)

list_dealers_stn = [
    "ITAU CV S/A",
    "BRADESCO S/A CTVM",
    "BTG PACTUAL CTVM S/A",
    "XP INVESTIMENTOS CCTVM S.A.",
    "BGC LIQUIDEZ DTVM LTDA",
    "TERRA INVESTIMENTOS DTVM LTDA",
    "J.P. MORGAN CCVM S/A",
    "GOLDMAN SACHS DO BRASIL CTVM SA",
    "BANCO DO BRASIL S.A.",
    "CAIXA ECONOMICA FEDERAL",
    "SANTANDER CCVM S/A",
]

df_grouped_dealers = df_grouped_f25[
    df_grouped_f25["Participante"].isin(list_dealers_stn)
]
df_grouped_dealers.reset_index(drop=True, inplace=True)

new_names = {
    "SANTANDER CCVM S/A": "SANTANDER",
    "BANCO DO BRASIL S.A.": "BB",
    "CAIXA ECONOMICA FEDERAL": "CEF",
    "XP INVESTIMENTOS CCTVM S.A.": "XP",
    "BRADESCO S/A CTVM": "BRADESCO",
    "BTG PACTUAL CTVM S/A": "BTG",
    "ITAU CV S/A": "ITAU",
    "J.P. MORGAN CCVM S/A": "JP",
    "GOLDMAN SACHS DO BRASIL CTVM SA": "GS",
    "BGC LIQUIDEZ DTVM LTDA": "BGC",
    "TERRA INVESTIMENTOS DTVM LTDA": "TERRA",
}

df_grouped_dealers["Participante"] = df_grouped_dealers["Participante"].map(new_names)

eventos = pd.DataFrame(
    [
        {
            "start": f"{begin_date} 09:40:00",
            "end": f"{begin_date} 09:40:00",
            "event": "Pré-Lote",
        },
        {
            "start": f"{begin_date} 10:30:00",
            "end": f"{begin_date} 10:30:00",
            "event": "Portarias",
        },
        {
            "start": f"{begin_date} 11:45:00",
            "end": f"{begin_date} 11:45:00",
            "event": "Resultado",
        },
    ]
)

df_grouped_dealers.rename(
    {"CodigoInstrumento": "Codigo", "QuantidadeNegociada": "Quantidade"},
    axis=1,
    inplace=True,
)
df_grouped_dealers.head(15)

chart_rule = (
    alt.Chart(eventos).mark_rule(color="black", strokeWidth=1).encode(alt.X("start:T"))
)
chart_text = (
    alt.Chart(eventos)
    .mark_text(
        align="left",
        baseline="top",
        dy=-147,
        dx=1,
        size=4,
    )
    .encode(alt.X("start:T"), text="event:N")
)

chart_position = (
    alt.Chart(
        df_grouped_dealers,
        title=f"Net Position {contrato_} - Período: {begin_date} - {end_date}",
    )
    .mark_line()
    .encode(
        alt.X(
            "DataHoraFechamento:T",
            axis=alt.Axis(
                format="%H:%M",
                labelFontSize=4,
                # titleFontSize=10,
                title="Data",
            ),
        ),
        alt.Y("NetPosition"),
        color="Participante",
    )
)

chart_total = chart_position + chart_rule + chart_text

chart_total.properties(width=5000, height=5000).configure_axis(grid=False)

chart_total.save("chart_total.html")
df_grouped_dealers.info()

chart_position.properties(width=1000, height=1000).configure_axis(grid=False)
chart_position.save("chart_position.html")

# 0      2023-11-20 09:00:00.023           ITAU CV S/A                    0

# chart_buyer = (
#     alt.Chart(df_total)
#     .mark_bar()
#     .encode(
#         alt.X("Quantidade", title="Quantidade de contratos", sort="size"),
#         alt.Y("Participante", sort="y"),
#         color=alt.Color("newcod").scale(scheme="tableau10"),
#     )
# )
# chart_seller = (
#     alt.Chart(df_total)
#     .mark_bar()
#     .encode(
#         alt.X("Quantidade", title="Quantidade de contratos", sort="size"),
#         alt.Y("Participante", sort="y"),
#         color=alt.Color("newcod").scale(scheme="viridis"),
#     )
# )
# # Insert vertical rule in the 0 value
# chart_rule = (
#     alt.Chart(pd.DataFrame({"x": [0]}))
#     .mark_rule(color="black", strokeWidth=2)
#     .encode(x="x")
# )
# chart_total = (chart_buyer + chart_seller + chart_rule).properties(
#     width=1200,
#     height=1000,
#     title=f"Volume de contratos negociados por participante ({begin_date})",
# )  # ,background="#f0f8ff")
# chart_total.save("chart_total.html")

# chart_buyer = (
#     alt.Chart(df_total)
#     .mark_bar()
#     .encode(
#         alt.X("Quantidade", title="Quantidade de contratos"),
#         alt.Y("Participante", sort="y"),
#         color="CodigoInstrumento",
#         tooltip=["Quantidade"],
#     )
# )

# chart_total = chart_total.properties(
#     width=800,
#     height=500,
#     title=f"Volume de contratos negociados por participante ({begin_date})",
# )

# chart_total.save("chart_total.html")

# merged_df = df_buyer.merge(
#     df_seller, how="inner", left_on="Comprador", right_on="Vendedor"
# )
# merged_df.drop(columns=["Vendedor"], inplace=True)
# merged_df = merged_df.rename(
#     columns={
#         "Comprador": "Participante",
#         "QuantidadeNegociada_x": "Comprador",
#         "QuantidadeNegociada_y": "Vendedor",
#     }
# )
# merged_df['Vol_Total'] = merged_df['Comprador'] + merged_df['Vendedor']
# merged_df["Vendedor"] = merged_df["Vendedor"] * -1


# chart_comprador = alt.Chart(merged_df).mark_bar().encode(alt.X("Comprador", title='Quantidade de contratos'), alt.Y("Participante", sort='-x'), color=alt.value("#1f77b4"), tooltip=["Comprador"])

# chart_vendedor = alt.Chart(merged_df).mark_bar().encode(alt.X("Vendedor", title='Quantidade de contratos'), alt.Y("Participante", sort='-x'), color=alt.value("#ff7f0e"), tooltip=["Vendedor"])

# chart = (chart_comprador + chart_vendedor).properties(width=800, height=500, title=f"Volume de contratos negociados por participante ({begin_date})")

# chart.save("chart.html")

import pyarrow as pa
import pyarrow.ipc as ipc
import pandas as pd

# Open the Arrow file
with open(
    "/home/felipe/AlgebraQuant/data/TradeIntraday/processed/2023-11-09_TradeIntraday.arrow",
    "rb",
) as f:
    reader = ipc.open_file(f)
    table = reader.read_all()

# Convert the Arrow Table to a list of dictionaries
data = [row for row in table]
data[0]

# Convert the list of dictionaries to a pandas DataFrame
df = pd.DataFrame(data)

# Now you can use df as a regular pandas DataFrame
