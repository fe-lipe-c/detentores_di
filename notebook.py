import pandas as pd
import numpy as np
import altair as alt
from utils import query_di
import config as cfg

begin_date = "2023-11-08"
end_date = "2023-11-08"
df = query_di(begin_date, end_date)
df = df.query(
    'DataHoraFechamento >= "2023-11-09 09:00:00" and DataHoraFechamento <= "2023-11-09 10:30:00"'
)
df.reset_index(drop=True, inplace=True)
df

df_contracts = (
    df.groupby(["CodigoInstrumento"]).agg({"QuantidadeNegociada": "sum"}).reset_index()
)
df_contracts.sort_values(by=["QuantidadeNegociada"], ascending=False, inplace=True)
df_contracts.reset_index(drop=True, inplace=True)
list_cod = df_contracts["CodigoInstrumento"].tolist()

# Sum the quantity of contracts traded by each participant, separated by buyer and seller.
df_buyer = (
    df.groupby(["Comprador", "CodigoInstrumento"])
    .agg({"QuantidadeNegociada": "sum"})
    .reset_index()
)
df_seller = (
    df.groupby(["Vendedor", "CodigoInstrumento"])
    .agg({"QuantidadeNegociada": "sum"})
    .reset_index()
)
df_seller["Tipo"] = "Vendedor"
df_buyer["Tipo"] = "Comprador"
df_seller.rename(
    columns={"Vendedor": "Participante", "QuantidadeNegociada": "Quantidade"},
    inplace=True,
)
df_buyer.rename(
    columns={"Comprador": "Participante", "QuantidadeNegociada": "Quantidade"},
    inplace=True,
)
df_seller["Quantidade"] = df_seller["Quantidade"] * -1

df_total = pd.concat([df_buyer, df_seller])
df_total.reset_index(drop=True, inplace=True)
# Get the total quantity of contracts traded by each instrument
df_total["newcod"] = df_total["CodigoInstrumento"].apply(
    lambda x: x if x in list_cod[:5] else "Outros"
)

chart_buyer = (
    alt.Chart(df_total)
    .mark_bar()
    .encode(
        alt.X("Quantidade", title="Quantidade de contratos", sort="size"),
        alt.Y("Participante", sort="y"),
        color=alt.Color("newcod").scale(scheme="tableau10"),
    )
)
chart_seller = (
    alt.Chart(df_total)
    .mark_bar()
    .encode(
        alt.X("Quantidade", title="Quantidade de contratos", sort="size"),
        alt.Y("Participante", sort="y"),
        color=alt.Color("newcod").scale(scheme="viridis"),
    )
)
# Insert vertical rule in the 0 value
chart_rule = (
    alt.Chart(pd.DataFrame({"x": [0]}))
    .mark_rule(color="black", strokeWidth=2)
    .encode(x="x")
)
chart_total = (chart_buyer + chart_seller + chart_rule).properties(
    width=1200,
    height=1000,
    title=f"Volume de contratos negociados por participante ({begin_date})",
)  # ,background="#f0f8ff")
chart_total.save("chart_total.html")

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
