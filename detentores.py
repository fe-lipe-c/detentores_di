import pandas as pd
from utils import query_di


def df_compra_venda(begin_date, begin_time, end_date, end_time):
    df = query_di(begin_date, end_date)

    df = df.query(
        f'DataHoraFechamento >= "{begin_date} {begin_time}" and DataHoraFechamento <= "{end_date} {end_time}"'
    )
    df.reset_index(drop=True, inplace=True)

    df_contracts = (
        df.groupby(["CodigoInstrumento"])
        .agg({"QuantidadeNegociada": "sum"})
        .reset_index()
    )
    df_contracts.sort_values(
        by=["QuantidadeNegociada"],
        ascending=False,
        inplace=True,
    )
    df_contracts.reset_index(drop=True, inplace=True)
    list_cod = df_contracts["CodigoInstrumento"].tolist()

    # Sum the quantity of contracts traded by each participant,
    # separated by buyer and seller.
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
    df_total.query(
        'Participante == "XP INVESTIMENTOS CCTVM S/A" and CodigoInstrumento == "DI1F25"'
    )

    return df_total
