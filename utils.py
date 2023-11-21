import clickhouse_driver
import config as cfg

client = clickhouse_driver.Client(
    host="localhost", database="AQDB", settings={"use_numpy": True}
)


def query_di(begin_date, end_date):
    """Query data from Clickhouse database."""

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
        CodigoInstrumento REGEXP '^DI[1-9]'
        AND toDate(DataHoraFechamento) BETWEEN '{begin_date}' AND '{end_date}'
    """

    df = client.query_dataframe(sql)

    participantes = cfg.DI_PARTICIPANTES

    # Create a new column with the name of the buyer and seller.
    df["CodigoParticipanteVendedor"] = df["CodigoParticipanteVendedor"].astype(str)
    df["CodigoParticipanteComprador"] = df["CodigoParticipanteComprador"].astype(str)
    df["Vendedor"] = df["CodigoParticipanteVendedor"].map(participantes)
    df["Comprador"] = df["CodigoParticipanteComprador"].map(participantes)

    return df
