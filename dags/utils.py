import sys
import json
import requests
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook

def get_agencies() -> dict:
    url = Variable.get("URL_AGENCIAS")+Variable.get("URL_AGENCIAS_ADD_QUERY")+Variable.get("URL_AGENCIAS_QUERY_TOP")+Variable.get("URL_AGENCIAS_VALUE_TOP")+Variable.get("URL_AGENCIAS_CONCAT")+Variable.get("URL_AGENCIAS_QUERY_SKIP")+Variable.get("URL_AGENCIAS_CONCAT")+Variable.get("URL_AGENCIAS_QUERY_FORMAT")+Variable.get("URL_AGENCIAS_CONCAT")+Variable.get("URL_AGENCIAS_QUERY_FILDERS")

    response = requests.get(url)

    for agencias in response.json().get("value", []):
        yield {
            "cnpj_base": agencias["CnpjBase"],
            "cnpj_sequencial": agencias["CnpjSequencial"],
            "cnpj_dv": agencias["CnpjDv"],
            "nome_if": agencias["NomeIf"],
            "segmento": agencias["Segmento"],
            "codigo_compensacao": agencias["CodigoCompe"],
            "nome_agencia": agencias["NomeAgencia"],
            "endereco": agencias["Endereco"],
            "numero": agencias["Numero"],
            "complemento": agencias["Complemento"],
            "bairro": agencias["Bairro"],
            "cep": agencias["Cep"],
            "municipio_ibge": agencias["MunicipioIbge"],
            "municipio": agencias["Municipio"],
            "uf": agencias["UF"],
            "data_inicio": agencias["DataInicio"],
            "ddd": agencias["DDD"],
            "telefone": agencias["Telefone"],
            "posicao": agencias["Posicao"]
        }

def load_agencies():

    insert_question_query = """
        INSERT INTO public.agency (
            cnpj_base,
            cnpj_sequencial,
            cnpj_dv,
            nome_if,
            segmento,
            codigo_compensacao,
            nome_agencia,
            endereco,
            numero,
            complemento,
            bairro,
            cep,
            municipio_ibge,
            municipio,
            uf,
            data_inicio,
            ddd,
            telefone,
            posicao
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s); 
        """

    rows = get_agencies()
    for row in rows:
        row = tuple(row.values())
        pg_hook = PostgresHook(postgres_conn_id='postgres_server')
        pg_hook.run(insert_question_query, parameters=(row))

def tssplit(s, quote='"\'', delimiter=':;,', escape='/^', trim=''):
    """Split a string by delimiters with quotes and escaped characters, optionally trimming results

    :param s: A string to split into chunks
    :param quote: Quote signs to protect a part of s from parsing
    :param delimiter: A chunk separator symbol
    :param escape: An escape character
    :param trim: Trim characters from chunks
    :return: A list of chunks
    """

    in_quotes = in_escape = False
    token = ''
    result = []

    for c in s:
        if in_escape:
            token += c
            in_escape = False
        elif c in escape:
            in_escape = True
            if in_quotes:
                token += c
        elif c in quote and not in_escape:
            in_quotes = False if in_quotes else True
        elif c in delimiter and not in_quotes:
            if trim:
                token = token.strip(trim)
            result.append(token)
            token = ''
        else:
            token += c

    if trim:
        token = token.strip(trim)
    result.append(token)
    return result

def load_banks():
    gcs_hook = GCSHook(gcp_conn_id='gcp_storage_bucket')
    blob = gcs_hook.download(object_name="ParticipantesSTRport.csv", bucket_name="airflow_bucket_banks")
    str_file = blob.decode("utf-8").splitlines()
    insert_question_query = """
        INSERT INTO public.bank (
            ispb,
            nome_reduzido,
            bank_code,
            flag_compensacao,
            acesso_principal,
            nome_extenso,
            inicio_operacao
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s); 
        """
    for row in str_file:
        valores = tuple(tssplit(row, quote='"', delimiter=','))
        pg_hook = PostgresHook(postgres_conn_id='postgres_server')
        pg_hook.run(insert_question_query, parameters=(valores))