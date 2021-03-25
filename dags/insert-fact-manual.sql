-- SELECT * FROM dim_agency
TRUNCATE TABLE dim_agency CASCADE;
	INSERT INTO dim_agency
	SELECT key_agency, cnpj_base, cnpj_sequencial, cnpj_dv, nome_if, segmento,
		codigo_compensacao, nome_agencia, endereco, numero, complemento,
		bairro, cep, municipio_ibge, municipio, uf, data_inicio, ddd, telefone, posicao
	FROM vw_source_agency;

-- SELECT * FROM dim_bank
TRUNCATE TABLE dim_bank CASCADE;
	INSERT INTO dim_bank
	SELECT * FROM vw_source_bank;

-- SELECT * FROM dim_calendar
TRUNCATE TABLE dim_calendar CASCADE;
	INSERT INTO dim_calendar
	SELECT * FROM vw_source_calendar;

-- SELECT * FROM fact_transfers
TRUNCATE TABLE fact_transfers CASCADE;
	INSERT INTO fact_transfers
	SELECT fact.*
	FROM vw_source_transfers AS fact
		INNER JOIN dim_agency
		ON "fact"."key_agency" = "dim_agency"."key_agency"

		INNER JOIN dim_bank
		ON "fact"."bank_code" = "dim_bank"."bank_code"

		INNER JOIN dim_calendar
		ON "fact"."transaction_date" = "dim_calendar"."transaction_date";