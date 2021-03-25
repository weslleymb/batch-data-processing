DO
$do$
	BEGIN
		IF EXISTS(SELECT * FROM pg_views WHERE viewname = 'vw_source_agency') THEN
			DROP VIEW vw_source_agency;
		END IF;

		IF EXISTS(SELECT * FROM pg_views WHERE viewname = 'vw_source_bank') THEN
			DROP VIEW vw_source_bank;
		END IF;
		
		IF EXISTS(SELECT 1 FROM pg_tables WHERE tablename = 'agency') THEN
			DROP TABLE agency;
		END IF;

		IF EXISTS(SELECT 1 FROM pg_tables WHERE tablename = 'bank') THEN
			DROP TABLE bank;
		END IF;

		CREATE TABLE agency (
			cnpj_base character varying null,
			cnpj_sequencial character varying null,
			cnpj_dv character varying null,
			nome_if character varying null,
			segmento character varying null,
			codigo_compensacao character varying null,
			nome_agencia character varying null,
			endereco character varying null,
			numero character varying null,
			complemento character varying null,
			bairro character varying null,
			cep character varying null,
			municipio_ibge character varying null,
			municipio character varying null,
			uf character varying null,
			data_inicio character varying null,
			ddd character varying null,
			telefone character varying null,
			posicao character varying null
		);
		
		CREATE TABLE bank (
			ispb character varying,
			nome_reduzido character varying,
			bank_code character varying,
			flag_compensacao character varying,
			acesso_principal character varying,
			nome_extenso character varying,
			inicio_operacao character varying
		);

		CREATE OR REPLACE VIEW public.vw_source_agency
		 AS
		WITH cte_dedup AS (
		  SELECT DISTINCT concat(bank.bank_code, '-', agency.codigo_compensacao) AS key_agency,
			agency.cnpj_base,
			agency.cnpj_sequencial,
			agency.cnpj_dv,
			agency.nome_if,
			agency.segmento,
			agency.codigo_compensacao,
			agency.nome_agencia,
			agency.endereco,
			agency.numero,
			agency.complemento,
			agency.bairro,
			agency.cep,
			agency.municipio_ibge,
			agency.municipio,
			agency.uf,
			agency.data_inicio,
			agency.ddd,
			agency.telefone,
			agency.posicao,
			ROW_NUMBER() OVER (PARTITION BY concat(bank.bank_code, '-', agency.codigo_compensacao) ORDER BY agency.cnpj_sequencial) AS rowid
			FROM agency
			LEFT JOIN bank ON bank.ispb::text = agency.cnpj_base::text
		) SELECT key_agency,
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
			posicao,
			rowid
		FROM cte_dedup
		WHERE rowid = 1;

		ALTER TABLE public.vw_source_agency
			OWNER TO airflow;
		
		CREATE OR REPLACE VIEW public.vw_source_bank
		AS
		SELECT rtrim(bank.ispb::text) AS ispb,
			rtrim(bank.nome_reduzido::text) AS nome_reduzido,
			rtrim(bank.bank_code::text) AS bank_code,
			rtrim(bank.flag_compensacao::text) AS flag_compensacao,
			rtrim(bank.acesso_principal::text) AS acesso_principal,
			rtrim(bank.nome_extenso::text) AS nome_extenso,
			rtrim(bank.inicio_operacao::text) AS inicio_operacao
		FROM bank
		WHERE try_cast(bank.bank_code::text, NULL::integer) IS NOT NULL;

		ALTER TABLE public.vw_source_bank
			OWNER TO airflow;

		CREATE OR REPLACE FUNCTION try_cast(_in text, INOUT _out ANYELEMENT) AS
		$func$
		BEGIN
		EXECUTE format('SELECT %L::%s', $1, pg_typeof(_out))
		INTO  _out;
		EXCEPTION WHEN others THEN
		-- do nothing: _out already carries default
		END
		$func$  LANGUAGE plpgsql;
	END;
$do$