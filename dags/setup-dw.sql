DO
$do$
    BEGIN 
        IF EXISTS(SELECT 1 FROM pg_tables WHERE tablename = 'fact_transfers') THEN
			DROP TABLE fact_transfers;
		END IF;

		IF EXISTS(SELECT 1 FROM pg_tables WHERE tablename = 'dim_agency') THEN
			DROP TABLE dim_agency CASCADE;
		END IF;

		IF EXISTS(SELECT 1 FROM pg_tables WHERE tablename = 'dim_bank') THEN
			DROP TABLE dim_bank CASCADE;
		END IF;

		IF EXISTS(SELECT 1 FROM pg_tables WHERE tablename = 'dim_calendar') THEN
			DROP TABLE dim_calendar CASCADE;
		END IF;
			
		CREATE TABLE public.dim_agency
		(
			key_agency text COLLATE pg_catalog."default" NOT NULL,
			cnpj_base character varying COLLATE pg_catalog."default",
			cnpj_sequencial character varying COLLATE pg_catalog."default",
			cnpj_dv character varying COLLATE pg_catalog."default",
			nome_if character varying COLLATE pg_catalog."default",
			segmento character varying COLLATE pg_catalog."default",
			codigo_compensacao character varying COLLATE pg_catalog."default",
			nome_agencia character varying COLLATE pg_catalog."default",
			endereco character varying COLLATE pg_catalog."default",
			numero character varying COLLATE pg_catalog."default",
			complemento character varying COLLATE pg_catalog."default",
			bairro character varying COLLATE pg_catalog."default",
			cep character varying COLLATE pg_catalog."default",
			municipio_ibge character varying COLLATE pg_catalog."default",
			municipio character varying COLLATE pg_catalog."default",
			uf character varying COLLATE pg_catalog."default",
			data_inicio character varying COLLATE pg_catalog."default",
			ddd character varying COLLATE pg_catalog."default",
			telefone character varying COLLATE pg_catalog."default",
			posicao character varying COLLATE pg_catalog."default",
			CONSTRAINT dim_agency_pkey PRIMARY KEY (key_agency)
		)

		TABLESPACE pg_default;

		ALTER TABLE public.dim_agency
			OWNER to airflow;
		
		CREATE TABLE public.dim_bank
		(
			ispb text COLLATE pg_catalog."default",
			nome_reduzido text COLLATE pg_catalog."default",
			bank_code text COLLATE pg_catalog."default" NOT NULL,
			flag_compensacao text COLLATE pg_catalog."default",
			acesso_principal text COLLATE pg_catalog."default",
			nome_extenso text COLLATE pg_catalog."default",
			inicio_operacao text COLLATE pg_catalog."default",
			CONSTRAINT dim_bank_pkey PRIMARY KEY (bank_code)
		)

		TABLESPACE pg_default;

		ALTER TABLE public.dim_bank
			OWNER to airflow;

        CREATE TABLE public.dim_calendar
        (
            transaction_date date NOT NULL,
            num_day double precision,
            num_month double precision,
            desc_month character varying COLLATE pg_catalog."default",
            num_year double precision,
            year_month double precision,
            desc_month_year text COLLATE pg_catalog."default",
            CONSTRAINT dim_calendar_pkey PRIMARY KEY (transaction_date)
        )

        TABLESPACE pg_default;

        ALTER TABLE public.dim_calendar
            OWNER to airflow;

        CREATE TABLE public.fact_transfers
        (
            id bigint,
            key_agency text COLLATE pg_catalog."default",
            bank_code character varying COLLATE pg_catalog."default",
            agency character varying COLLATE pg_catalog."default",
            transaction_date date,
            transaction_amount numeric(20,2),
            CONSTRAINT fk_agency FOREIGN KEY (key_agency)
                REFERENCES public.dim_agency (key_agency) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION,
            CONSTRAINT fk_bank FOREIGN KEY (bank_code)
                REFERENCES public.dim_bank (bank_code) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION,
            CONSTRAINT fk_calendar FOREIGN KEY (transaction_date)
                REFERENCES public.dim_calendar (transaction_date) MATCH SIMPLE
                ON UPDATE NO ACTION
                ON DELETE NO ACTION
        )

        TABLESPACE pg_default;

        ALTER TABLE public.fact_transfers
            OWNER to airflow;
    END;
$do$