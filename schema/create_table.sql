-- Table: public.call_log

-- DROP TABLE public.call_log;

CREATE TABLE public.call_log
(
    id serial primary key,
    phone_number character varying(16) COLLATE pg_catalog."default",
    call_type character varying(10) COLLATE pg_catalog."default",
    call_duration time without time zone,
    contact_id integer,
    created_at timestamp without time zone,
    CONSTRAINT fk_call_log_contact FOREIGN KEY (contact_id)
        REFERENCES public.contact (id) MATCH SIMPLE
        ON UPDATE NO ACTION
        ON DELETE NO ACTION
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.call_log
    OWNER to postgres;
	
	-- Table: public.contact

-- DROP TABLE public.contact;

CREATE TABLE public.contact
(
    id serial primary key
    name character varying COLLATE pg_catalog."default",
    phone character varying(16) COLLATE pg_catalog."default",
    address character varying(250) COLLATE pg_catalog."default",
    email character varying COLLATE pg_catalog."default",
    notes text COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE public.contact
    OWNER to postgres;
	
commit;