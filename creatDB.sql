--
-- PostgreSQL database dump
--

-- Dumped from database version 11.5
-- Dumped by pg_dump version 11beta2

-- Started on 2019-12-16 05:10:37

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_with_oids = false;

--
-- TOC entry 199 (class 1259 OID 16722)
-- Name: limits_per_hour; Type: TABLE; Schema: public; Owner: username
--

CREATE TABLE public.limits_per_hour (
    limit_name character varying(20) NOT NULL,
    limit_value bigint NOT NULL,
    effective_date date DEFAULT now() NOT NULL
);
ALTER TABLE ONLY public.limits_per_hour ALTER COLUMN limit_name SET STATISTICS 0;
ALTER TABLE ONLY public.limits_per_hour ALTER COLUMN limit_value SET STATISTICS 0;


ALTER TABLE public.limits_per_hour OWNER TO username;

--
-- TOC entry 2810 (class 0 OID 16722)
-- Dependencies: 199
-- Data for Name: limits_per_hour; Type: TABLE DATA; Schema: public; Owner: username
--

COPY public.limits_per_hour (limit_name, limit_value, effective_date) FROM stdin;
min	1024	2019-12-16
max	1073741824	2019-12-16
min	1024	2019-12-16
max	1073741824	2019-12-16
\.


-- Completed on 2019-12-16 05:10:37

--
-- PostgreSQL database dump complete
--

