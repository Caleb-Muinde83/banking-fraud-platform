--
-- PostgreSQL database dump
--

\restrict QFMXDUftzFsJ1mdc2dZfQhjRhPU0kWs8WswwYteKhsHB0CueIctvTbuoJqfZk9A

-- Dumped from database version 15.18
-- Dumped by pg_dump version 15.18

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: accounts; Type: TABLE; Schema: public; Owner: postgres_admin
--

CREATE TABLE public.accounts (
    account_id character varying NOT NULL,
    customer_id character varying NOT NULL,
    balance numeric(15,2),
    currency character varying,
    status character varying,
    created_at timestamp without time zone
);


ALTER TABLE public.accounts OWNER TO postgres_admin;

--
-- Name: alerts; Type: TABLE; Schema: public; Owner: postgres_admin
--

CREATE TABLE public.alerts (
    alert_id character varying NOT NULL,
    alert_type character varying NOT NULL,
    severity character varying NOT NULL,
    customer_id character varying,
    "timestamp" timestamp without time zone
);


ALTER TABLE public.alerts OWNER TO postgres_admin;

--
-- Name: audit_events; Type: TABLE; Schema: public; Owner: postgres_admin
--

CREATE TABLE public.audit_events (
    audit_id character varying NOT NULL,
    actor_type character varying NOT NULL,
    actor_id character varying NOT NULL,
    action character varying NOT NULL,
    resource character varying NOT NULL,
    "timestamp" timestamp without time zone
);


ALTER TABLE public.audit_events OWNER TO postgres_admin;

--
-- Name: beneficiaries; Type: TABLE; Schema: public; Owner: postgres_admin
--

CREATE TABLE public.beneficiaries (
    beneficiary_id character varying NOT NULL,
    customer_id character varying NOT NULL,
    account_number character varying NOT NULL,
    bank_name character varying NOT NULL,
    created_at timestamp without time zone
);


ALTER TABLE public.beneficiaries OWNER TO postgres_admin;

--
-- Name: cases; Type: TABLE; Schema: public; Owner: postgres_admin
--

CREATE TABLE public.cases (
    case_id character varying NOT NULL,
    alert_id character varying NOT NULL,
    status character varying,
    assignee character varying,
    created_at timestamp without time zone,
    resolved_at timestamp without time zone
);


ALTER TABLE public.cases OWNER TO postgres_admin;

--
-- Name: customers; Type: TABLE; Schema: public; Owner: postgres_admin
--

CREATE TABLE public.customers (
    customer_id character varying NOT NULL,
    first_name character varying NOT NULL,
    last_name character varying NOT NULL,
    email character varying NOT NULL,
    country character varying NOT NULL,
    risk_level character varying,
    created_at timestamp without time zone
);


ALTER TABLE public.customers OWNER TO postgres_admin;

--
-- Name: devices; Type: TABLE; Schema: public; Owner: postgres_admin
--

CREATE TABLE public.devices (
    device_id character varying NOT NULL,
    customer_id character varying NOT NULL,
    device_type character varying NOT NULL,
    first_seen timestamp without time zone,
    risk_score integer
);


ALTER TABLE public.devices OWNER TO postgres_admin;

--
-- Name: employee_actions; Type: TABLE; Schema: public; Owner: postgres_admin
--

CREATE TABLE public.employee_actions (
    action_id character varying NOT NULL,
    employee_id character varying NOT NULL,
    customer_id character varying,
    action_type character varying NOT NULL,
    "timestamp" timestamp without time zone
);


ALTER TABLE public.employee_actions OWNER TO postgres_admin;

--
-- Name: employees; Type: TABLE; Schema: public; Owner: postgres_admin
--

CREATE TABLE public.employees (
    employee_id character varying NOT NULL,
    department character varying NOT NULL,
    role character varying NOT NULL,
    created_at timestamp without time zone
);


ALTER TABLE public.employees OWNER TO postgres_admin;

--
-- Name: login_events; Type: TABLE; Schema: public; Owner: postgres_admin
--

CREATE TABLE public.login_events (
    event_id character varying NOT NULL,
    customer_id character varying NOT NULL,
    device_id character varying NOT NULL,
    country character varying NOT NULL,
    ip_address character varying NOT NULL,
    success boolean NOT NULL,
    "timestamp" timestamp without time zone
);


ALTER TABLE public.login_events OWNER TO postgres_admin;

--
-- Name: sessions; Type: TABLE; Schema: public; Owner: postgres_admin
--

CREATE TABLE public.sessions (
    session_id character varying NOT NULL,
    customer_id character varying NOT NULL,
    device_id character varying NOT NULL,
    ip_address character varying NOT NULL,
    created_at timestamp without time zone,
    expires_at timestamp without time zone NOT NULL
);


ALTER TABLE public.sessions OWNER TO postgres_admin;

--
-- Name: transactions; Type: TABLE; Schema: public; Owner: postgres_admin
--

CREATE TABLE public.transactions (
    transaction_id character varying NOT NULL,
    from_account character varying NOT NULL,
    to_account character varying NOT NULL,
    amount numeric(15,2) NOT NULL,
    currency character varying,
    transaction_type character varying NOT NULL,
    "timestamp" timestamp without time zone
);


ALTER TABLE public.transactions OWNER TO postgres_admin;

--
-- Name: accounts accounts_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT accounts_pkey PRIMARY KEY (account_id);


--
-- Name: alerts alerts_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_pkey PRIMARY KEY (alert_id);


--
-- Name: audit_events audit_events_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.audit_events
    ADD CONSTRAINT audit_events_pkey PRIMARY KEY (audit_id);


--
-- Name: beneficiaries beneficiaries_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.beneficiaries
    ADD CONSTRAINT beneficiaries_pkey PRIMARY KEY (beneficiary_id);


--
-- Name: cases cases_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.cases
    ADD CONSTRAINT cases_pkey PRIMARY KEY (case_id);


--
-- Name: customers customers_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.customers
    ADD CONSTRAINT customers_pkey PRIMARY KEY (customer_id);


--
-- Name: devices devices_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.devices
    ADD CONSTRAINT devices_pkey PRIMARY KEY (device_id);


--
-- Name: employee_actions employee_actions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.employee_actions
    ADD CONSTRAINT employee_actions_pkey PRIMARY KEY (action_id);


--
-- Name: employees employees_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.employees
    ADD CONSTRAINT employees_pkey PRIMARY KEY (employee_id);


--
-- Name: login_events login_events_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.login_events
    ADD CONSTRAINT login_events_pkey PRIMARY KEY (event_id);


--
-- Name: sessions sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_pkey PRIMARY KEY (session_id);


--
-- Name: transactions transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.transactions
    ADD CONSTRAINT transactions_pkey PRIMARY KEY (transaction_id);


--
-- Name: ix_accounts_account_id; Type: INDEX; Schema: public; Owner: postgres_admin
--

CREATE INDEX ix_accounts_account_id ON public.accounts USING btree (account_id);


--
-- Name: ix_alerts_alert_id; Type: INDEX; Schema: public; Owner: postgres_admin
--

CREATE INDEX ix_alerts_alert_id ON public.alerts USING btree (alert_id);


--
-- Name: ix_audit_events_audit_id; Type: INDEX; Schema: public; Owner: postgres_admin
--

CREATE INDEX ix_audit_events_audit_id ON public.audit_events USING btree (audit_id);


--
-- Name: ix_beneficiaries_beneficiary_id; Type: INDEX; Schema: public; Owner: postgres_admin
--

CREATE INDEX ix_beneficiaries_beneficiary_id ON public.beneficiaries USING btree (beneficiary_id);


--
-- Name: ix_cases_case_id; Type: INDEX; Schema: public; Owner: postgres_admin
--

CREATE INDEX ix_cases_case_id ON public.cases USING btree (case_id);


--
-- Name: ix_customers_customer_id; Type: INDEX; Schema: public; Owner: postgres_admin
--

CREATE INDEX ix_customers_customer_id ON public.customers USING btree (customer_id);


--
-- Name: ix_customers_email; Type: INDEX; Schema: public; Owner: postgres_admin
--

CREATE UNIQUE INDEX ix_customers_email ON public.customers USING btree (email);


--
-- Name: ix_devices_device_id; Type: INDEX; Schema: public; Owner: postgres_admin
--

CREATE INDEX ix_devices_device_id ON public.devices USING btree (device_id);


--
-- Name: ix_employee_actions_action_id; Type: INDEX; Schema: public; Owner: postgres_admin
--

CREATE INDEX ix_employee_actions_action_id ON public.employee_actions USING btree (action_id);


--
-- Name: ix_employees_employee_id; Type: INDEX; Schema: public; Owner: postgres_admin
--

CREATE INDEX ix_employees_employee_id ON public.employees USING btree (employee_id);


--
-- Name: ix_login_events_event_id; Type: INDEX; Schema: public; Owner: postgres_admin
--

CREATE INDEX ix_login_events_event_id ON public.login_events USING btree (event_id);


--
-- Name: ix_sessions_session_id; Type: INDEX; Schema: public; Owner: postgres_admin
--

CREATE INDEX ix_sessions_session_id ON public.sessions USING btree (session_id);


--
-- Name: ix_transactions_transaction_id; Type: INDEX; Schema: public; Owner: postgres_admin
--

CREATE INDEX ix_transactions_transaction_id ON public.transactions USING btree (transaction_id);


--
-- Name: accounts accounts_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT accounts_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id);


--
-- Name: alerts alerts_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.alerts
    ADD CONSTRAINT alerts_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id);


--
-- Name: beneficiaries beneficiaries_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.beneficiaries
    ADD CONSTRAINT beneficiaries_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id);


--
-- Name: cases cases_alert_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.cases
    ADD CONSTRAINT cases_alert_id_fkey FOREIGN KEY (alert_id) REFERENCES public.alerts(alert_id);


--
-- Name: devices devices_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.devices
    ADD CONSTRAINT devices_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id);


--
-- Name: employee_actions employee_actions_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.employee_actions
    ADD CONSTRAINT employee_actions_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id);


--
-- Name: employee_actions employee_actions_employee_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.employee_actions
    ADD CONSTRAINT employee_actions_employee_id_fkey FOREIGN KEY (employee_id) REFERENCES public.employees(employee_id);


--
-- Name: login_events login_events_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.login_events
    ADD CONSTRAINT login_events_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id);


--
-- Name: sessions sessions_customer_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.sessions
    ADD CONSTRAINT sessions_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id);


--
-- Name: transactions transactions_from_account_fkey; Type: FK CONSTRAINT; Schema: public; Owner: postgres_admin
--

ALTER TABLE ONLY public.transactions
    ADD CONSTRAINT transactions_from_account_fkey FOREIGN KEY (from_account) REFERENCES public.accounts(account_id);


--
-- Name: dbz_publication; Type: PUBLICATION; Schema: -; Owner: postgres_admin
--

CREATE PUBLICATION dbz_publication FOR ALL TABLES WITH (publish = 'insert, update, delete, truncate');


ALTER PUBLICATION dbz_publication OWNER TO postgres_admin;

--
-- PostgreSQL database dump complete
--

\unrestrict QFMXDUftzFsJ1mdc2dZfQhjRhPU0kWs8WswwYteKhsHB0CueIctvTbuoJqfZk9A

