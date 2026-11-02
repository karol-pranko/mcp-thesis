-- =============================================================================
-- Rola tylko do odczytu dla bramy MCP (zasada najmniejszych uprawnień).
--
-- REKOMENDACJA WDROŻENIOWA. Skrypt NIE był użyty w rundzie pomiarowej 20261004T120209_d347d2:
-- w rundzie brama i aparatura łączyły się rolą mcp_user z atrybutem SUPERUSER (scripts/setup_runpod.sh).
-- Skrypt nie był też przetestowany pomiarem; przed użyciem należy go dostosować do schematu i wersji PostgreSQL.
--
-- Użycie (jako właściciel bazy / administrator):
--   psql -d analytics_db -v reader_password="'...'" -f testbed/init-scripts/rola_mcp_reader.sql
-- =============================================================================

CREATE ROLE mcp_reader LOGIN PASSWORD :reader_password
    NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOREPLICATION NOBYPASSRLS
    CONNECTION LIMIT 20;

-- Domyślnie tylko odczyt i ograniczony czas zapytań
ALTER ROLE mcp_reader SET default_transaction_read_only = on;
ALTER ROLE mcp_reader SET statement_timeout = '10s';
ALTER ROLE mcp_reader SET idle_in_transaction_session_timeout = '60s';

-- Dostęp wyłącznie do odczytu tabel schematu public
REVOKE ALL ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO mcp_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO mcp_reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO mcp_reader;

-- Odebranie funkcji o skutkach ubocznych, przepuszczanych przez walidator AST (Scenariusz D).
-- Funkcje administracyjne (pg_terminate_backend, pg_cancel_backend, pg_reload_conf, pg_read_file, pg_ls_dir,
-- lo_import, lo_export) są domyślnie niedostępne dla roli bez uprawnień superużytkownika i ról pg_*;
-- poniższe REVOKE usuwają uprawnienia nadane PUBLIC dla funkcji dostępnych domyślnie.
REVOKE EXECUTE ON FUNCTION pg_sleep(double precision) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION pg_advisory_lock(bigint) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION set_config(text, text, boolean) FROM PUBLIC;
-- nextval/setval: brak uprawnienia USAGE/UPDATE do sekwencji wystarcza (nie nadawać GRANT ON SEQUENCES).
-- dblink_exec: nie instalować rozszerzenia dblink w bazie używanej przez bramę.
-- SELECT ... FOR UPDATE wymaga uprawnienia UPDATE na tabeli, którego rola nie ma.
