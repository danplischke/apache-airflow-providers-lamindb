export AIRFLOW_HOME="$PWD/.airflow"
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dev/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_ALL_ADMINS=True
export AIRFLOW__API__PORT="${AIRFLOW_PORT:-8080}"
export AIRFLOW__CORE__EXECUTION_API_SERVER_URL="http://localhost:${AIRFLOW__API__PORT}/execution/"
export OBJC_DISABLE_INITIALIZE_FORK_SAFETY=YES
export no_proxy='*'
# Without a lamindb_default connection, the DAGs use the throwaway instance in .cache/lamin-home
if [ -z "${AIRFLOW_CONN_LAMINDB_DEFAULT:-}" ]; then
    UV_CACHE_DIR="${UV_CACHE_DIR:-$(uv cache dir)}"
    export UV_CACHE_DIR
    export HOME="$PWD/.cache/lamin-home"
fi
