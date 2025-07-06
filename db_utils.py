# db_utils.py
import configparser
from sqlalchemy import create_engine, text


def create_db_engine(config: configparser.ConfigParser, db_type: str):
    """
    Crea y devuelve un motor (engine) de SQLAlchemy basado en la configuración y el tipo de BD.
    """
    try:
        if db_type == "mysql":
            creds = config["mysql"]
            # Asegúrate de tener instalado mysql-connector-python: pip install mysql-connector-python
            conn_str = (
                f"mysql+mysqlconnector://{creds['user']}:{creds['password']}"
                f"@{creds['host']}:{creds['port']}/{creds['database']}"
            )
            print("Conectando a MySQL...")
            return create_engine(conn_str)
        elif db_type == "sqlite":
            db_file = config["sqlite"]["db_file"]
            conn_str = f"sqlite:///{db_file}"
            print(f"Conectando a SQLite en '{db_file}'...")
            return create_engine(conn_str)
        else:
            raise ValueError("Tipo de base de datos no soportado.")
    except Exception as e:
        print(f"Error al crear la conexión a la base de datos: {e}")
        return None


def setup_etl_log_table(engine):
    """
    Crea una tabla para registrar el progreso del ETL si no existe.
    """
    try:
        with engine.connect() as connection:
            connection.execute(
                text(
                    """
            CREATE TABLE IF NOT EXISTS etl_log (
                table_name VARCHAR(255) PRIMARY KEY,
                last_processed_chunk INT,
                last_processed_timestamp DATETIME
            );
            """
                )
            )
            connection.commit()
        print("Tabla 'etl_log' de seguimiento asegurada.")
    except Exception as e:
        print(f"Error al crear la tabla de log: {e}")


def get_last_processed_chunk(table_name: str, engine) -> int:
    """
    Obtiene el último chunk procesado para una tabla específica.
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text(
                    "SELECT last_processed_chunk FROM etl_log WHERE table_name = :t_name"
                ),
                {"t_name": table_name},
            ).scalar_one_or_none()
            return result if result is not None else 0
    except Exception as e:
        print(
            f"No se pudo obtener el último chunk procesado para {table_name}. Asumiendo 0. Error: {e}"
        )
        return 0


def update_etl_log(table_name: str, chunk_index: int, connection):
    """
    Actualiza (o inserta) el registro de progreso para una tabla.
    Debe ser llamado dentro de una transacción.
    """
    # SQL compatible con MySQL (UPSERT) y SQLite (UPSERT)
    if connection.engine.dialect.name == "mysql":
        sql = text(
            """
        INSERT INTO etl_log (table_name, last_processed_chunk, last_processed_timestamp)
        VALUES (:t_name, :c_index, NOW())
        ON DUPLICATE KEY UPDATE last_processed_chunk = :c_index, last_processed_timestamp = NOW();
        """
        )
    else:  # sqlite
        sql = text(
            """
        INSERT INTO etl_log (table_name, last_processed_chunk, last_processed_timestamp)
        VALUES (:t_name, :c_index, datetime('now'))
        ON CONFLICT(table_name) DO UPDATE SET last_processed_chunk = :c_index, last_processed_timestamp = datetime('now');
        """
        )

    connection.execute(sql, {"t_name": table_name, "c_index": chunk_index})


def clear_etl_logs(engine):
    """
    Limpia todos los registros de log del ETL para permitir un procesamiento completo desde cero.
    """
    try:
        with engine.begin() as connection:
            connection.execute(text("DELETE FROM etl_log"))
        print("Logs de ETL limpiados exitosamente.")
    except Exception as e:
        print(f"Error al limpiar logs de ETL: {e}")


def get_etl_status(engine):
    """
    Obtiene el estado actual del ETL mostrando el progreso de cada tabla.
    """
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text(
                    """
                SELECT table_name, last_processed_chunk, last_processed_timestamp 
                FROM etl_log 
                ORDER BY last_processed_timestamp DESC
            """
                )
            )

            logs = result.fetchall()
            if logs:
                print("\n📊 Estado actual del ETL:")
                for log in logs:
                    print(f"   📋 {log[0]}: Chunk {log[1]} - {log[2]}")
            else:
                print("📊 No hay registros de ETL previos.")

            return logs
    except Exception as e:
        print(f"Error al obtener estado del ETL: {e}")
        return []
