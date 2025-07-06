import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text
from tqdm import tqdm
import os
import db_utils

# --- FUNCIONES DE TRANSFORMACIÓN ESPECÍFICAS ---


def transform_assessments(df: pd.DataFrame, courses_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el dataframe de 'assessments' aplicando reglas de negocio.
    Regla: Si es un examen ('Exam') y la fecha es nula, se imputa con la
           duración del curso correspondiente.
    """
    # Unir con los datos de cursos para obtener la duración (length)
    df_merged = pd.merge(
        df, courses_df, on=["code_module", "code_presentation"], how="left"
    )

    # Condición para la imputación de fechas de examen
    condition = (df_merged["assessment_type"] == "Exam") & (df_merged["date"].isnull())

    # Aplicar la regla de negocio
    df_merged.loc[condition, "date"] = df_merged.loc[
        condition, "module_presentation_length"
    ]

    # Asegurar que las columnas del dataframe original se mantengan
    return df_merged[df.columns]


def transform_student_assessment(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transforma el dataframe de 'studentAssessment' aplicando reglas de negocio.
    Regla: Crea el campo de dominio 'assessment_result' basado en el 'score'.
           Un score menor a 40 es 'Fail', de lo contrario es 'Pass'.
    """

    def classify_score(score):
        if pd.isna(score):
            return None
        return "Pass" if float(score) >= 40 else "Fail"

    df["assessment_result"] = df["score"].apply(classify_score)
    return df


def transform_generic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Función de transformación por defecto para tablas que no necesitan
    lógica de negocio específica.
    """
    return df


# --- FUNCIONES DE GESTIÓN DE BASES DE DATOS ---


def get_table_names(engine: Engine) -> list:
    """Obtiene la lista de tablas en la base de datos."""
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text("SELECT name FROM sqlite_master WHERE type='table';")
            )
            return [row[0] for row in result]
    except Exception as e:
        print(f"Error al obtener nombres de tablas: {e}")
        return []


def table_exists(engine: Engine, table_name: str) -> bool:
    """Verifica si una tabla existe en la base de datos."""
    try:
        with engine.connect() as connection:
            result = connection.execute(
                text(
                    "SELECT name FROM sqlite_master WHERE type='table' AND name = :table_name"
                ),
                {"table_name": table_name},
            )
            return result.fetchone() is not None
    except Exception as e:
        print(f"Error al verificar existencia de tabla {table_name}: {e}")
        return False


def clear_data_tables(engine: Engine):
    """Limpia todas las tablas de datos del ETL pero mantiene las tablas de log."""
    data_tables = [
        "courses",
        "vle",
        "studentInfo",
        "studentRegistration",
        "assessments",
        "studentAssessment",
        "studentVle",
    ]

    print("\n🧹 Limpiando tablas de datos existentes...")

    with engine.begin() as connection:
        for table_name in data_tables:
            try:
                # Verificar si la tabla existe antes de intentar limpiarla
                if table_exists(engine, table_name):
                    connection.execute(text(f"DELETE FROM {table_name}"))
                    print(f"   ✅ Tabla '{table_name}' limpiada")
                else:
                    print(f"   ℹ️  Tabla '{table_name}' no existe, se omite limpieza")
            except Exception as e:
                print(f"   ⚠️  No se pudo limpiar '{table_name}': {e}")

    # Limpiar logs ETL para permitir recarga completa (solo si la tabla existe)
    try:
        if table_exists(engine, "etl_log"):
            db_utils.clear_etl_logs(engine)
            print("   ✅ Logs de ETL limpiados")
        else:
            print("   ℹ️  Tabla 'etl_log' no existe, se omite limpieza de logs")
    except Exception as e:
        print(f"   ⚠️  Error al limpiar logs de ETL: {e}")


def get_data_source_summary(data_path: str) -> dict:
    """Obtiene un resumen de la fuente de datos."""
    summary = {
        "path": data_path,
        "files": {},
        "total_records": 0,
        "is_valid": True,
        "errors": [],
    }

    if not os.path.exists(data_path):
        summary["is_valid"] = False
        summary["errors"].append(f"Carpeta '{data_path}' no existe")
        return summary

    csv_files = [f for f in os.listdir(data_path) if f.endswith(".csv")]

    for csv_file in csv_files:
        file_path = os.path.join(data_path, csv_file)
        try:
            df = pd.read_csv(file_path)
            summary["files"][csv_file] = {
                "rows": len(df),
                "columns": len(df.columns),
                "size_mb": round(os.path.getsize(file_path) / 1024 / 1024, 2),
            }
            summary["total_records"] += len(df)
        except Exception as e:
            summary["errors"].append(f"Error al leer {csv_file}: {e}")

    return summary


# --- PROCESADOR PRINCIPAL DEL ETL ---


def process_csv_to_db(
    filepath: str,
    table_name: str,
    engine: Engine,
    batch_size: int,
    transform_func,
    **kwargs,
):
    """
    Procesa un archivo CSV en lotes, lo transforma y lo carga en la base de datos,
    controlando el progreso para poder reanudar el proceso si falla.
    """
    print(f"\n--- Iniciando procesamiento para la tabla: {table_name} ---")

    if not os.path.exists(filepath):
        print(f"Error: El archivo no fue encontrado en la ruta '{filepath}'")
        return

    start_chunk = db_utils.get_last_processed_chunk(table_name, engine)

    try:
        # Se define una lista de valores a ser tratados como nulos.
        # Esto maneja tanto '?' como cadenas vacías ''.
        missing_values = ["?", ""]

        # El procesamiento se realiza en un `with` para asegurar que el lector de archivos se cierre.
        with pd.read_csv(
            filepath, chunksize=batch_size, iterator=True, na_values=missing_values
        ) as reader:
            for i, chunk in enumerate(
                tqdm(reader, desc=f"Cargando {table_name}", unit=" chunks")
            ):
                current_chunk_index = i + 1

                if current_chunk_index <= start_chunk:
                    continue

                # NOTA: La limpieza de '?' y '' ya no es necesaria aquí,
                # pd.read_csv con na_values lo maneja eficientemente en la lectura.

                # 1. Aplicar función de transformación específica de la tabla
                transformed_chunk = transform_func(chunk, **kwargs)

                # 2. Carga (Load) y actualización del log dentro de una transacción atómica
                with engine.begin() as connection:
                    transformed_chunk.to_sql(
                        table_name, con=connection, if_exists="append", index=False
                    )
                    db_utils.update_etl_log(table_name, current_chunk_index, connection)

        print(f"Procesamiento de '{table_name}' completado exitosamente.")

    except Exception as e:
        print(f"\nError durante el procesamiento de '{table_name}': {e}")
        print(
            "El proceso se ha detenido. Puede reanudarlo ejecutando el script nuevamente."
        )


def run_full_etl(engine: Engine, config, clear_existing_data: bool = False):
    """
    Orquesta el proceso ETL completo, cargando todas las tablas en el orden correcto
    y aplicando las transformaciones de negocio necesarias.
    """
    batch_size = int(config["etl_settings"]["batch_size"])
    data_path = config["etl_settings"]["data_path"]

    # Mostrar información sobre la fuente de datos
    data_summary = get_data_source_summary(data_path)
    print(f"\n📊 Resumen de la fuente de datos '{data_path}':")
    print(f"   📁 Archivos CSV: {len(data_summary['files'])}")
    print(f"   📈 Total de registros: {data_summary['total_records']:,}")

    if data_summary["errors"]:
        print(f"   ⚠️  Errores encontrados: {len(data_summary['errors'])}")
        for error in data_summary["errors"]:
            print(f"      - {error}")

    if not data_summary["is_valid"]:
        print("❌ Fuente de datos no válida, abortando procesamiento.")
        return

    # Limpiar datos existentes si se solicita
    if clear_existing_data:
        clear_data_tables(engine)

    db_utils.setup_etl_log_table(engine)

    # Precargar datos de cursos en memoria, ya que son necesarios para la transformación de 'assessments'
    try:
        courses_filepath = os.path.join(data_path, "courses.csv")
        courses_df = pd.read_csv(courses_filepath)
        print(
            f"✅ Datos de 'courses' precargados para asistir en transformaciones ({len(courses_df)} registros)."
        )
    except FileNotFoundError:
        print(
            f"❌ Error fatal: {courses_filepath} no encontrado. Es un archivo esencial para el ETL. Abortando."
        )
        return

    # Plan de ejecución del ETL, mapeando tablas a sus funciones de transformación y dependencias
    etl_plan = {
        "courses": {"func": transform_generic, "args": {}},
        "vle": {"func": transform_generic, "args": {}},
        "studentInfo": {"func": transform_generic, "args": {}},
        "studentRegistration": {"func": transform_generic, "args": {}},
        "assessments": {
            "func": transform_assessments,
            "args": {"courses_df": courses_df},
        },
        "studentAssessment": {"func": transform_student_assessment, "args": {}},
        "studentVle": {"func": transform_generic, "args": {}},
    }

    # Orden de ejecución para respetar las restricciones de llaves foráneas
    execution_order = [
        "courses",
        "vle",
        "studentInfo",
        "studentRegistration",
        "assessments",
        "studentAssessment",
        "studentVle",
    ]

    # Procesar solo las tablas que existen en la fuente de datos
    available_tables = []
    for table_name in execution_order:
        filename = f"{table_name}.csv"
        filepath = os.path.join(data_path, filename)
        if os.path.exists(filepath):
            available_tables.append(table_name)
        else:
            print(
                f"⚠️  Tabla '{table_name}' no encontrada en la fuente de datos, se omitirá."
            )

    print(f"\n🔄 Procesando {len(available_tables)} tablas en orden:")
    for i, table_name in enumerate(available_tables, 1):
        print(f"   {i}. {table_name}")

    # Ejecutar el procesamiento
    for table_name in available_tables:
        plan = etl_plan[table_name]
        filename = f"{table_name}.csv"
        filepath = os.path.join(data_path, filename)

        process_csv_to_db(
            filepath, table_name, engine, batch_size, plan["func"], **plan["args"]
        )

    print(f"\n✅ Procesamiento ETL completado para la fuente: {data_path}")


def run_multi_source_etl(engine: Engine, config, data_sources: list):
    """
    Ejecuta el ETL para múltiples fuentes de datos.
    """
    print(
        f"\n🚀 Iniciando procesamiento ETL para {len(data_sources)} fuente(s) de datos"
    )

    for i, data_path in enumerate(data_sources):
        print(f"\n{'='*60}")
        print(f"PROCESANDO FUENTE {i+1}/{len(data_sources)}: {data_path}")
        print(f"{'='*60}")

        # Actualizar configuración con la ruta actual
        config.set("etl_settings", "data_path", data_path)

        # Limpiar datos existentes solo para la primera fuente o si es una fuente única
        clear_existing = (i == 0) or (len(data_sources) == 1)

        # Ejecutar ETL
        run_full_etl(engine, config, clear_existing_data=clear_existing)

        print(f"\n✅ Fuente {i+1} procesada exitosamente: {data_path}")

    print(f"\n🎉 ¡Procesamiento ETL completado para todas las fuentes!")
