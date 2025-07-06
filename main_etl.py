# main_etl.py
import configparser
import db_utils
import etl_processor
import os


def display_db_menu():
    """Muestra un menú interactivo para seleccionar la base de datos."""
    print("\n===== MENÚ DE CONFIGURACIÓN DEL ETL =====")
    print("Por favor, elija el destino de la base de datos:")
    print("1. MySQL")
    print("2. SQLite")
    choice = input("Ingrese su opción (1 o 2): ")
    while choice not in ["1", "2"]:
        choice = input("Opción inválida. Por favor, ingrese 1 o 2: ")
    return "mysql" if choice == "1" else "sqlite"


def display_data_source_menu():
    """Muestra un menú interactivo para seleccionar la fuente de datos."""
    print("\n===== SELECCIÓN DE FUENTE DE DATOS =====")
    print("Por favor, elija la fuente de datos a cargar:")
    print("1. Datos originales (carpeta 'data')")
    print("2. Datos transformados (carpeta 'data_2')")
    print("3. Ambas fuentes (se combinan en la base de datos)")
    choice = input("Ingrese su opción (1, 2 o 3): ")
    while choice not in ["1", "2", "3"]:
        choice = input("Opción inválida. Por favor, ingrese 1, 2 o 3: ")
    return choice


def validate_data_source(data_path):
    """Valida que la carpeta de datos exista y contenga archivos CSV."""
    if not os.path.exists(data_path):
        print(f"Error: La carpeta '{data_path}' no existe.")
        return False

    # Verificar que contenga al menos algunos archivos CSV esenciales
    essential_files = ["courses.csv", "studentInfo.csv"]
    missing_files = []

    for file in essential_files:
        if not os.path.exists(os.path.join(data_path, file)):
            missing_files.append(file)

    if missing_files:
        print(
            f"Error: Archivos esenciales no encontrados en '{data_path}': {missing_files}"
        )
        return False

    return True


def get_data_sources(choice):
    """Devuelve las carpetas de datos según la elección del usuario."""
    if choice == "1":
        return ["./data/"]
    elif choice == "2":
        return ["./data_2/"]
    elif choice == "3":
        return ["./data/", "./data_2/"]
    else:
        return []


def display_data_source_info(data_path):
    """Muestra información sobre la fuente de datos actual."""
    print(f"\n--- Información de la fuente de datos: {data_path} ---")

    if not os.path.exists(data_path):
        print("❌ Carpeta no encontrada")
        return

    # Contar archivos CSV
    csv_files = [f for f in os.listdir(data_path) if f.endswith(".csv")]
    print(f"✅ Archivos CSV encontrados: {len(csv_files)}")

    # Mostrar archivos disponibles
    total_size = 0
    for csv_file in sorted(csv_files):
        file_path = os.path.join(data_path, csv_file)
        if os.path.exists(file_path):
            try:
                import pandas as pd

                df = pd.read_csv(file_path, nrows=0)  # Solo leer headers
                file_size = os.path.getsize(file_path)
                total_size += file_size
                size_mb = round(file_size / 1024 / 1024, 2)
                print(f"  📄 {csv_file}: {len(df.columns)} columnas, {size_mb} MB")
            except Exception as e:
                print(f"  ❌ {csv_file}: Error al leer archivo")

    print(f"📊 Tamaño total: {round(total_size / 1024 / 1024, 2)} MB")


def confirm_multi_source_processing():
    """Confirma el procesamiento de múltiples fuentes explicando las implicaciones."""
    print("\n⚠️  IMPORTANTE: Procesamiento de múltiples fuentes")
    print("   Al cargar ambas fuentes de datos:")
    print("   • Los datos se COMBINARÁN en la misma base de datos")
    print("   • Se limpiarán los datos existentes antes del primer procesamiento")
    print("   • Los datos de ambas fuentes coexistirán en las mismas tablas")
    print(
        "   • Pueden existir diferentes conjuntos de estudiantes/cursos en cada fuente"
    )

    confirm = input("\n¿Está seguro de que desea continuar? (s/n): ").lower()
    return confirm in ["s", "si", "y", "yes"]


def show_processing_summary(data_sources, db_type):
    """Muestra un resumen de lo que se va a procesar."""
    print(f"\n📋 RESUMEN DEL PROCESAMIENTO")
    print(f"   🗄️  Base de datos: {db_type.upper()}")
    print(f"   📁 Fuentes de datos: {len(data_sources)}")

    for i, source in enumerate(data_sources, 1):
        print(f"      {i}. {source}")

    if len(data_sources) > 1:
        print(f"   🔄 Modo: Combinación de múltiples fuentes")
    else:
        print(f"   🔄 Modo: Fuente única")


def main():
    """Función principal que orquesta el pipeline ETL."""
    try:
        print("🚀 SISTEMA ETL OULAD - MULTI-FUENTE")
        print("=" * 50)

        # Leer el archivo de configuración
        config = configparser.ConfigParser()
        config.read("config.ini")

        if not config.sections():
            print("❌ Error: No se pudo leer el archivo 'config.ini' o está vacío.")
            return

        # Mostrar estado actual del ETL si existe
        print("\n🔍 Verificando estado del sistema...")

        # Mostrar menú y obtener la elección del usuario para la base de datos
        db_type = display_db_menu()

        # Crear el motor de la base de datos
        engine = db_utils.create_db_engine(config, db_type)

        if not engine:
            print("❌ No se pudo establecer la conexión a la base de datos. Abortando.")
            return

        print("✅ Conexión a la base de datos establecida exitosamente.")

        # Mostrar estado previo del ETL
        db_utils.get_etl_status(engine)

        # Mostrar menú para seleccionar fuente de datos
        data_choice = display_data_source_menu()
        data_sources = get_data_sources(data_choice)

        if not data_sources:
            print("❌ Error: No se pudo determinar la fuente de datos.")
            return

        # Validar fuentes de datos
        valid_sources = []
        for data_path in data_sources:
            if validate_data_source(data_path):
                valid_sources.append(data_path)
                display_data_source_info(data_path)
            else:
                print(f"⚠️  Fuente de datos '{data_path}' no válida, se omitirá.")

        if not valid_sources:
            print("❌ Error: No se encontraron fuentes de datos válidas.")
            return

        # Confirmar procesamiento de múltiples fuentes si es necesario
        if len(valid_sources) > 1 and not confirm_multi_source_processing():
            print("❌ Procesamiento cancelado por el usuario.")
            return

        # Mostrar resumen del procesamiento
        show_processing_summary(valid_sources, db_type)

        # Confirmación final
        confirm = input("\n¿Continuar con el procesamiento? (s/n): ").lower()
        if confirm not in ["s", "si", "y", "yes"]:
            print("❌ Procesamiento cancelado por el usuario.")
            return

        # Ejecutar el procesamiento ETL
        print(f"\n🚀 INICIANDO PROCESAMIENTO ETL...")
        etl_processor.run_multi_source_etl(engine, config, valid_sources)

        # Mostrar estado final
        print(f"\n📊 ESTADO FINAL DEL ETL:")
        db_utils.get_etl_status(engine)

        print(f"\n🎉 ¡Proceso ETL finalizado exitosamente!")

        # Mostrar resumen final
        print("\n📋 RESUMEN FINAL:")
        print(f"   ✅ Fuentes procesadas: {len(valid_sources)}")
        print(f"   ✅ Base de datos: {db_type.upper()}")
        for source in valid_sources:
            print(f"   ✅ {source}")

    except KeyboardInterrupt:
        print("\n\n⚠️  Procesamiento interrumpido por el usuario.")
        print(
            "   El progreso se ha guardado y puede reanudarse ejecutando el script nuevamente."
        )
    except Exception as e:
        print(f"\n❌ Ha ocurrido un error inesperado en el proceso principal: {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    main()
