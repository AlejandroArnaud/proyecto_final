# Sistema ETL Multi-Fuente para Dataset OULAD

## Descripción del Proyecto

Este sistema ETL (Extract, Transform, Load) fue desarrollado para procesar datos del dataset OULAD (Open University Learning Analytics Dataset) desde múltiples fuentes de datos. El sistema permite la carga y procesamiento de datos tanto del dataset original como de datos transformados procedentes de archivos Excel procesados.

## Características Principales

### Procesamiento Multi-Fuente
- Procesamiento de datos originales OULAD (carpeta `data/`)
- Procesamiento de datos transformados (carpeta `data_2/`)
- Capacidad de combinar ambas fuentes en una sola base de datos

### Características Técnicas
- Validación automática de integridad de datos
- Sistema de recuperación ante errores mediante logs
- Limpieza automática de datos entre cargas
- Soporte para bases de datos SQLite y MySQL
- Procesamiento en lotes para optimizar rendimiento

## Estructura del Proyecto

```
tarea_4/
├── main_etl.py              # Script principal del sistema ETL
├── etl_processor.py         # Módulo de procesamiento de datos
├── db_utils.py              # Utilidades para manejo de base de datos
├── config.ini               # Archivo de configuración
├── cleaning_process/        # Scripts de procesamiento del Excel
├── data/                    # Datos originales del dataset OULAD
├── data_2/                  # Datos procesados del archivo Excel
├── schema.sql               # Esquema de la base de datos
├── requirements.txt         # Dependencias del proyecto
└── README_ETL_MULTI_FUENTE.md
```

## Instalación y Configuración

### Requisitos Previos
```bash
pip install pandas sqlalchemy mysql-connector-python tqdm openpyxl
```

### Configuración de Base de Datos
El archivo `config.ini` contiene las configuraciones para ambas bases de datos:

```ini
[mysql]
host = localhost
user = root
password = password
database = oulad_db
port = 3306

[sqlite]
db_file = oulad.db

[etl_settings]
batch_size = 10000
data_path = ./data/
```

## Uso del Sistema

### Ejecución Principal
```bash
python main_etl.py
```

El sistema presenta dos menús interactivos:

1. **Selección de Base de Datos**: Permite elegir entre MySQL o SQLite
2. **Selección de Fuente de Datos**: Ofrece tres opciones:
   - Solo datos originales
   - Solo datos transformados
   - Ambas fuentes combinadas

### Validación de Datos
El sistema realiza validaciones automáticas antes del procesamiento:
- Verificación de existencia de carpetas
- Validación de archivos CSV requeridos
- Comprobación de integridad de datos
- Verificación de compatibilidad de esquemas

## Arquitectura del Sistema

### Flujo de Procesamiento
1. **Validación**: Verificación de requisitos y estructura de datos
2. **Configuración**: Selección de base de datos y fuentes
3. **Conexión**: Establecimiento de conexión con la base de datos
4. **Limpieza**: Eliminación de datos existentes (si es necesario)
5. **Procesamiento**: Carga de datos en el siguiente orden:
   - Courses
   - VLE
   - StudentInfo
   - StudentRegistration
   - Assessments
   - StudentAssessment
   - StudentVle
6. **Validación Final**: Verificación de la carga exitosa

### Transformaciones Aplicadas
- **Imputación de fechas**: Para evaluaciones tipo "Exam" sin fecha asignada
- **Clasificación de resultados**: Generación del campo `assessment_result` basado en puntuaciones
- **Normalización de datos**: Conversión automática de tipos de datos
- **Gestión de valores nulos**: Tratamiento de campos faltantes

## Consideraciones Técnicas

### Gestión de Memoria
El sistema utiliza procesamiento en lotes para manejar archivos grandes de manera eficiente. El tamaño de lote se puede ajustar en el archivo de configuración.

### Integridad de Datos
- Todas las operaciones se realizan dentro de transacciones para garantizar consistencia
- Sistema de logs para permitir la recuperación ante fallos
- Validación de restricciones de clave foránea

### Rendimiento
- Procesamiento optimizado mediante pandas
- Conexiones eficientes a base de datos usando SQLAlchemy
- Carga por lotes para minimizar el uso de memoria

## Combinación de Fuentes de Datos

Al combinar múltiples fuentes, el sistema:
- Agrega datos de ambas fuentes en las mismas tablas
- Mantiene la integridad referencial
- Permite el análisis conjunto de datos originales y transformados

**Nota**: Es importante considerar que los IDs pueden diferir entre fuentes, por lo que se recomienda validar los resultados después del procesamiento.

## Estructura de Datos

### Tablas Principales
- **courses**: Información de cursos y presentaciones
- **assessments**: Detalles de evaluaciones
- **vle**: Actividades del entorno virtual de aprendizaje
- **studentInfo**: Información demográfica de estudiantes
- **studentRegistration**: Registros de inscripción
- **studentAssessment**: Resultados de evaluaciones
- **studentVle**: Interacciones con el entorno virtual

### Tabla de Control
- **etl_log**: Registro de progreso del proceso ETL para recuperación

## Ejemplos de Consultas

### Información General del Dataset
```sql
SELECT 
    (SELECT COUNT(*) FROM courses) as total_courses,
    (SELECT COUNT(*) FROM studentInfo) as total_students,
    (SELECT COUNT(*) FROM assessments) as total_assessments;
```

### Distribución por Género
```sql
SELECT gender, COUNT(*) as count 
FROM studentInfo 
GROUP BY gender;
```

### Estadísticas de Evaluaciones
```sql
SELECT 
    ROUND(AVG(score), 2) as promedio_puntuacion,
    MIN(score) as puntuacion_minima,
    MAX(score) as puntuacion_maxima,
    COUNT(*) as total_evaluaciones
FROM studentAssessment 
WHERE score IS NOT NULL;
```

## Manejo de Errores

El sistema incluye manejo robusto de errores:
- Validación de archivos antes del procesamiento
- Recuperación automática mediante logs de progreso
- Mensajes informativos para facilitar la depuración
- Rollback automático en caso de errores críticos

## Limitaciones y Consideraciones

- Los archivos muy grandes pueden requerir ajustes en el tamaño de lote
- La combinación de fuentes puede generar datos duplicados si no se valida correctamente
- Se recomienda realizar respaldos antes del procesamiento en producción

## Mantenimiento

Para mantener el sistema:
1. Revisar periódicamente la configuración de conexiones
2. Monitorear el rendimiento del procesamiento
3. Validar la integridad de datos después de cada carga
4. Actualizar las transformaciones según nuevos requisitos

---

**Proyecto desarrollado para**: Maestría en Inteligencia Artificial - Ciencia de Datos  
**Fecha**: Diciembre 2024  
**Versión**: 2.0  
**Compatibilidad**: Python 3.7+, SQLite 3+, MySQL 5.7+ 